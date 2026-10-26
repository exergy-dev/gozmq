package socket

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"sync"
	"sync/atomic"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// identityRouter is the shared core of the identity-addressed engines
// (ROUTER and STREAM). Both map each attached pipe to a routing identity,
// fair-queue inbound messages, stamp the source identity as the leading recv
// frame, and route outbound messages by their leading identity frame. The
// engines differ only in their own Send framing and Metrics.
//
// The identity assigned at attach is also stored on the pipe itself
// (pipe.End.SetRoutingID), and Recv stamps from there. A message received
// after DetachPipe, which a closed pipe drains afterwards, therefore still
// carries its sender's identity, and nothing
// needs to outlive the detach: the engine keeps no reference to a departed
// pipe. Nor is it ever looked up by identity bytes, so a newer peer
// claiming the same identity can't be confused with the departed one.
//
// Concurrency: idOf/nextID/closed are guarded by mu, and so are the
// writers of routes, which Send's lookup reads without it; sent/recv are
// atomic.
type identityRouter struct {
	fq   *FairQueue
	name string // engine name for error messages ("router"/"stream")

	mu     sync.Mutex
	closed bool // fq.closeCh, closed by Close, aborts a Send parked at HWM
	// idOf maps an attached pipe to its routing identity (peer-advertised
	// PeerIdentity, or a synthesized 5-byte identity for anonymous peers).
	idOf map[*pipe.End][]byte
	// routes maps a routing identity (string key) back to the pipe.
	// Last-writer-wins on collision: a peer claiming an identity that is
	// already attached displaces the stale entry.
	//
	// This is libzmq's ZMQ_ROUTER_HANDOVER=1 behaviour, NOT its default.
	// libzmq defaults to handover=0, where the *new* peer is rejected and
	// the stale pipe keeps the identity — which strands a client that
	// reconnects before the server has noticed the old connection died,
	// the common case behind a NAT or load balancer. gozmq always hands
	// over; there is no option to get the rejecting behaviour. See
	// docs/COMPATIBILITY.md.
	//
	// The table is copy-on-write so that every Send's lookup is one atomic
	// load and a map read, with no lock: a table is never modified once
	// stored, and AttachPipe and DetachPipe (under mu) store a modified
	// clone. That makes an attach or detach O(peers), a map clone, which is
	// cheap next to the connection setup or teardown behind it.
	routes atomic.Pointer[routeTable]
	// nextID is the counter behind synthesized identities. It starts at a
	// random value, as in libzmq: routing always hands an identity over to
	// the newest claimant, so predictable identities would let a peer
	// advertising one hijack an anonymous peer's replies.
	nextID uint32

	gates // the FairQueue arms in; AttachPipe arms out
	counters
}

// routeTable is one immutable version of identityRouter.routes. A nil
// table is an engine with no peer yet; closedRoutes marks a closed one.
type routeTable struct {
	pipeOf map[string]*pipe.End
}

var closedRoutes = &routeTable{}

func newIdentityRouter(name string) identityRouter {
	g := newGates()
	return identityRouter{
		fq:     NewFairQueue(g.in),
		name:   name,
		idOf:   make(map[*pipe.End][]byte),
		nextID: rand.Uint32(),
		gates:  g,
	}
}

// routeOf returns the pipe the current table routes id to: under e.mu the
// table is current, without it a snapshot (as lookup takes).
func (e *identityRouter) routeOf(id string) (*pipe.End, bool) {
	t := e.routes.Load()
	if t == nil {
		return nil, false
	}
	p, ok := t.pipeOf[id]
	return p, ok
}

// setRouteLocked stores a new table: the current one with id routed to p,
// or with id removed when p is nil. Caller must hold e.mu.
func (e *identityRouter) setRouteLocked(id string, p *pipe.End) {
	var m map[string]*pipe.End
	if t := e.routes.Load(); t != nil {
		m = maps.Clone(t.pipeOf)
	}
	if p == nil {
		delete(m, id)
	} else {
		if m == nil {
			m = make(map[string]*pipe.End, 1)
		}
		m[id] = p
	}
	e.routes.Store(&routeTable{pipeOf: m})
}

// CanRecvNow exposes the FairQueue's pending-data level check.
func (e *identityRouter) CanRecvNow() bool { return e.fq.CanRecv() }

// CanSendNow reports whether at least one peer is currently attached.
func (e *identityRouter) CanSendNow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.closed && len(e.idOf) > 0
}

// AttachPipe registers p, assigning it a routing identity (its PeerIdentity
// or a synthesized one) and starting fair-queued inbound delivery. Re-attaching
// the same pipe is a no-op. Returns ErrSocketClosed after Close.
func (e *identityRouter) AttachPipe(p *pipe.End) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return errClosed(e.name + " attach")
	}
	if _, ok := e.idOf[p]; ok {
		e.mu.Unlock()
		return nil
	}
	id := p.PeerIdentity()
	if len(id) == 0 {
		id = e.synthesizeIdentityLocked()
	}
	// Copy defensively so later mutation of the peer-supplied slice can't
	// corrupt the routing table.
	idCopy := bytes.Clone(id)
	e.idOf[p] = idCopy
	e.setRouteLocked(string(idCopy), p)
	p.SetRoutingID(idCopy) // before fq.Attach: Recv reads it for every message
	e.armOut()
	e.mu.Unlock()
	return e.fq.Attach(p)
}

// DetachPipe removes p from the routing table and the fair queue. The
// identity→pipe index is cleared only if it still points at p, preserving a
// newer peer that reused the same identity.
func (e *identityRouter) DetachPipe(p *pipe.End) error {
	e.mu.Lock()
	if id, ok := e.idOf[p]; ok {
		delete(e.idOf, p)
		if cur, ok := e.routeOf(string(id)); ok && cur == p {
			e.setRouteLocked(string(id), nil)
		}
	}
	e.mu.Unlock()
	return e.fq.Detach(p)
}

// lookup finds the pipe for routing identity id on behalf of a Send named
// name ("router"/"stream"). It fails with ErrSocketClosed after Close. An
// unknown identity is ErrRouteUnknown when mandatory; otherwise it is
// counted as a drop and lookup returns a nil pipe and a nil error, which
// leaves the caller to release its message. It takes no lock (see routes):
// a peer attached or detached concurrently may or may not be seen, as with
// any send racing the attach or detach.
func (e *identityRouter) lookup(id []byte, name string, mandatory bool) (*pipe.End, error) {
	t := e.routes.Load()
	var p *pipe.End
	var ok bool
	if t != nil {
		p, ok = t.pipeOf[string(id)]
	}
	switch {
	case t == closedRoutes:
		return nil, errClosed(name + " send")
	case ok:
		return p, nil
	case mandatory:
		return nil, fmt.Errorf("%s send: identity %q: %w", name, id, zmqerr.ErrRouteUnknown)
	}
	e.dropped.Add(1)
	return nil, nil
}

// deliver hands out, the message derived for the peer behind p, to p (see
// sendTo) and releases out if p did not take it. The caller releases its
// own message after a nil return.
func (e *identityRouter) deliver(ctx context.Context, p *pipe.End, out *msg.Msg, id []byte, name string, mandatory bool) error {
	sent, err := e.sendTo(ctx, p, out, id, name, mandatory)
	if !sent {
		out.Release()
	}
	return err
}

// sendTo hands m to p, the pipe of the peer named id, and reports whether
// p took it, together with m's reference; otherwise m is still the
// caller's. It counts the send, and maps a closed engine to
// ErrSocketClosed and a peer that vanished between lookup and send to the
// same outcome as an unknown identity: ErrRouteUnknown when mandatory,
// otherwise a counted drop and a nil error.
func (e *identityRouter) sendTo(ctx context.Context, p *pipe.End, m *msg.Msg, id []byte, name string, mandatory bool) (sent bool, err error) {
	err = p.SendOr(ctx, m, e.fq.closeCh)
	switch {
	case err == nil:
		e.sent.Add(1)
		return true, nil
	case errors.Is(err, pipe.ErrAborted):
		err = errClosed(name + " send")
	case !errors.Is(err, io.ErrClosedPipe):
	case mandatory:
		err = fmt.Errorf("%s send: identity %q: peer disconnected: %w", name, id, zmqerr.ErrRouteUnknown)
	default:
		e.dropped.Add(1)
		err = nil
	}
	return false, err
}

// Recv fair-queues across attached pipes and prepends the source peer's
// routing identity as the leading frame. The identity comes from the source
// pipe (set by AttachPipe), so it is right even if the pipe has since been
// detached.
func (e *identityRouter) Recv(ctx context.Context) (*msg.Msg, error) {
	m, src, err := e.fq.RecvWithSource(ctx)
	if err != nil {
		return nil, err
	}
	// id is the defensive clone made once at AttachPipe and never mutated
	// afterwards, so prepend it without re-cloning per Recv.
	id := src.RoutingID()
	e.recv.Add(1)
	out := prependFrames([][]byte{id}, m)
	m.Release()
	return out, nil
}

// Close releases engine state, unblocks pending Recv with ErrSocketClosed, and
// drops references to attached pipes (without closing them — the transport owns
// their lifetime). Idempotent.
func (e *identityRouter) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.idOf = nil
	e.routes.Store(closedRoutes)
	e.mu.Unlock()
	return e.fq.Close()
}

// Metrics reports the engine's counters. Dropped counts silent
// non-mandatory ROUTER route misses, including vanished peers; it stays
// zero for STREAM, whose sends fail instead.
func (e *identityRouter) Metrics() Metrics {
	return e.snapshot(e.fq.QueueDepth())
}

// synthesizeIdentityLocked returns a fresh 5-byte routing identity for an
// anonymous peer: byte 0 is 0 (the libzmq marker for an auto-assigned
// identity), then a big-endian per-engine counter, as libzmq does. A
// counter cannot collide the way random bytes can (routes is
// last-writer-wins, so a collision would silently misroute replies); the
// in-use check only matters after 2^32 attaches or when a peer advertised
// a zero-led identity itself. Caller must hold e.mu.
func (e *identityRouter) synthesizeIdentityLocked() []byte {
	for {
		e.nextID++
		id := make([]byte, 5)
		binary.BigEndian.PutUint32(id[1:], e.nextID)
		if _, taken := e.routeOf(string(id)); !taken {
			return id
		}
	}
}
