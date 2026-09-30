package socket

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// identityRouter is the shared core of the identity-addressed engines
// (ROUTER and STREAM). Both map each attached pipe to a routing identity,
// fair-queue inbound messages, stamp the source identity as the leading recv
// frame, and route outbound messages by their leading identity frame. The
// engines differ only in their own Send framing and Metrics, plus whether
// the post-detach identity cache below is enabled (ROUTER yes, STREAM no).
//
// Concurrency: idOf/pipeOf/nextID/closed and the detach cache are guarded by mu;
// sent/recv are atomic.
type identityRouter struct {
	fq   *FairQueue
	name string // engine name for error messages ("router"/"stream")

	mu     sync.Mutex
	closed bool
	// idOf maps an attached pipe to its routing identity (peer-advertised
	// PeerIdentity, or a synthesized 5-byte identity for anonymous peers).
	idOf map[*pipe.End][]byte
	// pipeOf maps a routing identity (string key) back to the pipe.
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
	pipeOf map[string]*pipe.End
	// nextID is the counter behind synthesized identities. It starts at a
	// random value, as in libzmq: routing always hands an identity over to
	// the newest claimant, so predictable identities would let a peer
	// advertising one hijack an anonymous peer's replies.
	nextID uint32
	// sendGate is the Poller-facing PollOut signal, level-armed while at
	// least one peer is attached.
	sendGate readyGate

	counters

	// detachedIdOf retains, post-Detach, the identity in use for a pipe at
	// Detach time, so Recv can still stamp in-flight messages correctly
	// (race-safety against a concurrent Detach; see routerEngine doc).
	// Bounded to routerDetachCacheCap, FIFO eviction via detachedOrder.
	// nil when the cache is disabled (STREAM): all lookups miss and the
	// remember/remove helpers no-op.
	detachedIdOf  map[*pipe.End][]byte
	detachedOrder []*pipe.End
}

func newIdentityRouter(name string, detachCache bool) identityRouter {
	var detached map[*pipe.End][]byte
	if detachCache {
		detached = make(map[*pipe.End][]byte)
	}
	return identityRouter{
		fq:           NewFairQueue(),
		name:         name,
		idOf:         make(map[*pipe.End][]byte),
		pipeOf:       make(map[string]*pipe.End),
		nextID:       rand.Uint32(),
		sendGate:     newReadyGate(),
		detachedIdOf: detached,
	}
}

// routerDetachCacheCap bounds the side cache of identities for recently
// detached pipes. A small FIFO cap suffices: the FairQueue inbound channel has
// capacity 1 and a detached pipe's forwarder exits shortly after Detach, so the
// window in which it can still produce messages is short. Under extreme churn a
// handful of in-flight messages predating eviction fall back to the pipe's
// advertised PeerIdentity (never cross-wired to a newer peer, since the key is
// the pipe pointer).
const routerDetachCacheCap = 1024

// rememberDetachedLocked stores (pipe -> identity) in the post-detach side
// cache, evicting the oldest entry if full. No-op when the cache is disabled
// (nil map). Caller must hold e.mu.
func (e *identityRouter) rememberDetachedLocked(p *pipe.End, id []byte) {
	if e.detachedIdOf == nil {
		return
	}
	if _, exists := e.detachedIdOf[p]; exists {
		// Unreachable in practice (Detach runs once per pipe); guard drift.
		e.detachedIdOf[p] = id
		return
	}
	if len(e.detachedOrder) >= routerDetachCacheCap {
		evict := e.detachedOrder[0]
		e.detachedOrder = e.detachedOrder[1:]
		delete(e.detachedIdOf, evict)
	}
	e.detachedIdOf[p] = id
	e.detachedOrder = append(e.detachedOrder, p)
}

// removeDetachedLocked drops any cached detach entry for p, called from Attach
// so re-attaching a reused pipe value doesn't leave a stale entry. Caller must
// hold e.mu.
func (e *identityRouter) removeDetachedLocked(p *pipe.End) {
	if _, ok := e.detachedIdOf[p]; !ok {
		return
	}
	delete(e.detachedIdOf, p)
	if i := slices.Index(e.detachedOrder, p); i >= 0 {
		e.detachedOrder = slices.Delete(e.detachedOrder, i, i+1)
	}
}

// ReadyRecv exposes the inbound FairQueue's read-ready signal.
func (e *identityRouter) ReadyRecv() <-chan struct{} { return e.fq.ReadyRecv() }

// CanRecvNow exposes the FairQueue's pending-data level check.
func (e *identityRouter) CanRecvNow() bool { return e.fq.HasPending() }

// CanSendNow reports whether at least one peer is currently attached.
func (e *identityRouter) CanSendNow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.closed && len(e.idOf) > 0
}

// ReadyWrite signals (level-armed) when at least one peer is attached.
func (e *identityRouter) ReadyWrite() <-chan struct{} {
	e.mu.Lock()
	e.signalReadyWriteLocked()
	e.mu.Unlock()
	return e.sendGate.ch
}

func (e *identityRouter) signalReadyWriteLocked() {
	if !e.closed && len(e.idOf) > 0 {
		e.sendGate.arm()
	}
}

// AttachPipe registers p, assigning it a routing identity (its PeerIdentity
// or a synthesized one) and starting fair-queued inbound delivery. Re-attaching
// the same pipe is a no-op. Returns ErrSocketClosed after Close.
func (e *identityRouter) AttachPipe(p *pipe.End) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("%s attach: %w", e.name, zmqerr.ErrSocketClosed)
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
	e.pipeOf[string(idCopy)] = p
	e.removeDetachedLocked(p)
	e.signalReadyWriteLocked()
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
		if cur, ok := e.pipeOf[string(id)]; ok && cur == p {
			delete(e.pipeOf, string(id))
		}
		e.rememberDetachedLocked(p, id)
	}
	e.mu.Unlock()
	return e.fq.Detach(p)
}

// Recv fair-queues across attached pipes and prepends the source peer's
// routing identity as the leading frame. Identity resolution tolerates a
// concurrent Detach via the post-detach cache (ROUTER; nil and always a
// miss for STREAM); if both miss, it falls back to the pipe's advertised
// PeerIdentity.
func (e *identityRouter) Recv(ctx context.Context) (*msg.Msg, error) {
	m, src, err := e.fq.RecvWithSource(ctx)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	id := e.idOf[src]
	if id == nil {
		id = e.detachedIdOf[src]
	}
	e.mu.Unlock()
	if id == nil {
		id = src.PeerIdentity()
	}
	// Prepend the identity in a fresh frame slice. id is already the
	// defensive clone made once at AttachPipe (or the post-detach cache /
	// PeerIdentity fallback) and is never mutated afterward, so prepend it
	// directly without re-cloning per Recv.
	e.recv.Add(1)
	return prependFrames([][]byte{id}, m), nil
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
	e.pipeOf = nil
	e.detachedIdOf = nil
	e.detachedOrder = nil
	e.mu.Unlock()
	return e.fq.Close()
}

// metrics builds the engine's counters; extraDropped is folded into the
// FairQueue's own drop count (ROUTER adds its route-miss drops; STREAM adds 0).
func (e *identityRouter) metrics(extraDropped uint64) Metrics {
	return e.snapshot(extraDropped+e.fq.Dropped(), e.fq.QueueDepth())
}

// synthesizeIdentityLocked returns a fresh 5-byte routing identity for an
// anonymous peer: byte 0 is 0 (the libzmq marker for an auto-assigned
// identity), then a big-endian per-engine counter, as libzmq does. A
// counter cannot collide the way random bytes can (pipeOf is
// last-writer-wins, so a collision would silently misroute replies); the
// in-use check only matters after 2^32 attaches or when a peer advertised
// a zero-led identity itself. Caller must hold e.mu.
func (e *identityRouter) synthesizeIdentityLocked() []byte {
	for {
		e.nextID++
		id := make([]byte, 5)
		binary.BigEndian.PutUint32(id[1:], e.nextID)
		if _, taken := e.pipeOf[string(id)]; !taken {
			return id
		}
	}
}
