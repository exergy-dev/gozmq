package socket

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// routerEngine implements iface.SocketEngine for the ROUTER socket type.
//
// ROUTER is the identity-aware counterpart to DEALER. Each attached pipe
// is mapped to a routing identity (either its peer-advertised
// PeerIdentity or, for anonymous peers, a 5-byte synthesized identity
// generated at attach time — byte 0 == 0 and the remaining 4 bytes
// random, matching libzmq's anonymous-router convention).
//
// Send: the outbound *msg.Msg's leading frame is the peer identity; the
// engine routes the remaining frames to the pipe whose routing identity
// matches. When the leading identity does not correspond to any attached
// peer, the configured mandatory flag (see WithRouterMandatory on the
// Socket layer) selects the behaviour:
//   - mandatory=false (libzmq default): the send is silently dropped.
//   - mandatory=true:                    Send returns zmqerr.ErrRouteUnknown.
//
// Recv: fair-queues across attached pipes and prepends the source pipe's
// routing identity as the leading frame of the returned message, so the
// application can echo that frame back on Send to reply to the same peer.
//
// Identity collisions on AttachPipe use last-writer-wins — a reconnecting
// peer with the same identity displaces the stale entry. DetachPipe
// removes the identity→pipe mapping only if it still points at the
// detaching pipe, preserving newer mappings across churn.
//
// Recv is race-safe against concurrent Detach: a message that was
// enqueued into the FairQueue by a pipe's forwarder before Detach ran
// still resolves to that pipe's original identity via a bounded
// post-detach side cache (detachedIdOf). The cache is keyed by the
// iface.Pipe pointer rather than by identity bytes, so a newer peer
// that attaches with a colliding PeerIdentity cannot be mistaken for
// the departed original.

// routerDetachCacheCap bounds the side cache of identities for recently
// detached pipes. Entries in this cache let Recv stamp the correct
// identity on in-flight messages that were queued in the FairQueue
// before Detach ran. A small FIFO cap is sufficient: the inbound
// channel in FairQueue has capacity 1, and the forwarder goroutine
// for a detached pipe exits shortly after Detach, so the window in
// which a detached pipe can still produce messages is short. Under
// extreme churn we may lose the identity for a handful of in-flight
// messages that predate the eviction; those fall back to the pipe's
// advertised PeerIdentity (same behaviour as before the race fix for
// non-anonymous peers) without cross-wiring to a newer peer's id,
// since the key is the iface.Pipe pointer itself.
const routerDetachCacheCap = 1024

type routerEngine struct {
	fq *FairQueue

	mu        sync.Mutex
	closed    bool
	mandatory bool
	// idOf maps an attached pipe to the routing identity used for it —
	// either the peer-advertised PeerIdentity, or (for anonymous peers)
	// a synthesized 5-byte identity generated at attach time.
	idOf map[iface.Pipe][]byte
	// pipeOf maps a routing identity (as string key) back to the pipe.
	// Last-writer-wins on identity collisions, matching libzmq behavior
	// where a reconnecting peer with the same identity displaces the
	// stale mapping.
	pipeOf map[string]iface.Pipe
	// detachedIdOf retains, post-Detach, the identity that was in use
	// for a pipe at the time of Detach. Recv consults this as a
	// fallback when idOf misses, so in-flight messages queued in the
	// FairQueue before the Detach still get the correct leading frame
	// — even if a new peer with a colliding PeerIdentity has since
	// attached. Bounded to routerDetachCacheCap entries; eviction is
	// FIFO via detachedOrder. Keyed by iface.Pipe pointer, so a new
	// peer cannot ever resurface as the same map key.
	detachedIdOf  map[iface.Pipe][]byte
	detachedOrder []iface.Pipe
	// readyW is the Poller-facing PollOut signal. Level-armed while
	// at least one peer is attached. See engine.go Pollable.
	readyW chan struct{}

	sent    atomic.Uint64
	recv    atomic.Uint64
	dropped atomic.Uint64 // non-mandatory routes with unknown identity
}

// NewRouterEngine returns a new ROUTER socket engine. If mandatory is
// true, Send returns zmqerr.ErrRouteUnknown when the leading identity
// frame does not match an attached peer; otherwise such sends are
// silently dropped.
func NewRouterEngine(mandatory bool) iface.SocketEngine {
	return &routerEngine{
		fq:           NewFairQueue(),
		mandatory:    mandatory,
		idOf:         make(map[iface.Pipe][]byte),
		pipeOf:       make(map[string]iface.Pipe),
		detachedIdOf: make(map[iface.Pipe][]byte),
		readyW:       make(chan struct{}, 1),
	}
}

// ReadyRecv exposes the inbound FairQueue's read-ready signal.
func (e *routerEngine) ReadyRecv() <-chan struct{} { return e.fq.ReadyRecv() }

// ReadyWrite signals when at least one peer is attached.
func (e *routerEngine) ReadyWrite() <-chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.closed && len(e.idOf) > 0 {
		select {
		case e.readyW <- struct{}{}:
		default:
		}
	}
	return e.readyW
}

// CanRecvNow exposes the FairQueue's pending-data level check.
func (e *routerEngine) CanRecvNow() bool { return e.fq.HasPending() }

// CanSendNow reports whether at least one peer is currently attached.
func (e *routerEngine) CanSendNow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.closed && len(e.idOf) > 0
}

func (e *routerEngine) signalReadyWriteLocked() {
	if e.readyW == nil || e.closed {
		return
	}
	if len(e.idOf) > 0 {
		select {
		case e.readyW <- struct{}{}:
		default:
		}
	}
}

func (e *routerEngine) AttachPipe(p iface.Pipe) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("router attach: %w", zmqerr.ErrSocketClosed)
	}
	if _, ok := e.idOf[p]; ok {
		e.mu.Unlock()
		return nil
	}
	id := p.PeerIdentity()
	if len(id) == 0 {
		id = synthesizedIdentity()
	}
	// Copy defensively so later mutations on the peer-supplied slice
	// don't corrupt our routing table.
	idCopy := append([]byte(nil), id...)
	e.idOf[p] = idCopy
	e.pipeOf[string(idCopy)] = p
	// A pipe value could theoretically be re-attached (same pointer)
	// after detach; drop any stale detached-cache entry so the live
	// mapping is authoritative.
	e.removeDetachedLocked(p)
	e.signalReadyWriteLocked()
	e.mu.Unlock()
	return e.fq.Attach(p)
}

func (e *routerEngine) DetachPipe(p iface.Pipe) error {
	e.mu.Lock()
	if id, ok := e.idOf[p]; ok {
		delete(e.idOf, p)
		// Only clear the identity → pipe index entry if it still points
		// at this pipe. If a newer peer reused the same identity and
		// overwrote us, leave that newer mapping intact.
		if cur, ok := e.pipeOf[string(id)]; ok && cur == p {
			delete(e.pipeOf, string(id))
		}
		// Preserve the (pipe → identity) mapping in a bounded side
		// cache so Recv can still stamp the correct identity on any
		// messages queued in the FairQueue before this Detach ran.
		// Keyed by the iface.Pipe pointer, so a new peer that later
		// attaches with a colliding PeerIdentity can never be
		// misidentified via this cache.
		e.rememberDetachedLocked(p, id)
	}
	e.mu.Unlock()
	return e.fq.Detach(p)
}

// rememberDetachedLocked stores (pipe -> identity) in the post-detach
// side cache, evicting the oldest entry if the cache is full. Caller
// must hold e.mu.
func (e *routerEngine) rememberDetachedLocked(p iface.Pipe, id []byte) {
	if e.detachedIdOf == nil {
		return
	}
	if _, exists := e.detachedIdOf[p]; exists {
		// Refresh the value; leave order position untouched. This
		// branch is unreachable in practice (Detach only runs once
		// per attached pipe) but guards against drift.
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

// removeDetachedLocked drops any cached detach entry for p. Called
// from AttachPipe so re-attaching a pipe value (unusual but possible
// with pipe pointer reuse) doesn't leave a stale cache entry behind.
// Caller must hold e.mu.
func (e *routerEngine) removeDetachedLocked(p iface.Pipe) {
	if _, ok := e.detachedIdOf[p]; !ok {
		return
	}
	delete(e.detachedIdOf, p)
	for i, q := range e.detachedOrder {
		if q == p {
			e.detachedOrder = append(e.detachedOrder[:i], e.detachedOrder[i+1:]...)
			return
		}
	}
}

func (e *routerEngine) Send(ctx context.Context, m *msg.Msg) error {
	if m == nil || m.NumFrames() == 0 {
		return fmt.Errorf("router send: missing identity frame: %w", zmqerr.ErrRouteUnknown)
	}
	frames := m.Frames()
	id := frames[0]
	rest := frames[1:]

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("router send: %w", zmqerr.ErrSocketClosed)
	}
	p, ok := e.pipeOf[string(id)]
	mandatory := e.mandatory
	e.mu.Unlock()

	if !ok {
		if mandatory {
			return fmt.Errorf("router send: identity %q: %w", id, zmqerr.ErrRouteUnknown)
		}
		// Silent drop — count as dropped so observers see the unknown-
		// identity miss even though the caller saw a successful send.
		e.dropped.Add(1)
		return nil
	}

	// Build the routed message: everything after the identity frame.
	// Copy the frame slice header so we don't share backing storage
	// with the caller's message (which we do not own).
	payloadFrames := make([][]byte, len(rest))
	copy(payloadFrames, rest)
	routed := msg.FromFrames(payloadFrames...)
	if err := p.Send(ctx, routed); err != nil {
		return err
	}
	e.sent.Add(1)
	return nil
}

func (e *routerEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	m, src, err := e.fq.RecvWithSource(ctx)
	if err != nil {
		return nil, err
	}
	// Identity lookup must tolerate a concurrent DetachPipe that ran
	// between the FairQueue forwarder enqueuing this message and our
	// dequeue here. The live idOf map is authoritative for attached
	// pipes; if the pipe was detached, the identity is preserved
	// verbatim in detachedIdOf so we still stamp the correct leading
	// frame on this in-flight message. Both lookups are keyed by the
	// iface.Pipe pointer, so a newly attached peer with a colliding
	// PeerIdentity can never be mistaken for the original sender.
	e.mu.Lock()
	var id []byte
	if cur, ok := e.idOf[src]; ok {
		id = cur
	} else if cached, ok := e.detachedIdOf[src]; ok {
		id = cached
	}
	e.mu.Unlock()
	if id == nil {
		// Fallback: pipe never attached (shouldn't happen) or was
		// detached long enough ago that it was evicted from the
		// bounded detach cache. Use the peer's advertised identity so
		// we still return a well-formed message. This may be empty
		// for anonymous peers; callers see a zero-length leading frame
		// rather than a stale/cross-wired identity.
		id = src.PeerIdentity()
	}
	// Prepend id as the leading frame. Build a fresh frame slice to
	// avoid aliasing the caller-observable frame slice of m.
	orig := m.Frames()
	out := make([][]byte, 0, len(orig)+1)
	out = append(out, append([]byte(nil), id...))
	out = append(out, orig...)
	e.recv.Add(1)
	return msg.FromFrames(out...), nil
}

// Metrics reports the ROUTER engine's counters. Dropped covers two
// classes: silent non-mandatory route-misses (unknown identity, this
// engine), plus FairQueue ctx-cancel/close drops on the inbound side.
// QueueDepth samples the underlying FairQueue.
func (e *routerEngine) Metrics() iface.SocketMetrics {
	return iface.SocketMetrics{
		Sent:       e.sent.Load(),
		Recv:       e.recv.Load(),
		Dropped:    e.dropped.Load() + e.fq.Dropped(),
		QueueDepth: e.fq.QueueDepth(),
	}
}

func (e *routerEngine) Close() error {
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

// synthesizedIdentity returns a 5-byte routing identity for an anonymous
// peer: byte 0 is 0 (the libzmq marker for an auto-assigned identity),
// the remaining 4 bytes are uniformly random. Collisions across peers are
// astronomically unlikely but handled harmlessly by the pipeOf map
// (last-writer-wins; older entry cleared on Detach only if still current).
func synthesizedIdentity() []byte {
	out := make([]byte, 5)
	// byte 0 stays zero; fill 1..4 with randomness.
	if _, err := rand.Read(out[1:]); err != nil {
		// crypto/rand failing is an operational catastrophe; fall back
		// to a deterministic but still unique-per-process identity so we
		// don't panic in a library context. The monotonic counter lives
		// in a package-level var guarded by its own mutex.
		synthFallback(out[1:])
	}
	return out
}

var (
	synthMu      sync.Mutex
	synthCounter uint32
)

func synthFallback(dst []byte) {
	synthMu.Lock()
	synthCounter++
	c := synthCounter
	synthMu.Unlock()
	dst[0] = byte(c >> 24)
	dst[1] = byte(c >> 16)
	dst[2] = byte(c >> 8)
	dst[3] = byte(c)
}
