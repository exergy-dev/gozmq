package socket

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"slices"
	"sync"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// identityRouter is the shared core of the identity-addressed engines
// (ROUTER and STREAM). Both map each attached pipe to a routing identity,
// fair-queue inbound messages, stamp the source identity as the leading recv
// frame, and route outbound messages by their leading identity frame. The
// engines differ only in their own Send framing and Metrics, plus whether
// the post-detach identity cache below is enabled (ROUTER yes, STREAM no).
//
// Concurrency: idOf/pipeOf/closed and the detach cache are guarded by mu;
// sent/recv are atomic.
type identityRouter struct {
	fq   *FairQueue
	name string // engine name for error messages ("router"/"stream")

	mu     sync.Mutex
	closed bool
	// idOf maps an attached pipe to its routing identity (peer-advertised
	// PeerIdentity, or a synthesized 5-byte identity for anonymous peers).
	idOf map[iface.Pipe][]byte
	// pipeOf maps a routing identity (string key) back to the pipe.
	// Last-writer-wins on collision (a reconnecting peer displaces the
	// stale entry), matching libzmq.
	pipeOf map[string]iface.Pipe
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
	detachedIdOf  map[iface.Pipe][]byte
	detachedOrder []iface.Pipe
}

func newIdentityRouter(name string, detachCache bool) identityRouter {
	var detached map[iface.Pipe][]byte
	if detachCache {
		detached = make(map[iface.Pipe][]byte)
	}
	return identityRouter{
		fq:           NewFairQueue(),
		name:         name,
		idOf:         make(map[iface.Pipe][]byte),
		pipeOf:       make(map[string]iface.Pipe),
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
func (e *identityRouter) rememberDetachedLocked(p iface.Pipe, id []byte) {
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
func (e *identityRouter) removeDetachedLocked(p iface.Pipe) {
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
func (e *identityRouter) AttachPipe(p iface.Pipe) error {
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
		id = synthesizedIdentity()
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
func (e *identityRouter) DetachPipe(p iface.Pipe) error {
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
	// Build a fresh frame slice to avoid aliasing m's caller-observable
	// frames, then transfer pooled-buffer ownership. id is already the
	// defensive clone made once at AttachPipe (or the post-detach cache /
	// PeerIdentity fallback) and is never mutated afterward, so prepend it
	// directly without re-cloning per Recv. The out slice is built locally
	// and handed off, so FromFramesOwned adopts it without a second copy.
	orig := m.Frames()
	out := make([][]byte, 0, len(orig)+1)
	out = append(out, id)
	out = append(out, orig...)
	e.recv.Add(1)
	result := msg.FromFramesOwned(out)
	result.TransferPool(m)
	return result, nil
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
func (e *identityRouter) metrics(extraDropped uint64) iface.SocketMetrics {
	return e.snapshot(extraDropped+e.fq.Dropped(), e.fq.QueueDepth())
}

// synthesizedIdentity returns a 5-byte routing identity for an anonymous peer:
// byte 0 is 0 (the libzmq marker for an auto-assigned identity), the remaining
// 4 bytes are uniformly random. Collisions are astronomically unlikely and are
// handled harmlessly by pipeOf (last-writer-wins).
func synthesizedIdentity() []byte {
	out := make([]byte, 5)
	// byte 0 stays zero; fill 1..4 with randomness.
	if _, err := rand.Read(out[1:]); err != nil {
		// crypto/rand failing is an operational catastrophe; fall back to a
		// deterministic but still unique-per-process identity so we don't
		// panic in a library context.
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
