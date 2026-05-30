package socket

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"sync"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// identityRouter is the shared core of the identity-addressed engines
// (ROUTER and STREAM). Both map each attached pipe to a routing identity,
// fair-queue inbound messages, stamp the source identity as the leading recv
// frame, and route outbound messages by their leading identity frame. The
// engines differ only in their Send framing and (for ROUTER) an extra
// post-detach identity cache; those differences are supplied via the optional
// *Locked hooks below plus each engine's own Send/Metrics.
//
// Concurrency: idOf/pipeOf/closed are guarded by mu; sent/recv are atomic.
// Every hook is invoked while mu is held.
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

	// Optional hooks, all invoked while mu is held (nil = no-op).
	postAttachLocked func(p iface.Pipe)            // after idOf/pipeOf set
	postDetachLocked func(p iface.Pipe, id []byte) // after idOf/pipeOf cleared
	extraIDLocked    func(src iface.Pipe) []byte   // fallback id lookup on idOf miss
	postCloseLocked  func()                        // after base state cleared
}

func newIdentityRouter(name string) identityRouter {
	return identityRouter{
		fq:     NewFairQueue(),
		name:   name,
		idOf:   make(map[iface.Pipe][]byte),
		pipeOf: make(map[string]iface.Pipe),
		sendGate: newReadyGate(),
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
	if e.postAttachLocked != nil {
		e.postAttachLocked(p)
	}
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
		if e.postDetachLocked != nil {
			e.postDetachLocked(p, id)
		}
	}
	e.mu.Unlock()
	return e.fq.Detach(p)
}

// Recv fair-queues across attached pipes and prepends the source peer's
// routing identity as the leading frame. Identity resolution tolerates a
// concurrent Detach via the extraIDLocked hook (ROUTER's post-detach cache);
// if both miss, it falls back to the pipe's advertised PeerIdentity.
func (e *identityRouter) Recv(ctx context.Context) (*msg.Msg, error) {
	m, src, err := e.fq.RecvWithSource(ctx)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	id := e.idOf[src]
	if id == nil && e.extraIDLocked != nil {
		id = e.extraIDLocked(src)
	}
	e.mu.Unlock()
	if id == nil {
		id = src.PeerIdentity()
	}
	// Build a fresh frame slice to avoid aliasing m's caller-observable
	// frames, then transfer pooled-buffer ownership.
	orig := m.Frames()
	out := make([][]byte, 0, len(orig)+1)
	out = append(out, bytes.Clone(id))
	out = append(out, orig...)
	e.recv.Add(1)
	result := msg.FromFrames(out...)
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
	if e.postCloseLocked != nil {
		e.postCloseLocked()
	}
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
