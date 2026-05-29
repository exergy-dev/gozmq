package socket

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// routerEngine implements iface.SocketEngine for the ROUTER socket type.
//
// ROUTER is the identity-aware counterpart to DEALER. Most of its behavior —
// identity assignment, fair-queued recv with identity stamping, identity-keyed
// send routing, Poller signalling — lives in the shared identityRouter base.
// ROUTER adds: a configurable mandatory flag, a dropped counter for silent
// route-misses, and a bounded post-detach identity cache.
//
// Send: the outbound message's leading frame is the peer identity; the engine
// routes the remaining frames to the matching pipe. An unknown identity is a
// silent drop (mandatory=false, libzmq default) or returns ErrRouteUnknown
// (mandatory=true).
//
// Recv is race-safe against concurrent Detach: a message enqueued by a pipe's
// forwarder before Detach ran still resolves to that pipe's original identity
// via the post-detach side cache (detachedIdOf), keyed by the iface.Pipe
// pointer so a newer peer with a colliding PeerIdentity is never confused for
// the departed original.

// routerDetachCacheCap bounds the side cache of identities for recently
// detached pipes. A small FIFO cap suffices: the FairQueue inbound channel has
// capacity 1 and a detached pipe's forwarder exits shortly after Detach, so the
// window in which it can still produce messages is short. Under extreme churn a
// handful of in-flight messages predating eviction fall back to the pipe's
// advertised PeerIdentity (never cross-wired to a newer peer, since the key is
// the pipe pointer).
const routerDetachCacheCap = 1024

type routerEngine struct {
	identityRouter

	mandatory bool
	dropped   atomic.Uint64 // non-mandatory routes with unknown identity

	// detachedIdOf retains, post-Detach, the identity in use for a pipe at
	// Detach time, so Recv can still stamp in-flight messages correctly.
	// Bounded to routerDetachCacheCap, FIFO eviction via detachedOrder.
	detachedIdOf  map[iface.Pipe][]byte
	detachedOrder []iface.Pipe
}

// NewRouterEngine returns a new ROUTER socket engine. If mandatory is true,
// Send returns zmqerr.ErrRouteUnknown when the leading identity frame does not
// match an attached peer; otherwise such sends are silently dropped.
func NewRouterEngine(mandatory bool) iface.SocketEngine {
	e := &routerEngine{
		identityRouter: newIdentityRouter("router"),
		mandatory:      mandatory,
		detachedIdOf:   make(map[iface.Pipe][]byte),
	}
	// Wire the base hooks to ROUTER's post-detach identity cache.
	e.postAttachLocked = e.removeDetachedLocked
	e.postDetachLocked = e.rememberDetachedLocked
	e.extraIDLocked = func(src iface.Pipe) []byte { return e.detachedIdOf[src] }
	e.postCloseLocked = func() { e.detachedIdOf = nil; e.detachedOrder = nil }
	return e
}

// rememberDetachedLocked stores (pipe -> identity) in the post-detach side
// cache, evicting the oldest entry if full. Caller must hold e.mu.
func (e *routerEngine) rememberDetachedLocked(p iface.Pipe, id []byte) {
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
func (e *routerEngine) removeDetachedLocked(p iface.Pipe) {
	if _, ok := e.detachedIdOf[p]; !ok {
		return
	}
	delete(e.detachedIdOf, p)
	if i := slices.Index(e.detachedOrder, p); i >= 0 {
		e.detachedOrder = slices.Delete(e.detachedOrder, i, i+1)
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
		// Silent drop — counted so observers see the unknown-identity miss.
		e.dropped.Add(1)
		return nil
	}

	// Route everything after the identity frame. Clone the frame-slice header
	// so we don't share backing storage with the caller's message.
	routed := msg.FromFrames(slices.Clone(rest)...)
	if err := p.Send(ctx, routed); err != nil {
		return err
	}
	e.sent.Add(1)
	return nil
}

// Metrics reports the ROUTER counters. Dropped covers silent non-mandatory
// route-misses plus the FairQueue's ctx-cancel/close drops.
func (e *routerEngine) Metrics() iface.SocketMetrics {
	return e.metrics(e.dropped.Load())
}

var (
	_ iface.SocketEngine = (*routerEngine)(nil)
	_ Pollable           = (*routerEngine)(nil)
)
