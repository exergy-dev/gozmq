package socket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
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
// routes the remaining frames to the matching pipe. An unknown identity — or a
// peer whose pipe closed before it was detached — is a silent drop
// (mandatory=false, libzmq default) or returns ErrRouteUnknown
// (mandatory=true).
//
// Recv is race-safe against concurrent Detach: a message enqueued by a pipe's
// forwarder before Detach ran still resolves to that pipe's original identity
// via the post-detach side cache (identityRouter.detachedIdOf, enabled here at
// construction), keyed by the iface.Pipe pointer so a newer peer with a
// colliding PeerIdentity is never confused for the departed original.

type routerEngine struct {
	identityRouter

	mandatory bool
	// dropped (promoted from identityRouter.counters) counts non-mandatory
	// routes with an unknown identity or a vanished peer.
}

// NewRouterEngine returns a new ROUTER socket engine. If mandatory is true,
// Send returns zmqerr.ErrRouteUnknown when the leading identity frame does not
// match an attached peer; otherwise such sends are silently dropped.
func NewRouterEngine(mandatory bool) iface.SocketEngine {
	return &routerEngine{
		// detachCache=true: ROUTER keeps the post-detach identity cache so
		// Recv can stamp messages from just-detached peers (see doc above).
		identityRouter: newIdentityRouter("router", true),
		mandatory:      mandatory,
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
	// (the frame BYTES still alias m's), hand the locally-built slice off via
	// FromFramesOwned, and transfer pooled-buffer ownership so the session
	// writer's post-flush Recycle reclaims via routed instead of leaving m's
	// pooled frames live under the in-flight write (use-after-free).
	routed := msg.FromFramesOwned(slices.Clone(rest))
	routed.TransferPool(m)
	if err := p.Send(ctx, routed); err != nil {
		if !errors.Is(err, io.ErrClosedPipe) {
			return err
		}
		// The peer vanished between lookup and send: same as unknown.
		if mandatory {
			return fmt.Errorf("router send: identity %q: peer disconnected: %w", id, zmqerr.ErrRouteUnknown)
		}
		e.dropped.Add(1)
		return nil
	}
	e.sent.Add(1)
	return nil
}

// Metrics reports the ROUTER counters. Dropped covers silent non-mandatory
// route-misses (including vanished peers) plus the FairQueue's
// ctx-cancel/close drops.
func (e *routerEngine) Metrics() iface.SocketMetrics {
	return e.metrics(e.dropped.Load())
}

var (
	_ iface.SocketEngine = (*routerEngine)(nil)
	_ Pollable           = (*routerEngine)(nil)
)
