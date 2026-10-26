package socket

import (
	"context"
	"fmt"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// routerEngine implements Engine for the ROUTER socket type.
//
// ROUTER is the identity-aware counterpart to DEALER. Most of its behavior —
// identity assignment, fair-queued recv with identity stamping, identity-keyed
// send routing, Poller signalling — lives in the shared identityRouter base.
// ROUTER adds: a configurable mandatory flag and a dropped counter for silent
// route-misses.
//
// Send: the outbound message's leading frame is the peer identity; the engine
// routes the remaining frames to the matching pipe. An unknown identity — or a
// peer whose pipe closed before it was detached — is a silent drop
// (mandatory=false, libzmq default) or returns ErrRouteUnknown
// (mandatory=true).
//
// Recv is race-safe against concurrent Detach: the identity travels with the
// source pipe (see identityRouter).

type routerEngine struct {
	identityRouter

	mandatory bool
	// dropped (promoted from identityRouter.counters) counts non-mandatory
	// routes with an unknown identity or a vanished peer.
}

// NewRouterEngine returns a new ROUTER socket engine. If mandatory is true,
// Send returns zmqerr.ErrRouteUnknown when the leading identity frame does not
// match an attached peer; otherwise such sends are silently dropped.
func NewRouterEngine(mandatory bool) Engine {
	return &routerEngine{
		identityRouter: newIdentityRouter("router"),
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

	p, err := e.lookup(id, "router", e.mandatory)
	if p == nil {
		if err == nil {
			m.Release()
		}
		return err
	}
	// Route everything after the identity frame in a message derived from
	// m (m itself is never modified; the frame bytes are shared
	// read-only). The derived message carries its own reference, so m's
	// is released whatever happens to it.
	if err := e.deliver(ctx, p, m.Derive(rest...), id, "router", e.mandatory); err != nil {
		return err
	}
	m.Release()
	return nil
}

var _ Engine = (*routerEngine)(nil)
