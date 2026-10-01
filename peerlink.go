package gozmq

import "github.com/exergy-dev/gozmq/internal/session"

// peerLink unifies the LINGER-drain and teardown of an attached peer so the
// Socket holds one []peerLink and runs one close path instead of typed
// slices (sessions / inproc links) with their own loops.
//
// The two-phase drain mirrors CloseCtx's design §6.4 sequence:
//   - beginDrain signals "no more inbound; flush what's queued, keep the
//     conn alive" (session: Drain; inproc: nothing to do).
//   - drained fires when the outbound flush is complete (session: Drain's
//     channel; inproc: the peer received everything we sent).
//   - stop is the hard teardown run after the drain (session: Stop;
//     inproc: close and detach both sides).
type peerLink interface {
	beginDrain()
	drained() <-chan struct{}
	stop() error
}

// alreadyClosed is a shared pre-closed channel for links that have no drain
// phase (waiting inproc Connects): selecting on it returns immediately.
var alreadyClosed = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// sessionLink wraps a session (server-accepted, a reconnector's client
// session, or a STREAM Connect's), registered at start and removed by its
// OnClosed. ep is the Connect endpoint, empty for an accepted peer.
type sessionLink struct {
	s  *session.Session
	ep string
}

func (l sessionLink) beginDrain()              { l.s.Drain() }
func (l sessionLink) drained() <-chan struct{} { return l.s.Drain() }
func (l sessionLink) stop() error              { return l.s.Stop() }
