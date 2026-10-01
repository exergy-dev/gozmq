package gozmq

import (
	"github.com/exergy-dev/gozmq/internal/session"
	"github.com/exergy-dev/gozmq/internal/stream"
)

// peerLink unifies the LINGER-drain and teardown of an attached peer so the
// Socket holds one []peerLink and runs one close path instead of three typed
// slices (sessions / stream adapters / inproc links) with their own loops.
//
// The two-phase drain mirrors CloseCtx's design §6.4 sequence:
//   - beginDrain signals "no more inbound; flush what's queued, keep the
//     conn alive" (session: Drain; inproc/stream: nothing to do).
//   - drained fires when the outbound flush is complete (session: Drain's
//     channel; inproc: the peer received everything we sent;
//     stream: never linger-drained, so a pre-closed channel).
//   - stop is the hard teardown run after the drain (session: Stop;
//     inproc: close and detach both sides; stream: adapter.Close).
type peerLink interface {
	beginDrain()
	drained() <-chan struct{}
	stop() error
}

// alreadyClosed is a shared pre-closed channel for links that have no drain
// phase (stream adapters, waiting inproc Connects): selecting on it returns
// immediately.
var alreadyClosed = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// sessionLink wraps a session (server-accepted or a reconnector's client
// session), registered at start and removed by its OnClosed.
type sessionLink struct{ s *session.Session }

func (l sessionLink) beginDrain()              { l.s.Drain() }
func (l sessionLink) drained() <-chan struct{} { return l.s.Drain() }
func (l sessionLink) stop() error              { return l.s.Stop() }

// streamLink wraps a raw-bytes STREAM adapter. STREAM is not linger-drained
// (its writer half-closes or exits on ctx), so drained returns immediately.
type streamLink struct{ ad *stream.Adapter }

func (l streamLink) beginDrain()              {}
func (l streamLink) drained() <-chan struct{} { return alreadyClosed }
func (l streamLink) stop() error              { return l.ad.Close() }
