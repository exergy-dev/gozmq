package gozmq

import (
	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/session"
	"github.com/exergy-dev/gozmq/internal/stream"
)

// peerLink unifies the LINGER-drain and teardown of an attached peer so the
// Socket holds one []peerLink and runs one close path instead of three typed
// slices (sessions / stream adapters / direct pipes) with their own loops.
//
// The two-phase drain mirrors CloseCtx's design §6.4 sequence:
//   - beginDrain signals "no more inbound; flush what's queued, keep the
//     conn alive" (session: CancelWriter; direct/stream: nothing to do).
//   - drained fires when the outbound flush is complete (session:
//     WriterFlushed; direct pipe: the peer received everything we sent;
//     stream: never linger-drained, so a pre-closed channel).
//   - stop is the hard teardown run after the drain (session: Stop;
//     direct: pipe.Close; stream: adapter.Close).
type peerLink interface {
	beginDrain()
	drained() <-chan struct{}
	stop() error
}

// alreadyClosed is a shared pre-closed channel for links that have no drain
// phase (stream adapters): selecting on it returns immediately.
var alreadyClosed = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// sessionLink wraps an session.Session (server-accepted, or a reconnector's
// current client session collected at close time).
type sessionLink struct{ s session.Session }

func (l sessionLink) beginDrain()              { l.s.CancelWriter() }
func (l sessionLink) drained() <-chan struct{} { return l.s.WriterFlushed() }
func (l sessionLink) stop() error              { return l.s.Stop() }

// pipeLink wraps a direct (inproc-bypass) engine pipe. Its drain ends once
// the peer socket has received everything we sent: the peer is usually
// alive and would never close its end, so waiting for that held Close for
// the full linger.
type pipeLink struct{ p *pipe.End }

func (l pipeLink) beginDrain()              {}
func (l pipeLink) drained() <-chan struct{} { return l.p.SendDrained() }
func (l pipeLink) stop() error              { _ = l.p.Close(); return nil }

// streamLink wraps a raw-bytes STREAM adapter. STREAM is not linger-drained
// (its writer half-closes or exits on ctx), so drained returns immediately.
type streamLink struct{ ad *stream.Adapter }

func (l streamLink) beginDrain()              {}
func (l streamLink) drained() <-chan struct{} { return alreadyClosed }
func (l streamLink) stop() error              { return l.ad.Close() }
