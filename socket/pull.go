package socket

import (
	"context"
	"fmt"
	"sync"

	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
)

// pullEngine implements iface.SocketEngine for the PULL socket type.
//
// PULL is recv-only. Inbound messages are fair-queued across the attached
// peer pipes by a shared FairQueue: each pipe gets a forwarder goroutine
// that pumps its Recv into a single channel, and PULL.Recv returns the
// next message from whichever peer produced one first. Recv blocks until
// some peer has a message, ctx cancels, or the engine closes. Send always
// returns zmqerr.ErrFSMViolation (wrapped); PULL has no outbound path.
type pullEngine struct {
	fq *FairQueue

	mu     sync.Mutex
	closed bool
}

// NewPullEngine returns a new PULL socket engine.
func NewPullEngine() iface.SocketEngine {
	return &pullEngine{fq: NewFairQueue()}
}

func (e *pullEngine) AttachPipe(p iface.Pipe) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("pull attach: %w", zmqerr.ErrSocketClosed)
	}
	e.mu.Unlock()
	return e.fq.Attach(p)
}

func (e *pullEngine) DetachPipe(p iface.Pipe) error {
	return e.fq.Detach(p)
}

func (e *pullEngine) Send(ctx context.Context, m *msg.Msg) error {
	return fmt.Errorf("pull send: %w", zmqerr.ErrFSMViolation)
}

func (e *pullEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	return e.fq.Recv(ctx)
}

func (e *pullEngine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()
	return e.fq.Close()
}
