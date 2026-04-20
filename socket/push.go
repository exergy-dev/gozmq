package socket

import (
	"context"
	"fmt"
	"sync"

	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
)

// pushEngine implements iface.SocketEngine for the PUSH socket type.
//
// PUSH is send-only. Outbound messages are dispatched round-robin across
// the attached peer pipes using a shared LoadBalancer; if the chosen peer
// is at HWM, Send blocks (strict cursor — PUSH does not rotate past a
// full peer) until the peer drains, ctx cancels, or the engine closes.
// Recv always returns zmqerr.ErrFSMViolation (wrapped); PUSH has no
// inbound path.
type pushEngine struct {
	lb *LoadBalancer

	mu     sync.Mutex
	closed bool
}

// NewPushEngine returns a new PUSH socket engine.
func NewPushEngine() iface.SocketEngine {
	return &pushEngine{lb: NewLoadBalancer()}
}

func (e *pushEngine) AttachPipe(p iface.Pipe) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("push attach: %w", zmqerr.ErrSocketClosed)
	}
	e.mu.Unlock()
	return e.lb.Attach(p)
}

func (e *pushEngine) DetachPipe(p iface.Pipe) error {
	return e.lb.Detach(p)
}

func (e *pushEngine) Send(ctx context.Context, m *msg.Msg) error {
	return e.lb.Send(ctx, m)
}

func (e *pushEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	return nil, fmt.Errorf("push recv: %w", zmqerr.ErrFSMViolation)
}

func (e *pushEngine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()
	return e.lb.Close()
}
