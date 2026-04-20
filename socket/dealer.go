package socket

import (
	"context"
	"fmt"
	"sync"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// dealerEngine implements iface.SocketEngine for the DEALER socket type.
//
// DEALER is an asynchronous request/reply peer — the symmetric counterpart
// to ROUTER, but without identity bookkeeping. It composes the two shared
// primitives:
//
//   - Send load-balances outbound messages round-robin across attached
//     peer pipes (reusing LoadBalancer; strict cursor — blocks on an
//     HWM-full peer rather than rotating past it).
//   - Recv fair-queues inbound messages across attached peer pipes
//     (reusing FairQueue).
//
// DEALER does not mangle identity envelopes or enforce a lock-step FSM:
// Send and Recv may interleave freely from multiple goroutines. Any
// REQ-style empty-delimiter framing required by a downstream ROUTER/REP
// peer is the application's responsibility.
//
// Thread-safe: Send, Recv, AttachPipe, DetachPipe, and Close are
// goroutine-safe via the underlying primitives plus a small mutex
// guarding the closed flag.
type dealerEngine struct {
	lb *LoadBalancer
	fq *FairQueue

	mu     sync.Mutex
	closed bool
}

// NewDealerEngine returns a new DEALER socket engine.
func NewDealerEngine() iface.SocketEngine {
	return &dealerEngine{
		lb: NewLoadBalancer(),
		fq: NewFairQueue(),
	}
}

func (e *dealerEngine) AttachPipe(p iface.Pipe) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("dealer attach: %w", zmqerr.ErrSocketClosed)
	}
	e.mu.Unlock()
	if err := e.lb.Attach(p); err != nil {
		return err
	}
	if err := e.fq.Attach(p); err != nil {
		// Best-effort rollback: if FairQueue refuses (closed), drop from
		// the LoadBalancer too so state stays consistent.
		_ = e.lb.Detach(p)
		return err
	}
	return nil
}

func (e *dealerEngine) DetachPipe(p iface.Pipe) error {
	_ = e.lb.Detach(p)
	_ = e.fq.Detach(p)
	return nil
}

func (e *dealerEngine) Send(ctx context.Context, m *msg.Msg) error {
	return e.lb.Send(ctx, m)
}

func (e *dealerEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	return e.fq.Recv(ctx)
}

func (e *dealerEngine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()
	_ = e.lb.Close()
	_ = e.fq.Close()
	return nil
}
