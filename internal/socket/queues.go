package socket

import (
	"context"
	"fmt"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// pushEngine is PUSH: send-only. Outbound messages are dispatched
// round-robin across the attached peer pipes by a LoadBalancer; a peer at
// HWM is rotated past (libzmq lb_t parity), and Send blocks only when
// EVERY attached peer is full — until one drains, ctx cancels, or the
// engine closes. Recv always returns zmqerr.ErrFSMViolation (wrapped).
type pushEngine struct {
	gates
	lb *LoadBalancer
}

// NewPushEngine returns a new PUSH socket engine.
func NewPushEngine() Engine {
	g := newGates()
	return &pushEngine{gates: g, lb: NewLoadBalancer(g.out)}
}

func (e *pushEngine) AttachPipe(p *pipe.End) error { return e.lb.Attach(p) }
func (e *pushEngine) DetachPipe(p *pipe.End) error { return e.lb.Detach(p) }
func (e *pushEngine) Send(ctx context.Context, m *msg.Msg) error {
	return e.lb.Send(ctx, m)
}
func (e *pushEngine) Recv(context.Context) (*msg.Msg, error) {
	return nil, fmt.Errorf("push recv: %w", zmqerr.ErrFSMViolation)
}
func (e *pushEngine) Close() error     { return e.lb.Close() }
func (e *pushEngine) CanRecvNow() bool { return false }
func (e *pushEngine) CanSendNow() bool { return e.lb.HasPeers() }
func (e *pushEngine) Metrics() Metrics { return Metrics{Sent: e.lb.Sent()} }

// pullEngine is PULL: recv-only. Inbound messages are fair-queued across
// the attached peer pipes by a FairQueue, which Recv reads round-robin.
// Recv blocks until some peer has a message, ctx cancels, or the engine
// closes. Send always returns zmqerr.ErrFSMViolation (wrapped).
type pullEngine struct {
	gates
	fq *FairQueue
}

// NewPullEngine returns a new PULL socket engine.
func NewPullEngine() Engine {
	g := newGates()
	return &pullEngine{gates: g, fq: NewFairQueue(g.in)}
}

func (e *pullEngine) AttachPipe(p *pipe.End) error { return e.fq.Attach(p) }
func (e *pullEngine) DetachPipe(p *pipe.End) error { return e.fq.Detach(p) }
func (e *pullEngine) Send(context.Context, *msg.Msg) error {
	return fmt.Errorf("pull send: %w", zmqerr.ErrFSMViolation)
}
func (e *pullEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	return e.fq.Recv(ctx)
}
func (e *pullEngine) Close() error     { return e.fq.Close() }
func (e *pullEngine) CanRecvNow() bool { return e.fq.CanRecv() }
func (e *pullEngine) CanSendNow() bool { return false }
func (e *pullEngine) Metrics() Metrics {
	return Metrics{Recv: e.fq.Recvd(), QueueDepth: e.fq.QueueDepth()}
}

// dealerEngine is DEALER: the asynchronous request/reply peer, ROUTER's
// counterpart without identity bookkeeping. Send load-balances across the
// attached peers (an HWM-full peer is rotated past, blocking only when
// every peer is full) and Recv fair-queues them. There is no lock-step
// FSM: Send and Recv may interleave freely from multiple goroutines, and
// any REQ-style empty-delimiter framing a downstream ROUTER/REP expects is
// the application's responsibility.
type dealerEngine struct {
	gates
	lb *LoadBalancer
	fq *FairQueue
}

// NewDealerEngine returns a new DEALER socket engine.
func NewDealerEngine() Engine {
	g := newGates()
	return &dealerEngine{gates: g, lb: NewLoadBalancer(g.out), fq: NewFairQueue(g.in)}
}

func (e *dealerEngine) AttachPipe(p *pipe.End) error {
	if err := e.lb.Attach(p); err != nil {
		return err
	}
	if err := e.fq.Attach(p); err != nil {
		_ = e.lb.Detach(p)
		return err
	}
	return nil
}

func (e *dealerEngine) DetachPipe(p *pipe.End) error {
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
	_ = e.lb.Close()
	_ = e.fq.Close()
	return nil
}

func (e *dealerEngine) CanRecvNow() bool { return e.fq.CanRecv() }
func (e *dealerEngine) CanSendNow() bool { return e.lb.HasPeers() }
func (e *dealerEngine) Metrics() Metrics {
	return Metrics{Sent: e.lb.Sent(), Recv: e.fq.Recvd(), QueueDepth: e.fq.QueueDepth()}
}
