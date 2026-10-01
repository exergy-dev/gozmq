package socket

import (
	"context"
	"fmt"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// --------------------------------------------------------------------------
// Simple engine combinators
// --------------------------------------------------------------------------

type sendOnlyEngine struct {
	gates
	name string
	lb   *LoadBalancer
}

func newSendOnlyEngine(name string) Engine {
	g := newGates()
	return &sendOnlyEngine{gates: g, name: name, lb: NewLoadBalancer(g.out)}
}

func (e *sendOnlyEngine) AttachPipe(p *pipe.End) error { return e.lb.Attach(p) }
func (e *sendOnlyEngine) DetachPipe(p *pipe.End) error { return e.lb.Detach(p) }
func (e *sendOnlyEngine) Send(ctx context.Context, m *msg.Msg) error {
	return e.lb.Send(ctx, m)
}
func (e *sendOnlyEngine) Recv(context.Context) (*msg.Msg, error) {
	return nil, fmt.Errorf("%s recv: %w", e.name, zmqerr.ErrFSMViolation)
}
func (e *sendOnlyEngine) Close() error     { return e.lb.Close() }
func (e *sendOnlyEngine) CanRecvNow() bool { return false }
func (e *sendOnlyEngine) CanSendNow() bool { return e.lb.HasPeers() }
func (e *sendOnlyEngine) Metrics() Metrics {
	return Metrics{Sent: e.lb.Sent()}
}

type recvOnlyEngine struct {
	gates
	name string
	fq   *FairQueue
}

func newRecvOnlyEngine(name string) Engine {
	g := newGates()
	return &recvOnlyEngine{gates: g, name: name, fq: NewFairQueue(g.in)}
}

func (e *recvOnlyEngine) AttachPipe(p *pipe.End) error { return e.fq.Attach(p) }
func (e *recvOnlyEngine) DetachPipe(p *pipe.End) error { return e.fq.Detach(p) }
func (e *recvOnlyEngine) Send(context.Context, *msg.Msg) error {
	return fmt.Errorf("%s send: %w", e.name, zmqerr.ErrFSMViolation)
}
func (e *recvOnlyEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	return e.fq.Recv(ctx)
}
func (e *recvOnlyEngine) Close() error     { return e.fq.Close() }
func (e *recvOnlyEngine) CanRecvNow() bool { return e.fq.CanRecv() }
func (e *recvOnlyEngine) CanSendNow() bool { return false }
func (e *recvOnlyEngine) Metrics() Metrics {
	return Metrics{
		Recv:       e.fq.Recvd(),
		QueueDepth: e.fq.QueueDepth(),
	}
}

type queuePairEngine struct {
	gates
	lb *LoadBalancer
	fq *FairQueue
}

func newQueuePairEngine() Engine {
	g := newGates()
	return &queuePairEngine{gates: g, lb: NewLoadBalancer(g.out), fq: NewFairQueue(g.in)}
}

func (e *queuePairEngine) AttachPipe(p *pipe.End) error {
	if err := e.lb.Attach(p); err != nil {
		return err
	}
	if err := e.fq.Attach(p); err != nil {
		_ = e.lb.Detach(p)
		return err
	}
	return nil
}

func (e *queuePairEngine) DetachPipe(p *pipe.End) error {
	_ = e.lb.Detach(p)
	_ = e.fq.Detach(p)
	return nil
}

func (e *queuePairEngine) Send(ctx context.Context, m *msg.Msg) error {
	return e.lb.Send(ctx, m)
}

func (e *queuePairEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	return e.fq.Recv(ctx)
}

func (e *queuePairEngine) Close() error {
	_ = e.lb.Close()
	_ = e.fq.Close()
	return nil
}

func (e *queuePairEngine) CanRecvNow() bool { return e.fq.CanRecv() }
func (e *queuePairEngine) CanSendNow() bool { return e.lb.HasPeers() }
func (e *queuePairEngine) Metrics() Metrics {
	return Metrics{
		Sent:       e.lb.Sent(),
		Recv:       e.fq.Recvd(),
		QueueDepth: e.fq.QueueDepth(),
	}
}
