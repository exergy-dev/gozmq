package socket

import (
	"context"
	"fmt"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/msg"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
)

// --------------------------------------------------------------------------
// Simple engine combinators
// --------------------------------------------------------------------------

type sendOnlyEngine struct {
	name string
	lb   *LoadBalancer
}

func newSendOnlyEngine(name string) iface.SocketEngine {
	return &sendOnlyEngine{name: name, lb: NewLoadBalancer()}
}

func (e *sendOnlyEngine) AttachPipe(p iface.Pipe) error { return e.lb.Attach(p) }
func (e *sendOnlyEngine) DetachPipe(p iface.Pipe) error { return e.lb.Detach(p) }
func (e *sendOnlyEngine) Send(ctx context.Context, m *msg.Msg) error {
	return e.lb.Send(ctx, m)
}
func (e *sendOnlyEngine) Recv(context.Context) (*msg.Msg, error) {
	return nil, fmt.Errorf("%s recv: %w", e.name, zmqerr.ErrFSMViolation)
}
func (e *sendOnlyEngine) Close() error                { return e.lb.Close() }
func (e *sendOnlyEngine) ReadyRecv() <-chan struct{}  { return nil }
func (e *sendOnlyEngine) ReadyWrite() <-chan struct{} { return e.lb.ReadyWrite() }
func (e *sendOnlyEngine) CanRecvNow() bool            { return false }
func (e *sendOnlyEngine) CanSendNow() bool            { return e.lb.HasPeers() }
func (e *sendOnlyEngine) Metrics() iface.SocketMetrics {
	return iface.SocketMetrics{Sent: e.lb.Sent()}
}

type recvOnlyEngine struct {
	name string
	fq   *FairQueue
}

func newRecvOnlyEngine(name string) iface.SocketEngine {
	return &recvOnlyEngine{name: name, fq: NewFairQueue()}
}

func (e *recvOnlyEngine) AttachPipe(p iface.Pipe) error { return e.fq.Attach(p) }
func (e *recvOnlyEngine) DetachPipe(p iface.Pipe) error { return e.fq.Detach(p) }
func (e *recvOnlyEngine) Send(context.Context, *msg.Msg) error {
	return fmt.Errorf("%s send: %w", e.name, zmqerr.ErrFSMViolation)
}
func (e *recvOnlyEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	return e.fq.Recv(ctx)
}
func (e *recvOnlyEngine) Close() error                { return e.fq.Close() }
func (e *recvOnlyEngine) ReadyRecv() <-chan struct{}  { return e.fq.ReadyRecv() }
func (e *recvOnlyEngine) ReadyWrite() <-chan struct{} { return nil }
func (e *recvOnlyEngine) CanRecvNow() bool            { return e.fq.HasPending() }
func (e *recvOnlyEngine) CanSendNow() bool            { return false }
func (e *recvOnlyEngine) Metrics() iface.SocketMetrics {
	return iface.SocketMetrics{
		Recv:       e.fq.Recvd(),
		Dropped:    e.fq.Dropped(),
		QueueDepth: e.fq.QueueDepth(),
	}
}

type queuePairEngine struct {
	lb *LoadBalancer
	fq *FairQueue
}

func newQueuePairEngine() iface.SocketEngine {
	return &queuePairEngine{lb: NewLoadBalancer(), fq: NewFairQueue()}
}

func (e *queuePairEngine) AttachPipe(p iface.Pipe) error {
	if err := e.lb.Attach(p); err != nil {
		return err
	}
	if err := e.fq.Attach(p); err != nil {
		_ = e.lb.Detach(p)
		return err
	}
	return nil
}

func (e *queuePairEngine) DetachPipe(p iface.Pipe) error {
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

func (e *queuePairEngine) ReadyRecv() <-chan struct{}  { return e.fq.ReadyRecv() }
func (e *queuePairEngine) ReadyWrite() <-chan struct{} { return e.lb.ReadyWrite() }
func (e *queuePairEngine) CanRecvNow() bool            { return e.fq.HasPending() }
func (e *queuePairEngine) CanSendNow() bool            { return e.lb.HasPeers() }
func (e *queuePairEngine) Metrics() iface.SocketMetrics {
	return iface.SocketMetrics{
		Sent:       e.lb.Sent(),
		Recv:       e.fq.Recvd(),
		Dropped:    e.fq.Dropped(),
		QueueDepth: e.fq.QueueDepth(),
	}
}
