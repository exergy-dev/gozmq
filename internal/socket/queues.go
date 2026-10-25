package socket

import (
	"context"
	"fmt"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// lbfqEngine is the engine behind PUSH, PULL and DEALER: a LoadBalancer for
// the send direction, a FairQueue for the receive direction, or both. A
// direction the pattern lacks is a nil half: its calls return
// zmqerr.ErrFSMViolation (wrapped), its readiness checks answer false, and
// it contributes nothing to Metrics.
//
//   - PUSH (send-only): messages are dispatched round-robin across the
//     attached peer pipes; a peer at HWM is rotated past (libzmq lb_t
//     parity), and Send blocks only when EVERY attached peer is full, until
//     one drains, ctx cancels, or the engine closes.
//   - PULL (recv-only): messages are fair-queued across the attached peer
//     pipes, which Recv reads round-robin. Recv blocks until some peer has
//     a message, ctx cancels, or the engine closes.
//   - DEALER: the asynchronous request/reply peer, ROUTER's counterpart
//     without identity bookkeeping. There is no lock-step FSM: Send and
//     Recv may interleave freely from multiple goroutines, and any
//     REQ-style empty-delimiter framing a downstream ROUTER/REP expects is
//     the application's responsibility.
type lbfqEngine struct {
	name string // for error messages
	gates
	lb *LoadBalancer
	fq *FairQueue
}

// NewPushEngine returns a new PUSH socket engine.
func NewPushEngine() Engine {
	g := newGates()
	return &lbfqEngine{name: "push", gates: g, lb: NewLoadBalancer(g.out)}
}

// NewPullEngine returns a new PULL socket engine.
func NewPullEngine() Engine {
	g := newGates()
	return &lbfqEngine{name: "pull", gates: g, fq: NewFairQueue(g.in)}
}

// NewDealerEngine returns a new DEALER socket engine.
func NewDealerEngine() Engine {
	g := newGates()
	return &lbfqEngine{name: "dealer", gates: g, lb: NewLoadBalancer(g.out), fq: NewFairQueue(g.in)}
}

func (e *lbfqEngine) AttachPipe(p *pipe.End) error {
	if e.lb != nil {
		if err := e.lb.Attach(p); err != nil {
			return err
		}
	}
	if e.fq != nil {
		if err := e.fq.Attach(p); err != nil {
			if e.lb != nil {
				_ = e.lb.Detach(p)
			}
			return err
		}
	}
	return nil
}

func (e *lbfqEngine) DetachPipe(p *pipe.End) error {
	if e.lb != nil {
		_ = e.lb.Detach(p)
	}
	if e.fq != nil {
		_ = e.fq.Detach(p)
	}
	return nil
}

func (e *lbfqEngine) Send(ctx context.Context, m *msg.Msg) error {
	if e.lb == nil {
		return fmt.Errorf("%s send: %w", e.name, zmqerr.ErrFSMViolation)
	}
	return e.lb.Send(ctx, m)
}

func (e *lbfqEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	if e.fq == nil {
		return nil, fmt.Errorf("%s recv: %w", e.name, zmqerr.ErrFSMViolation)
	}
	return e.fq.Recv(ctx)
}

func (e *lbfqEngine) Close() error {
	if e.lb != nil {
		_ = e.lb.Close()
	}
	if e.fq != nil {
		_ = e.fq.Close()
	}
	return nil
}

func (e *lbfqEngine) CanRecvNow() bool { return e.fq != nil && e.fq.CanRecv() }
func (e *lbfqEngine) CanSendNow() bool { return e.lb != nil && e.lb.HasPeers() }

func (e *lbfqEngine) Metrics() Metrics {
	var m Metrics
	if e.lb != nil {
		m.Sent = e.lb.Sent()
	}
	if e.fq != nil {
		m.Recv, m.QueueDepth = e.fq.Recvd(), e.fq.QueueDepth()
	}
	return m
}
