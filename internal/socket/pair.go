package socket

import (
	"context"
	"fmt"
	"sync"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// pairEngine implements iface.SocketEngine for the PAIR socket type.
//
// PAIR is exclusive 1:1: at most one peer pipe may be attached at a time.
// Send forwards on that pipe; Recv reads from it. Send and Recv block
// waiting for a peer when none is attached and unblock as soon as
// AttachPipe succeeds, the caller's context cancels, or the engine is
// closed. A second AttachPipe while a peer is already bound returns
// zmqerr.ErrFSMViolation (wrapped). DetachPipe clears the binding; a
// subsequent AttachPipe re-arms the engine.
//
// Send/Recv do not currently fail with ErrFSMViolation when no peer has
// ever been attached — they park until one arrives (or ctx fires) to
// mirror libzmq's blocking semantics. The contract specifically reserves
// ErrFSMViolation for the "a peer is already attached" attach failure.
type pairEngine struct {
	mu       sync.Mutex
	pipe     iface.Pipe
	closed   bool
	closeCh  chan struct{}
	notifyCh chan struct{} // closed on attach; a fresh one is made if the pipe detaches
	readyR   chan struct{}
	readyW   chan struct{}

	counters
}

// NewPairEngine returns a new PAIR socket engine.
func NewPairEngine() iface.SocketEngine {
	return &pairEngine{
		closeCh:  make(chan struct{}),
		notifyCh: make(chan struct{}),
		readyR:   make(chan struct{}, 1),
		readyW:   make(chan struct{}, 1),
	}
}

func (e *pairEngine) AttachPipe(p iface.Pipe) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("pair attach: %w", zmqerr.ErrSocketClosed)
	}
	if e.pipe != nil {
		return fmt.Errorf("pair attach: already bound to a peer: %w", zmqerr.ErrFSMViolation)
	}
	e.pipe = p
	// Wake any Send/Recv goroutines parked waiting for a peer. Swap the
	// notify channel for a fresh one so a future detach/reattach cycle
	// can wake new waiters.
	close(e.notifyCh)
	e.notifyCh = make(chan struct{})
	select {
	case e.readyW <- struct{}{}:
	default:
	}
	return nil
}

func (e *pairEngine) DetachPipe(p iface.Pipe) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pipe == p {
		e.pipe = nil
	}
	return nil
}

func (e *pairEngine) Send(ctx context.Context, m *msg.Msg) error {
	for {
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return fmt.Errorf("pair send: %w", zmqerr.ErrSocketClosed)
		}
		p := e.pipe
		closeCh := e.closeCh
		notifyCh := e.notifyCh
		e.mu.Unlock()

		if p == nil {
			// Wait for an attach (notifyCh close), engine close, or ctx.
			select {
			case <-notifyCh:
				continue
			case <-closeCh:
				return fmt.Errorf("pair send: %w", zmqerr.ErrSocketClosed)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		// Race the blocking Send against close.
		childCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- p.Send(childCtx, m) }()
		select {
		case err := <-done:
			cancel()
			if err == nil {
				e.sent.Add(1)
			}
			return err
		case <-closeCh:
			cancel()
			<-done
			return fmt.Errorf("pair send: %w", zmqerr.ErrSocketClosed)
		}
	}
}

func (e *pairEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	for {
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return nil, fmt.Errorf("pair recv: %w", zmqerr.ErrSocketClosed)
		}
		p := e.pipe
		closeCh := e.closeCh
		notifyCh := e.notifyCh
		e.mu.Unlock()

		if p == nil {
			select {
			case <-notifyCh:
				continue
			case <-closeCh:
				return nil, fmt.Errorf("pair recv: %w", zmqerr.ErrSocketClosed)
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		childCtx, cancel := context.WithCancel(ctx)
		type result struct {
			m   *msg.Msg
			err error
		}
		done := make(chan result, 1)
		go func() {
			m, err := p.Recv(childCtx)
			done <- result{m: m, err: err}
		}()
		select {
		case r := <-done:
			cancel()
			if r.err == nil {
				e.recv.Add(1)
			}
			return r.m, r.err
		case <-closeCh:
			cancel()
			<-done
			return nil, fmt.Errorf("pair recv: %w", zmqerr.ErrSocketClosed)
		}
	}
}

// Metrics reports the PAIR engine's counters. PAIR has no internal
// inbound queue (it reads directly from the attached pipe), so
// QueueDepth is always zero.
func (e *pairEngine) Metrics() iface.SocketMetrics {
	return e.snapshot(0, 0)
}

func (e *pairEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	close(e.closeCh)
	e.pipe = nil
	return nil
}

func (e *pairEngine) ReadyRecv() <-chan struct{} {
	if e.CanRecvNow() {
		select {
		case e.readyR <- struct{}{}:
		default:
		}
	}
	return e.readyR
}

func (e *pairEngine) ReadyWrite() <-chan struct{} {
	if e.CanSendNow() {
		select {
		case e.readyW <- struct{}{}:
		default:
		}
	}
	return e.readyW
}

func (e *pairEngine) CanRecvNow() bool {
	e.mu.Lock()
	p := e.pipe
	closed := e.closed
	e.mu.Unlock()
	if closed || p == nil {
		return false
	}
	rc, ok := p.(recvChanner)
	return ok && len(rc.RecvCh()) > 0
}

func (e *pairEngine) CanSendNow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.closed && e.pipe != nil
}

var _ Pollable = (*pairEngine)(nil)
