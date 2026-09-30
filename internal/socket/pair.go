package socket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// pairEngine implements iface.SocketEngine for the PAIR socket type.
//
// PAIR is exclusive 1:1: at most one peer pipe may be attached at a time.
// Send forwards on that pipe; Recv reads from it. Send and Recv block
// waiting for a peer when none is attached and unblock as soon as
// AttachPipe succeeds, the caller's context cancels, or the engine is
// closed. A second AttachPipe while a live peer is bound returns
// zmqerr.ErrFSMViolation (wrapped).
//
// Peer loss is not an error, matching libzmq, which simply blocks: when
// the attached pipe closes (observed by Send/Recv, by DetachPipe, or by an
// AttachPipe that finds the old pipe already closed) it is retired, which
// frees the slot for the next peer. Recv first delivers whatever a retired
// pipe still has buffered, then waits for the next peer; a Send racing the
// close retries on the next peer.
type pairEngine struct {
	mu   sync.Mutex
	pipe chanPipe
	// retired holds detached pipes that may still have inbound messages
	// buffered, oldest first. Recv drains them before the current pipe so
	// a reconnect never reorders or loses what the old peer already sent.
	retired  []chanPipe
	closed   bool
	closeCh  chan struct{}
	notifyCh chan struct{} // closed on attach; a fresh one is made if the pipe detaches
	recvGate readyGate
	sendGate readyGate

	counters
}

// NewPairEngine returns a new PAIR socket engine.
func NewPairEngine() iface.SocketEngine {
	return &pairEngine{
		closeCh:  make(chan struct{}),
		notifyCh: make(chan struct{}),
		recvGate: newReadyGate(),
		sendGate: newReadyGate(),
	}
}

func (e *pairEngine) AttachPipe(p iface.Pipe) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("pair attach: %w", zmqerr.ErrSocketClosed)
	}
	if e.pipe != nil {
		select {
		case <-e.pipe.Closed():
			// The old peer is gone but its DetachPipe has not landed yet
			// (the accept path can win that race); don't refuse its
			// successor.
			e.retireLocked(e.pipe)
		default:
			return fmt.Errorf("pair attach: already bound to a peer: %w", zmqerr.ErrFSMViolation)
		}
	}
	e.pipe = asChanPipe(p)
	// Arm the Poller's recv gate straight from the pipe's data path so a
	// blocking Poll wakes when a message arrives (no forwarder goroutine
	// exists to do it).
	e.pipe.SetRecvReady(e.recvGate.ch)
	// Wake any Send/Recv goroutines parked waiting for a peer. Swap the
	// notify channel for a fresh one so a future detach/reattach cycle
	// can wake new waiters.
	close(e.notifyCh)
	e.notifyCh = make(chan struct{})
	e.sendGate.arm()
	return nil
}

func (e *pairEngine) DetachPipe(p iface.Pipe) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pipe != nil && iface.Pipe(e.pipe) == p {
		e.retireLocked(e.pipe)
	}
	return nil
}

// retireLocked frees the peer slot held by p (a no-op if p no longer holds
// it, so the socket layer's DetachPipe and a Send/Recv that observed the
// close can race in either order) and queues p for Recv to drain. Retired
// pipes with nothing left buffered are dropped so a PAIR that never calls
// Recv does not accumulate them across reconnects.
func (e *pairEngine) retireLocked(p chanPipe) {
	if e.pipe != p {
		return
	}
	e.pipe = nil
	kept := e.retired[:0]
	for _, r := range e.retired {
		if len(r.RecvCh()) > 0 {
			kept = append(kept, r)
		}
	}
	clear(e.retired[len(kept):])
	e.retired = append(kept, p)
}

// dropRetired removes a fully drained pipe from the retired queue.
func (e *pairEngine) dropRetired(p chanPipe) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, r := range e.retired {
		if r == p {
			e.retired = append(e.retired[:i], e.retired[i+1:]...)
			return
		}
	}
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
		// A closed pipe may still have send capacity; check first so the
		// message waits for the next peer instead of vanishing into it.
		select {
		case <-p.Closed():
			e.retire(p)
			continue
		default:
		}
		// Inline the channel send instead of spawning a per-Send goroutine,
		// racing it against engine close, peer loss, and ctx.
		select {
		case p.SendCh() <- m:
			e.sent.Add(1)
			return nil
		case <-closeCh:
			return fmt.Errorf("pair send: %w", zmqerr.ErrSocketClosed)
		case <-p.Closed():
			e.retire(p)
		case <-ctx.Done():
			return ctx.Err()
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
		var old chanPipe
		if len(e.retired) > 0 {
			old = e.retired[0]
		}
		p := e.pipe
		closeCh := e.closeCh
		notifyCh := e.notifyCh
		e.mu.Unlock()

		if old != nil {
			rm, err := takeRetired(ctx, old)
			if err != nil {
				return nil, err
			}
			if rm != nil {
				e.recv.Add(1)
				return rm, nil
			}
			e.dropRetired(old)
			continue
		}
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
		// Inline the channel read instead of spawning a per-Recv goroutine.
		// On peer loss the pipe is retired; the next iteration drains it.
		select {
		case rm := <-p.RecvCh():
			e.recv.Add(1)
			return rm, nil
		case <-closeCh:
			return nil, fmt.Errorf("pair recv: %w", zmqerr.ErrSocketClosed)
		case <-p.Closed():
			e.retire(p)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (e *pairEngine) retire(p chanPipe) {
	e.mu.Lock()
	e.retireLocked(p)
	e.mu.Unlock()
}

// takeRetired returns the next message left on a retired pipe, or
// (nil, nil) once it has nothing more to deliver.
func takeRetired(ctx context.Context, p chanPipe) (*msg.Msg, error) {
	select {
	case m := <-p.RecvCh():
		return m, nil
	default:
	}
	select {
	case <-p.Closed():
	default:
		// Detached while still open (socket shutdown): only what was
		// already buffered counts.
		return nil, nil
	}
	// The pipe's own Recv waits for an unbounded (HWM 0) direction to
	// finish flushing before reporting io.EOF, so nothing is left behind.
	m, err := p.Recv(ctx)
	if errors.Is(err, io.EOF) {
		return nil, nil
	}
	return m, err
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
	e.retired = nil
	return nil
}

func (e *pairEngine) ReadyRecv() <-chan struct{} {
	return e.recvGate.signal(e.CanRecvNow())
}

func (e *pairEngine) ReadyWrite() <-chan struct{} {
	return e.sendGate.signal(e.CanSendNow())
}

func (e *pairEngine) CanRecvNow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false
	}
	for _, r := range e.retired {
		if len(r.RecvCh()) > 0 {
			return true
		}
	}
	return e.pipe != nil && len(e.pipe.RecvCh()) > 0
}

func (e *pairEngine) CanSendNow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.closed && e.pipe != nil
}

var _ Pollable = (*pairEngine)(nil)
