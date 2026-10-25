package socket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// pairEngine implements Engine for the PAIR socket type.
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
	pipe *pipe.End
	// retired holds detached pipes that may still have inbound messages
	// buffered, oldest first. Recv drains them before the current pipe so
	// a reconnect never reorders or loses what the old peer already sent.
	retired []*pipe.End
	lifecycle
	attached broadcast // woken on attach

	gates
	counters
}

// NewPairEngine returns a new PAIR socket engine.
func NewPairEngine() Engine {
	e := &pairEngine{gates: newGates()}
	e.init()
	return e
}

func (e *pairEngine) AttachPipe(p *pipe.End) error {
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
	e.pipe = p
	// The pipe arms the in-gate itself: CanRecvNow marks it idle when it
	// finds nothing, and the next message runs this. Start idle, so a
	// Poller that parked while no peer was attached wakes too.
	p.SetNotify(e.armIn)
	if p.MarkIdle() {
		e.armIn()
	}
	// Wake any Send/Recv goroutines parked waiting for a peer.
	e.attached.wake()
	e.armOut()
	return nil
}

func (e *pairEngine) DetachPipe(p *pipe.End) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pipe != nil && e.pipe == p {
		e.retireLocked(e.pipe)
	}
	return nil
}

// retireLocked frees the peer slot held by p (a no-op if p no longer holds
// it, so the socket layer's DetachPipe and a Send/Recv that observed the
// close can race in either order) and queues p for Recv to drain. Retired
// pipes with nothing left buffered are dropped so a PAIR that never calls
// Recv does not accumulate them across reconnects.
func (e *pairEngine) retireLocked(p *pipe.End) {
	if e.pipe != p {
		return
	}
	e.pipe = nil
	kept := e.retired[:0]
	for _, r := range e.retired {
		if r.Pending() {
			kept = append(kept, r)
		}
	}
	clear(e.retired[len(kept):])
	e.retired = append(kept, p)
}

// dropRetired removes a fully drained pipe from the retired queue.
func (e *pairEngine) dropRetired(p *pipe.End) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if i := slices.Index(e.retired, p); i >= 0 {
		e.retired = slices.Delete(e.retired, i, i+1)
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
		attached := e.attached.wait()
		e.mu.Unlock()

		if p == nil {
			if err := awaitPeer(ctx, attached, e.closeCh); err != nil {
				return fmt.Errorf("pair send: %w", err)
			}
			continue
		}
		// SendOr refuses a closed pipe even if it has room, so the message
		// waits for the next peer instead of vanishing into it.
		switch err := p.SendOr(ctx, m, e.closeCh); {
		case err == nil:
			e.sent.Add(1)
			return nil
		case errors.Is(err, pipe.ErrAborted):
			return fmt.Errorf("pair send: %w", zmqerr.ErrSocketClosed)
		case errors.Is(err, io.ErrClosedPipe):
			e.retire(p)
		default:
			return err
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
		var old *pipe.End
		if len(e.retired) > 0 {
			old = e.retired[0]
		}
		p := e.pipe
		attached := e.attached.wait()
		e.mu.Unlock()

		if old != nil {
			// A retired pipe is closed or detached: only what is already
			// queued counts, and TryRecv hands all of it out.
			if rm, ok := old.TryRecv(); ok {
				e.recv.Add(1)
				return rm, nil
			}
			e.dropRetired(old)
			continue
		}
		if p == nil {
			if err := awaitPeer(ctx, attached, e.closeCh); err != nil {
				return nil, fmt.Errorf("pair recv: %w", err)
			}
			continue
		}
		// On peer loss (EOF: closed and empty) the pipe is retired.
		switch rm, err := p.RecvOr(ctx, e.closeCh); {
		case err == nil:
			e.recv.Add(1)
			return rm, nil
		case errors.Is(err, pipe.ErrAborted):
			return nil, fmt.Errorf("pair recv: %w", zmqerr.ErrSocketClosed)
		case errors.Is(err, io.EOF):
			e.retire(p)
		default:
			return nil, err
		}
	}
}

func (e *pairEngine) retire(p *pipe.End) {
	e.mu.Lock()
	e.retireLocked(p)
	e.mu.Unlock()
}

// Metrics reports the PAIR engine's counters; QueueDepth counts the
// messages queued in the attached and retired pipes.
func (e *pairEngine) Metrics() Metrics {
	e.mu.Lock()
	var depth int
	if e.pipe != nil {
		depth = e.pipe.Len()
	}
	for _, r := range e.retired {
		depth += r.Len()
	}
	e.mu.Unlock()
	return e.snapshot(uint64(depth))
}

func (e *pairEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.markClosed() {
		return nil
	}
	e.pipe = nil
	e.retired = nil
	return nil
}

func (e *pairEngine) CanRecvNow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false
	}
	for _, r := range e.retired {
		if r.Pending() {
			return true
		}
	}
	// MarkIdle registers the wake-up a Poller needs when this is false.
	return e.pipe != nil && e.pipe.MarkIdle()
}

func (e *pairEngine) CanSendNow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.closed && e.pipe != nil
}
