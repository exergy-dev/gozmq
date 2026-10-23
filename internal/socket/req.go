package socket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// reqState enumerates the REQ lock-step FSM. REQ starts in canSend;
// after a successful Send it transitions to canRecv; after a successful
// Recv it returns to canSend. Calling Send in canRecv (or Recv in
// canSend) is an ErrFSMViolation.
//
// sending and receiving mark a Send or Recv in progress. They are
// reserved under e.mu before the engine lock is dropped, so a concurrent
// call sees them and fails with ErrFSMViolation instead of both passing
// the check and putting two requests (or two readers) in flight.
type reqState uint8

const (
	reqStateCanSend reqState = iota
	reqStateCanRecv
	reqStateSending
	reqStateReceiving
)

// reqEngine implements Engine for the REQ socket type.
//
// FSM: REQ enforces strict lock-step alternation — canSend → canRecv →
// canSend. Calling Send while the engine expects Recv (or vice versa)
// returns zmqerr.ErrFSMViolation (wrapped). A successful Send transitions
// to canRecv; a successful Recv (even one that observes a malformed reply)
// transitions back to canSend. A Recv that fails with a context error
// (cancel/deadline) does NOT flip the FSM: the reply is still in flight
// and a retried Recv awaits it, matching libzmq's EAGAIN semantics. Only
// peer-gone/close failures reset the FSM to canSend.
//
// Wire shape: REQ auto-prepends an empty delimiter frame to every
// outbound message before load-balancing it to one peer. Recv strips
// that delimiter and returns the remaining frames. This matches the
// REQ↔REP envelope convention (empty delimiter separates identity stack
// from body).
//
// Peer selection: outbound sends are round-robin load-balanced across
// the attached pipe set. Once Send picks a peer, that peer is sticky
// until the matching Recv completes — Recv blocks on the sticky pipe
// specifically and ignores messages from other peers. This is required
// for correctness: the REP on that peer is the only socket holding the
// request's identity envelope; no other peer can answer. If the sticky
// pipe detaches before the reply arrives the in-flight Recv surfaces
// ErrSocketClosed, stickiness is cleared, and the FSM returns to
// canSend so the caller may retry. Send also discards any messages
// still buffered on the chosen pipe from a previous, abandoned exchange
// — a late reply must never be delivered as the answer to a later
// request.
//
// Thread-safe: Send and Recv serialize against each other via the engine
// mutex plus the FSM state; a call made while another is in progress
// returns ErrFSMViolation. Recv reads the sticky pipe's receive channel
// inline — no per-request goroutine or context is allocated; the parked
// Recv unblocks on the sticky pipe's Closed() signal or on the engine's
// closeCh.
type reqEngine struct {
	mu       sync.Mutex
	closed   bool
	closeCh  chan struct{}
	attached broadcast // woken on attach
	state    reqState
	ring     pipeRing
	// sticky is the pipe picked by the most recent Send; Recv reads its
	// reply directly. Cleared after a successful Recv (or on detach).
	sticky *pipe.End

	gates
	counters
}

// reqDelimiter is the empty delimiter frame REQ prepends to each request.
// Frames are never mutated once queued, so one shared value serves all.
var reqDelimiter = [][]byte{{}}

// NewReqEngine returns a new REQ socket engine.
func NewReqEngine() Engine {
	return &reqEngine{closeCh: make(chan struct{}), gates: newGates()}
}

func (e *reqEngine) AttachPipe(p *pipe.End) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("req attach: %w", zmqerr.ErrSocketClosed)
	}
	if !e.ring.add(p) {
		return nil
	}
	// The pipe arms the in-gate itself once marked idle (see send and
	// CanRecvNow), so a blocking Poll wakes when the reply lands.
	p.SetNotify(e.armIn)
	// Wake any Send parked waiting for a peer.
	e.attached.wake()
	if e.state == reqStateCanSend {
		e.armOut()
	}
	return nil
}

func (e *reqEngine) DetachPipe(p *pipe.End) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ring.remove(p)
	// If the detached pipe was our sticky peer, the pending request is
	// unanswerable: drop the stickiness so the next Send can pick a new
	// peer. A Recv parked on the sticky pipe unblocks via the pipe's
	// Closed() signal (the session closes the pipe on detach) and surfaces
	// ErrSocketClosed. Actual libzmq drops the outstanding request here too.
	if e.sticky == p {
		e.sticky = nil
	}
	return nil
}

func (e *reqEngine) Send(ctx context.Context, m *msg.Msg) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("req send: %w", zmqerr.ErrSocketClosed)
	}
	if e.state != reqStateCanSend {
		state := e.state
		e.mu.Unlock()
		return fmt.Errorf("req send: %s: %w", state.violation(), zmqerr.ErrFSMViolation)
	}
	e.state = reqStateSending
	e.mu.Unlock()

	if err := e.send(ctx, m); err != nil {
		e.mu.Lock()
		if e.state == reqStateSending {
			e.state = reqStateCanSend
			e.armOut()
		}
		e.mu.Unlock()
		return err
	}
	return nil
}

// violation describes why a call is out of order in state s.
func (s reqState) violation() string {
	switch s {
	case reqStateCanSend:
		return "out of order (Send expected next)"
	case reqStateCanRecv:
		return "out of order (Recv expected next)"
	default:
		return "another Send or Recv is in progress"
	}
}

// send delivers one request with the FSM reserved in reqStateSending. On
// success it moves the FSM to canRecv and releases m (the wire message
// holds its own reference); on error the caller restores canSend.
func (e *reqEngine) send(ctx context.Context, m *msg.Msg) error {
	// Build wire message: empty delimiter + caller's frames.
	wire := prependFrames(reqDelimiter, m)
	queued := false
	defer func() {
		if !queued {
			wire.Release()
		}
	}()

	// Pick a peer and deliver. Retry on peer churn / closed pipe.
	for {
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return fmt.Errorf("req send: %w", zmqerr.ErrSocketClosed)
		}
		if e.ring.len() == 0 {
			notify := e.attached.wait()
			closeCh := e.closeCh
			e.mu.Unlock()
			if err := awaitPeer(ctx, notify, closeCh); err != nil {
				return fmt.Errorf("req send: %w", err)
			}
			continue
		}
		idx, p := e.ring.current()
		closeCh := e.closeCh
		e.mu.Unlock()

		// Discard anything still buffered on the chosen pipe: it predates
		// this request, so it can only be a stale reply from an earlier
		// exchange the FSM already abandoned (e.g. a malformed reply or a
		// peer-gone reset). Delivering it as this request's answer would
		// desynchronize request/reply pairing.
		for {
			stale, ok := p.TryRecv()
			if !ok {
				break
			}
			stale.Release()
		}

		// Block at HWM, racing engine close and the caller's ctx — a bare
		// pipe Send here would violate the Close-unblocks-pending-Send
		// contract.
		switch err := p.SendOr(ctx, wire, closeCh); {
		case err == nil:
			queued = true
		case errors.Is(err, io.ErrClosedPipe):
			// Peer vanished; drop it from the set and retry.
			_ = e.DetachPipe(p)
			continue
		case errors.Is(err, pipe.ErrAborted):
			return fmt.Errorf("req send: %w", zmqerr.ErrSocketClosed)
		default:
			return err
		}

		e.mu.Lock()
		if e.closed {
			// Close won this race. The peer received the request; the
			// caller will see ErrSocketClosed on the subsequent Recv.
			e.mu.Unlock()
			return fmt.Errorf("req send: %w", zmqerr.ErrSocketClosed)
		}
		// Advance cursor past p if it's still at idx.
		e.ring.advancePast(idx, p)
		// Transition to canRecv with p as the sticky pipe. Recv reads
		// p's reply directly.
		e.sticky = p
		e.state = reqStateCanRecv
		// A Poller may have parked before this request, when REQ could
		// not receive: mark the sticky pipe idle so the reply wakes it.
		if p.MarkIdle() {
			e.armIn()
		}
		e.mu.Unlock()
		m.Release()
		e.sent.Add(1)
		return nil
	}
}

func (e *reqEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, fmt.Errorf("req recv: %w", zmqerr.ErrSocketClosed)
	}
	if e.state != reqStateCanRecv {
		state := e.state
		e.mu.Unlock()
		return nil, fmt.Errorf("req recv: %s: %w", state.violation(), zmqerr.ErrFSMViolation)
	}
	e.state = reqStateReceiving
	sticky := e.sticky
	closeCh := e.closeCh
	e.mu.Unlock()

	m, err := e.recvSticky(ctx, sticky, closeCh)
	if err != nil {
		// A context error leaves the FSM in canRecv with the sticky pipe
		// intact: the reply is still in flight and a retried Recv must
		// await it (libzmq EAGAIN semantics). Peer-gone and close reset
		// to canSend so a fresh Send can pick a new peer.
		e.mu.Lock()
		if e.state == reqStateReceiving {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				e.state = reqStateCanRecv
				e.armIn() // a reply may have landed while Receiving hid it
			} else {
				e.state = reqStateCanSend
				e.sticky = nil
				e.armOut()
			}
		}
		e.mu.Unlock()
		return nil, err
	}

	stripped, serr := stripReqDelimiter(m)
	m.Release()
	// A malformed reply still flips the state so the FSM can continue.
	e.mu.Lock()
	e.state = reqStateCanSend
	e.sticky = nil
	e.armOut()
	e.mu.Unlock()
	if serr != nil {
		return nil, serr
	}
	e.recv.Add(1)
	return stripped, nil
}

// recvSticky reads one reply from the sticky pipe, racing the read against
// engine close, peer detach (pipe close), and the caller's context. It
// allocates no per-call context or goroutine — the pipe's receive channel
// is selected on inline. The returned error is already wrapped with the
// REQ recv prefix where appropriate.
func (e *reqEngine) recvSticky(ctx context.Context, sticky *pipe.End, closeCh <-chan struct{}) (*msg.Msg, error) {
	if sticky == nil {
		// Sticky peer detached between Send and Recv. Treat as peer gone.
		select {
		case <-closeCh:
			return nil, fmt.Errorf("req recv: %w", zmqerr.ErrSocketClosed)
		default:
		}
		return nil, fmt.Errorf("req recv: peer gone: %w", zmqerr.ErrSocketClosed)
	}

	switch m, err := sticky.RecvOr(ctx, closeCh); {
	case err == nil:
		return m, nil
	case errors.Is(err, pipe.ErrAborted):
		return nil, fmt.Errorf("req recv: %w", zmqerr.ErrSocketClosed)
	case errors.Is(err, io.EOF):
		// Peer gone, and any reply it sent before closing was delivered.
		return nil, fmt.Errorf("req recv: peer gone: %w", zmqerr.ErrSocketClosed)
	default:
		return nil, err
	}
}

func (e *reqEngine) CanRecvNow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || e.state != reqStateCanRecv || e.sticky == nil {
		return false
	}
	// A Recv is non-blocking-ready iff a reply is already buffered on the
	// sticky pipe; if not, MarkIdle has the reply arm the gate.
	return e.sticky.MarkIdle()
}

func (e *reqEngine) CanSendNow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.closed && e.state == reqStateCanSend && e.ring.len() > 0
}

// stripReqDelimiter removes the leading empty delimiter frame from an
// inbound REP reply. The wire shape is [empty, body...]; we return a
// *Msg derived from m holding just the body frames (a single body frame,
// the common case, fits the Msg's inline slot without allocating).
func stripReqDelimiter(m *msg.Msg) (*msg.Msg, error) {
	frames := m.Frames()
	if len(frames) < 1 || len(frames[0]) != 0 {
		return nil, fmt.Errorf("req recv: missing empty delimiter: %w", zmqerr.ErrMalformedFrame)
	}
	return m.Derive(frames[1:]...), nil
}

// Metrics reports the REQ engine's counters; QueueDepth counts the
// messages queued in the attached pipes.
func (e *reqEngine) Metrics() Metrics {
	e.mu.Lock()
	var depth int
	for _, p := range e.ring.pipes {
		depth += p.Len()
	}
	e.mu.Unlock()
	return e.snapshot(0, uint64(depth))
}

func (e *reqEngine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	close(e.closeCh)
	e.ring.pipes = nil
	e.sticky = nil
	e.mu.Unlock()
	return nil
}
