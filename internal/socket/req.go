package socket

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// reqState enumerates the REQ lock-step FSM. REQ starts in canSend;
// after a successful Send it transitions to canRecv; after a successful
// Recv it returns to canSend. Calling Send in canRecv (or Recv in
// canSend) is an ErrFSMViolation.
type reqState uint8

const (
	reqStateCanSend reqState = iota
	reqStateCanRecv
)

// reqEngine implements iface.SocketEngine for the REQ socket type.
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
// mutex plus the FSM state. Recv reads the sticky pipe's receive channel
// inline — no per-request goroutine or context is allocated; the parked
// Recv unblocks on the sticky pipe's Closed() signal or on the engine's
// closeCh.
type reqEngine struct {
	mu       sync.Mutex
	closed   bool
	closeCh  chan struct{}
	notifyCh chan struct{} // closed on attach; swapped for a fresh one after
	recvGate readyGate
	sendGate readyGate
	state    reqState
	ring     pipeRing
	// sticky is the pipe picked by the most recent Send; Recv reads its
	// reply directly. Cleared after a successful Recv (or on detach).
	sticky chanPipe

	counters
}

// NewReqEngine returns a new REQ socket engine.
func NewReqEngine() iface.SocketEngine {
	return &reqEngine{
		closeCh:  make(chan struct{}),
		notifyCh: make(chan struct{}),
		recvGate: newReadyGate(),
		sendGate: newReadyGate(),
	}
}

func (e *reqEngine) AttachPipe(p iface.Pipe) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("req attach: %w", zmqerr.ErrSocketClosed)
	}
	if !e.ring.add(p) {
		return nil
	}
	// Arm the Poller's recv gate straight from the pipe's data path so a
	// blocking Poll wakes when the reply lands on the sticky pipe (REQ
	// has no forwarder goroutine to do it). Arming for non-sticky pipes
	// is a harmless spurious level-trigger wake.
	asChanPipe(p).SetRecvReady(e.recvGate.ch)
	// Wake any Send parked waiting for a peer.
	close(e.notifyCh)
	e.notifyCh = make(chan struct{})
	if e.state == reqStateCanSend {
		e.sendGate.arm()
	}
	return nil
}

func (e *reqEngine) DetachPipe(p iface.Pipe) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ring.remove(p)
	// If the detached pipe was our sticky peer, the pending request is
	// unanswerable: drop the stickiness so the next Send can pick a new
	// peer. A Recv parked on the sticky pipe unblocks via the pipe's
	// Closed() signal (the session closes the pipe on detach) and surfaces
	// ErrSocketClosed. Actual libzmq drops the outstanding request here too.
	if iface.Pipe(e.sticky) == p {
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
		e.mu.Unlock()
		return fmt.Errorf("req send: out of order (Recv expected next): %w", zmqerr.ErrFSMViolation)
	}
	e.mu.Unlock()

	// Build wire message: empty delimiter + caller's frames. The slice is
	// built locally and handed off, so adopt it directly (no extra copy).
	var body [][]byte
	if m != nil {
		body = m.Frames()
	}
	wireFrames := make([][]byte, 0, len(body)+1)
	wireFrames = append(wireFrames, []byte{})
	wireFrames = append(wireFrames, body...)
	wire := msg.FromFramesOwned(wireFrames)

	// Pick a peer and deliver. Retry on peer churn / closed pipe.
	for {
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return fmt.Errorf("req send: %w", zmqerr.ErrSocketClosed)
		}
		if e.ring.len() == 0 {
			notify := e.notifyCh
			closeCh := e.closeCh
			e.mu.Unlock()
			select {
			case <-notify:
			case <-closeCh:
				return fmt.Errorf("req send: %w", zmqerr.ErrSocketClosed)
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		idx, p := e.ring.current()
		closeCh := e.closeCh
		e.mu.Unlock()

		cp := asChanPipe(p)
		// Discard anything still buffered on the chosen pipe: it predates
		// this request, so it can only be a stale reply from an earlier
		// exchange the FSM already abandoned (e.g. a malformed reply or a
		// peer-gone reset). Delivering it as this request's answer would
		// desynchronize request/reply pairing.
		for {
			stale, ok := cp.TryRecv()
			if !ok {
				break
			}
			msg.Recycle(stale)
		}

		delivered := cp.TrySend(wire)
		if !delivered {
			// Pipe at HWM (or closing): block, racing engine close, pipe
			// close, and the caller's ctx — a bare pipe Send here would
			// violate the Close-unblocks-pending-Send contract.
			select {
			case cp.SendCh() <- wire:
				delivered = true
			case <-p.Closed():
				// Peer vanished; drop it from the set and retry below.
			case <-closeCh:
				return fmt.Errorf("req send: %w", zmqerr.ErrSocketClosed)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if !delivered {
			_ = e.DetachPipe(p)
			continue
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
		// p's reply directly — no forwarder goroutine.
		e.sticky = cp
		e.state = reqStateCanRecv
		e.mu.Unlock()
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
		e.mu.Unlock()
		return nil, fmt.Errorf("req recv: out of order (Send expected next): %w", zmqerr.ErrFSMViolation)
	}
	sticky := e.sticky
	closeCh := e.closeCh
	e.mu.Unlock()

	m, err := e.recvSticky(ctx, sticky, closeCh)
	if err != nil {
		// A context error leaves the FSM in canRecv with the sticky pipe
		// intact: the reply is still in flight and a retried Recv must
		// await it (libzmq EAGAIN semantics). Peer-gone and close reset
		// to canSend so a fresh Send can pick a new peer.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		e.mu.Lock()
		if e.state == reqStateCanRecv {
			e.state = reqStateCanSend
			e.sticky = nil
		}
		e.mu.Unlock()
		return nil, err
	}

	stripped, serr := stripReqDelimiter(m)
	if serr != nil {
		// Malformed reply — still flip state so the FSM can continue.
		e.mu.Lock()
		e.state = reqStateCanSend
		e.sticky = nil
		if e.ring.len() > 0 {
			e.sendGate.arm()
		}
		e.mu.Unlock()
		return nil, serr
	}
	e.mu.Lock()
	e.state = reqStateCanSend
	e.sticky = nil
	if e.ring.len() > 0 {
		e.sendGate.arm()
	}
	e.mu.Unlock()
	e.recv.Add(1)
	return stripped, nil
}

// recvSticky reads one reply from the sticky pipe, racing the read against
// engine close, peer detach (pipe close), and the caller's context. It
// allocates no per-call context or goroutine — the pipe's receive channel
// is selected on inline. The returned error is already wrapped with the
// REQ recv prefix where appropriate.
func (e *reqEngine) recvSticky(ctx context.Context, sticky chanPipe, closeCh <-chan struct{}) (*msg.Msg, error) {
	if sticky == nil {
		// Sticky peer detached between Send and Recv. Treat as peer gone.
		select {
		case <-closeCh:
			return nil, fmt.Errorf("req recv: %w", zmqerr.ErrSocketClosed)
		default:
		}
		return nil, fmt.Errorf("req recv: peer gone: %w", zmqerr.ErrSocketClosed)
	}

	recvCh := sticky.RecvCh()
	// Non-blocking fast path: reply already buffered.
	select {
	case m := <-recvCh:
		if m != nil {
			return m, nil
		}
	default:
	}
	select {
	case m := <-recvCh:
		if m != nil {
			return m, nil
		}
		// Channel yielded a nil (closed); fall through to peer-gone.
		return nil, fmt.Errorf("req recv: peer gone: %w", zmqerr.ErrSocketClosed)
	case <-sticky.Closed():
		// Peer gone — but drain any reply that landed before close.
		select {
		case m := <-recvCh:
			if m != nil {
				return m, nil
			}
		default:
		}
		return nil, fmt.Errorf("req recv: peer gone: %w", zmqerr.ErrSocketClosed)
	case <-closeCh:
		return nil, fmt.Errorf("req recv: %w", zmqerr.ErrSocketClosed)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (e *reqEngine) ReadyRecv() <-chan struct{} {
	return e.recvGate.signal(e.CanRecvNow())
}

func (e *reqEngine) ReadyWrite() <-chan struct{} {
	return e.sendGate.signal(e.CanSendNow())
}

func (e *reqEngine) CanRecvNow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || e.state != reqStateCanRecv || e.sticky == nil {
		return false
	}
	// A Recv is non-blocking-ready iff a reply is already buffered on the
	// sticky pipe's receive channel.
	return len(e.sticky.RecvCh()) > 0
}

func (e *reqEngine) CanSendNow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.closed && e.state == reqStateCanSend && e.ring.len() > 0
}

var _ Pollable = (*reqEngine)(nil)

// stripReqDelimiter removes the leading empty delimiter frame from an
// inbound REP reply. The wire shape is [empty, body...]; we return a
// fresh *Msg holding just the body frames. The engine solely owns the
// inbound *Msg (it is about to TransferPool it), so the resliced frame
// header can be adopted directly without a copy.
func stripReqDelimiter(m *msg.Msg) (*msg.Msg, error) {
	frames := m.Frames()
	if len(frames) < 1 || len(frames[0]) != 0 {
		return nil, fmt.Errorf("req recv: missing empty delimiter: %w", zmqerr.ErrMalformedFrame)
	}
	result := msg.FromFramesOwned(frames[1:])
	result.TransferPool(m)
	return result, nil
}

// Metrics reports the REQ engine's counters. REQ has no fair-queue;
// inbound replies are read directly off the sticky pipe, so QueueDepth is
// always zero.
func (e *reqEngine) Metrics() iface.SocketMetrics {
	return e.snapshot(0, 0)
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
