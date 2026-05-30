package socket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/zmqerr"
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
// transitions back to canSend.
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
// canSend so the caller may retry.
//
// Thread-safe: Send and Recv serialize against each other via the engine
// mutex plus the FSM state. Recv reads the sticky pipe's receive channel
// inline (via the recvChanner fast path) — no per-request goroutine or
// context is allocated; the parked Recv unblocks on the sticky pipe's
// Closed() signal or on the engine's closeCh.
type reqEngine struct {
	mu       sync.Mutex
	closed   bool
	closeCh  chan struct{}
	notifyCh chan struct{} // closed on attach; swapped for a fresh one after
	recvGate readyGate
	sendGate readyGate
	state    reqState
	pipes    []iface.Pipe
	cursor   int
	// sticky is the pipe picked by the most recent Send; Recv reads its
	// reply directly. Cleared after a successful Recv (or on detach).
	sticky iface.Pipe
	// recvCtx is a single long-lived context derived from closeCh; it is
	// cancelled in Close. Recv uses it only for the fallback path where the
	// sticky pipe does not implement recvChanner (so p.Recv can be unblocked
	// by Close without a per-Recv context allocation).
	recvCtx    context.Context
	cancelRecv context.CancelFunc

	counters
}

// NewReqEngine returns a new REQ socket engine.
func NewReqEngine() iface.SocketEngine {
	ctx, cancel := context.WithCancel(context.Background())
	return &reqEngine{
		closeCh:    make(chan struct{}),
		notifyCh:   make(chan struct{}),
		recvGate:   newReadyGate(),
		sendGate:   newReadyGate(),
		recvCtx:    ctx,
		cancelRecv: cancel,
	}
}

func (e *reqEngine) AttachPipe(p iface.Pipe) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("req attach: %w", zmqerr.ErrSocketClosed)
	}
	if slices.Contains(e.pipes, p) {
		return nil
	}
	e.pipes = append(e.pipes, p)
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
	for i, existing := range e.pipes {
		if existing == p {
			e.pipes = slices.Delete(e.pipes, i, i+1)
			if len(e.pipes) == 0 {
				e.cursor = 0
			} else {
				if i < e.cursor {
					e.cursor--
				}
				e.cursor %= len(e.pipes)
			}
			break
		}
	}
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
		if len(e.pipes) == 0 {
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
		idx := e.cursor % len(e.pipes)
		p := e.pipes[idx]
		e.mu.Unlock()

		err := p.Send(ctx, wire)
		if err == nil {
			e.mu.Lock()
			if e.closed {
				// Close won this race. The peer received the request; the
				// caller will see ErrSocketClosed on the subsequent Recv.
				e.mu.Unlock()
				return fmt.Errorf("req send: %w", zmqerr.ErrSocketClosed)
			}
			// Advance cursor past p if it's still at idx.
			if idx < len(e.pipes) && e.pipes[idx] == p {
				e.cursor = (idx + 1) % len(e.pipes)
			}
			// Transition to canRecv with p as the sticky pipe. Recv reads
			// p's reply directly — no forwarder goroutine.
			e.sticky = p
			e.state = reqStateCanRecv
			e.mu.Unlock()
			e.sent.Add(1)
			return nil
		}
		if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, io.EOF) {
			// Peer vanished; drop it from the set and retry.
			_ = e.DetachPipe(p)
			continue
		}
		return err
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
		// Don't flip the FSM on error — the caller may retry the Recv. But
		// clear stickiness so a fresh Send can pick a new peer, matching the
		// original ctx.Done/error behavior.
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
		if len(e.pipes) > 0 {
			e.sendGate.arm()
		}
		e.mu.Unlock()
		return nil, serr
	}
	e.mu.Lock()
	e.state = reqStateCanSend
	e.sticky = nil
	if len(e.pipes) > 0 {
		e.sendGate.arm()
	}
	e.mu.Unlock()
	e.recv.Add(1)
	return stripped, nil
}

// recvSticky reads one reply from the sticky pipe, racing the read against
// engine close, peer detach (pipe close), and the caller's context. It
// allocates no per-call context or goroutine when the pipe exposes its
// receive channel (the common case). The returned error is already wrapped
// with the REQ recv prefix where appropriate.
func (e *reqEngine) recvSticky(ctx context.Context, sticky iface.Pipe, closeCh <-chan struct{}) (*msg.Msg, error) {
	if sticky == nil {
		// Sticky peer detached between Send and Recv. Treat as peer gone.
		select {
		case <-closeCh:
			return nil, fmt.Errorf("req recv: %w", zmqerr.ErrSocketClosed)
		default:
		}
		return nil, fmt.Errorf("req recv: peer gone: %w", zmqerr.ErrSocketClosed)
	}

	if rc, ok := sticky.(recvChanner); ok {
		recvCh := rc.RecvCh()
		var closedCh <-chan struct{}
		if c, ok := sticky.(interface{ Closed() <-chan struct{} }); ok {
			closedCh = c.Closed()
		}
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
		case <-closedCh:
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

	// Fallback for pipes without a receive channel: race p.Recv against the
	// engine's long-lived recvCtx (cancelled by Close) and the caller's ctx.
	// A child context is needed to merge the two cancellation sources, but
	// it is derived once around this single blocking call rather than per
	// forwarder goroutine.
	mergedCtx, cancel := mergeCancel(ctx, e.recvCtx)
	defer cancel()
	m, err := sticky.Recv(mergedCtx)
	if err != nil {
		select {
		case <-closeCh:
			return nil, fmt.Errorf("req recv: %w", zmqerr.ErrSocketClosed)
		default:
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
			return nil, fmt.Errorf("req recv: peer gone: %w", zmqerr.ErrSocketClosed)
		}
		return nil, err
	}
	return m, nil
}

// mergeCancel returns a context cancelled when either parent or extra is
// cancelled. It spawns one watcher goroutine that exits as soon as the
// returned cancel runs (which the caller defers), so no goroutine leaks
// past the single blocking Recv it guards.
func mergeCancel(parent, extra context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	stop := make(chan struct{})
	go func() {
		select {
		case <-extra.Done():
			cancel()
		case <-stop:
		}
	}()
	return ctx, func() {
		cancel()
		close(stop)
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
	if rc, ok := e.sticky.(recvChanner); ok {
		return len(rc.RecvCh()) > 0
	}
	return false
}

func (e *reqEngine) CanSendNow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.closed && e.state == reqStateCanSend && len(e.pipes) > 0
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
	e.cancelRecv()
	e.pipes = nil
	e.sticky = nil
	e.mu.Unlock()
	return nil
}
