package socket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

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
// mutex plus the FSM state. A per-request forwarder goroutine
// (stickyForward) pumps the sticky pipe's Recv into a channel; Close
// waits for any in-flight forwarder to drain.
type reqEngine struct {
	mu       sync.Mutex
	closed   bool
	closeCh  chan struct{}
	notifyCh chan struct{} // closed on attach; swapped for a fresh one after
	state    reqState
	pipes    []iface.Pipe
	cursor   int
	// sticky is the pipe picked by the most recent Send; Recv filters on
	// it. Cleared after a successful Recv.
	sticky iface.Pipe
	// stickyCh receives the peer's reply for the sticky pipe; fed by a
	// per-request goroutine that pumps sticky.Recv until it sees the
	// reply or the engine closes. A fresh channel is allocated per Send.
	stickyCh chan stickyRecvResult
	// forwarderWG tracks the per-request stickyForward goroutine so that
	// Close can wait for it to exit and goleak stays clean.
	forwarderWG sync.WaitGroup

	sent atomic.Uint64
	recv atomic.Uint64
}

type stickyRecvResult struct {
	m   *msg.Msg
	err error
}

// NewReqEngine returns a new REQ socket engine.
func NewReqEngine() iface.SocketEngine {
	return &reqEngine{
		closeCh:  make(chan struct{}),
		notifyCh: make(chan struct{}),
	}
}

func (e *reqEngine) AttachPipe(p iface.Pipe) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("req attach: %w", zmqerr.ErrSocketClosed)
	}
	for _, existing := range e.pipes {
		if existing == p {
			return nil
		}
	}
	e.pipes = append(e.pipes, p)
	// Wake any Send parked waiting for a peer.
	close(e.notifyCh)
	e.notifyCh = make(chan struct{})
	return nil
}

func (e *reqEngine) DetachPipe(p iface.Pipe) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, existing := range e.pipes {
		if existing == p {
			e.pipes = append(e.pipes[:i], e.pipes[i+1:]...)
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
	// peer. (The in-flight Recv will surface io.EOF via the stickyCh
	// feeder.) Actual libzmq drops the outstanding request here too.
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

	// Build wire message: empty delimiter + caller's frames. Copy the
	// slice header so the caller's *Msg is untouched.
	var body [][]byte
	if m != nil {
		body = m.Frames()
	}
	wireFrames := make([][]byte, 0, len(body)+1)
	wireFrames = append(wireFrames, []byte{})
	wireFrames = append(wireFrames, body...)
	wire := msg.FromFrames(wireFrames...)

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
				// Close won this race; don't launch a forwarder (it
				// would outlive Close's WaitGroup drain). The peer
				// received the request; the caller will see
				// ErrSocketClosed on the subsequent Recv.
				e.mu.Unlock()
				return fmt.Errorf("req send: %w", zmqerr.ErrSocketClosed)
			}
			// Advance cursor past p if it's still at idx.
			if idx < len(e.pipes) && e.pipes[idx] == p {
				e.cursor = (idx + 1) % len(e.pipes)
			}
			// Transition to canRecv with p as the sticky pipe.
			e.sticky = p
			e.stickyCh = make(chan stickyRecvResult, 1)
			e.state = reqStateCanRecv
			// Launch the dedicated forwarder for this request.
			e.forwarderWG.Add(1)
			go e.stickyForward(p, e.stickyCh)
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

// stickyForward pumps p.Recv into out until it produces a result or the
// engine closes. The engine sets state back to canSend on a successful
// drain in Recv; if the engine closes first, the goroutine exits on
// io.EOF / ErrClosedPipe from p.Recv (after the pipe is closed by
// Close), and the result is still written to the channel so a parked
// Recv can unblock.
func (e *reqEngine) stickyForward(p iface.Pipe, out chan<- stickyRecvResult) {
	defer e.forwarderWG.Done()
	// The engine's Recv owns the context; we read with Background here
	// and rely on the engine closing either the pipe or signalling
	// through closeCh. To unblock this goroutine on engine.Close, we
	// race p.Recv against e.closeCh via a child context.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watcherDone := make(chan struct{})
	var watcherWG sync.WaitGroup
	watcherWG.Add(1)
	go func() {
		defer watcherWG.Done()
		select {
		case <-e.closeCh:
			cancel()
		case <-watcherDone:
		}
	}()
	m, err := p.Recv(ctx)
	close(watcherDone)
	watcherWG.Wait()
	out <- stickyRecvResult{m: m, err: err}
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
	stickyCh := e.stickyCh
	closeCh := e.closeCh
	e.mu.Unlock()

	select {
	case r := <-stickyCh:
		if r.err != nil {
			// Don't flip state on error — caller may retry. But clear
			// stickiness so a fresh Send can pick a new peer.
			e.mu.Lock()
			if e.state == reqStateCanRecv {
				e.state = reqStateCanSend
				e.sticky = nil
				e.stickyCh = nil
			}
			e.mu.Unlock()
			// If the engine itself closed, surface ErrSocketClosed
			// regardless of which underlying error the forwarder saw.
			select {
			case <-closeCh:
				return nil, fmt.Errorf("req recv: %w", zmqerr.ErrSocketClosed)
			default:
			}
			if errors.Is(r.err, io.EOF) || errors.Is(r.err, io.ErrClosedPipe) {
				return nil, fmt.Errorf("req recv: peer gone: %w", zmqerr.ErrSocketClosed)
			}
			return nil, r.err
		}
		stripped, err := stripReqDelimiter(r.m)
		if err != nil {
			// Malformed reply — still flip state so the FSM can continue.
			e.mu.Lock()
			e.state = reqStateCanSend
			e.sticky = nil
			e.stickyCh = nil
			e.mu.Unlock()
			return nil, err
		}
		e.mu.Lock()
		e.state = reqStateCanSend
		e.sticky = nil
		e.stickyCh = nil
		e.mu.Unlock()
		e.recv.Add(1)
		return stripped, nil
	case <-closeCh:
		return nil, fmt.Errorf("req recv: %w", zmqerr.ErrSocketClosed)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// stripReqDelimiter removes the leading empty delimiter frame from an
// inbound REP reply. The wire shape is [empty, body...]; we return a
// fresh *Msg holding just the body frames.
func stripReqDelimiter(m *msg.Msg) (*msg.Msg, error) {
	frames := m.Frames()
	if len(frames) < 1 || len(frames[0]) != 0 {
		return nil, fmt.Errorf("req recv: missing empty delimiter: %w", zmqerr.ErrMalformedFrame)
	}
	rest := frames[1:]
	out := make([][]byte, len(rest))
	copy(out, rest)
	result := msg.FromFrames(out...)
	result.TransferPool(m)
	return result, nil
}

// Metrics reports the REQ engine's counters. REQ has no fair-queue;
// inbound replies are pulled from a per-request sticky channel which is
// allocated on Send and consumed on Recv, so QueueDepth is always zero.
func (e *reqEngine) Metrics() iface.SocketMetrics {
	return iface.SocketMetrics{
		Sent: e.sent.Load(),
		Recv: e.recv.Load(),
	}
}

func (e *reqEngine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	close(e.closeCh)
	e.pipes = nil
	e.sticky = nil
	// Leave stickyCh alone — the forwarder goroutine (if running) will
	// write to it and exit when p.Recv unblocks via the closed ctx.
	e.mu.Unlock()
	// Wait for any in-flight sticky forwarder to exit so goleak stays
	// clean. The forwarder unblocks via the ctx cancelled on closeCh.
	e.forwarderWG.Wait()
	return nil
}
