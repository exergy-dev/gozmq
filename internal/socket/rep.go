package socket

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// repState enumerates the REP lock-step FSM. REP starts in canRecv;
// after a successful Recv it transitions to canSend; after a successful
// Send it returns to canRecv. Out-of-order calls return ErrFSMViolation.
// receiving marks a Recv in progress, reserved under e.mu so a concurrent
// Recv fails instead of overwriting the first one's envelope.
type repState uint8

const (
	repStateCanRecv repState = iota
	repStateCanSend
	repStateReceiving
)

// repEngine implements iface.SocketEngine for the REP socket type.
//
// FSM: the inverse of REQ — canRecv → canSend → canRecv. Recv first,
// then Send once. Calling the wrong direction returns
// zmqerr.ErrFSMViolation (wrapped). The alternation is strict: even a
// failed Send consumes the envelope and returns the FSM to canRecv,
// matching libzmq's REP behaviour so a broken peer cannot wedge the
// socket.
//
// Wire shape: inbound messages from a REQ peer arrive as
// [identity*, empty, body...]. REP splits this envelope:
//
//   - Recv fair-queues across attached pipes, strips everything up to
//     and including the first empty frame (the identity stack plus the
//     delimiter), stashes it, and returns only the body frames to the
//     application.
//   - Send prepends the stashed envelope back onto the outbound body
//     so the reply is routed along the same pipe to the same REQ peer.
//
// Peer stickiness: REP replies on the exact pipe that delivered the
// request. If that pipe detaches between Recv and Send, the reply is
// silently dropped (matching libzmq) and the FSM still returns to
// canRecv so the application may continue.
//
// Thread-safe: Send and Recv serialize via the engine mutex; the FSM
// state guards alternation, and a Recv made while another is in progress
// returns ErrFSMViolation.
type repEngine struct {
	fq *FairQueue

	mu     sync.Mutex
	closed bool
	state  repState
	// currentPeer is the pipe that delivered the most recent request;
	// Send routes the reply to it. Nil unless state == canSend.
	currentPeer iface.Pipe
	// envelope is the identity stack (frames up to and including the
	// empty delimiter) captured from the current request. Valid only in
	// canSend. Stored as a fresh slice independent of any caller *Msg.
	envelope [][]byte
	// closeCh is closed by Close so a Send parked at HWM unblocks.
	closeCh chan struct{}

	counters
}

// NewRepEngine returns a new REP socket engine.
func NewRepEngine() iface.SocketEngine {
	return &repEngine{
		fq:      NewFairQueue(),
		closeCh: make(chan struct{}),
	}
}

func (e *repEngine) AttachPipe(p iface.Pipe) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("rep attach: %w", zmqerr.ErrSocketClosed)
	}
	e.mu.Unlock()
	return e.fq.Attach(p)
}

func (e *repEngine) DetachPipe(p iface.Pipe) error {
	e.mu.Lock()
	// If the detaching pipe was our current reply target, clear the
	// envelope. Subsequent Send will drop the reply silently.
	if e.currentPeer == p {
		e.currentPeer = nil
		e.envelope = nil
	}
	e.mu.Unlock()
	return e.fq.Detach(p)
}

func (e *repEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, fmt.Errorf("rep recv: %w", zmqerr.ErrSocketClosed)
	}
	switch e.state {
	case repStateCanRecv:
	case repStateReceiving:
		e.mu.Unlock()
		return nil, fmt.Errorf("rep recv: another Recv is in progress: %w", zmqerr.ErrFSMViolation)
	default:
		e.mu.Unlock()
		return nil, fmt.Errorf("rep recv: out of order (Send expected next): %w", zmqerr.ErrFSMViolation)
	}
	e.state = repStateReceiving
	e.mu.Unlock()

	for {
		m, src, err := e.fq.RecvWithSource(ctx)
		if err != nil {
			e.mu.Lock()
			if e.state == repStateReceiving {
				e.state = repStateCanRecv
			}
			e.mu.Unlock()
			return nil, err
		}
		envelope, body, ok := splitRepEnvelope(m)
		if !ok {
			// Malformed incoming frame — skip it and keep receiving.
			// Strict libzmq drops such frames silently; we do the same
			// to stay interop-safe.
			continue
		}
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return nil, fmt.Errorf("rep recv: %w", zmqerr.ErrSocketClosed)
		}
		e.currentPeer = src
		e.envelope = envelope
		e.state = repStateCanSend
		e.mu.Unlock()
		e.recv.Add(1)
		return body, nil
	}
}

func (e *repEngine) Send(ctx context.Context, m *msg.Msg) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("rep send: %w", zmqerr.ErrSocketClosed)
	}
	if e.state != repStateCanSend {
		e.mu.Unlock()
		return fmt.Errorf("rep send: out of order (Recv expected next): %w", zmqerr.ErrFSMViolation)
	}
	peer := e.currentPeer
	envelope := e.envelope
	// Consume the envelope regardless of outcome; FSM returns to
	// canRecv either way, matching libzmq's REP alternation.
	e.currentPeer = nil
	e.envelope = nil
	e.state = repStateCanRecv
	e.mu.Unlock()

	if peer == nil {
		// Peer detached between Recv and Send; silently drop per
		// libzmq's REP semantics. The state is already restored to
		// canRecv above so the application can continue.
		return nil
	}

	var body [][]byte
	if m != nil {
		body = m.Frames()
	}
	wireFrames := make([][]byte, 0, len(envelope)+len(body))
	wireFrames = append(wireFrames, envelope...)
	wireFrames = append(wireFrames, body...)
	// wireFrames is built locally and handed off; FromFramesOwned adopts it
	// without the extra copy FromFrames would make.
	wire := msg.FromFramesOwned(wireFrames)
	cp := asChanPipe(peer)
	if !cp.TrySend(wire) {
		// Pipe closed or at HWM. A requester that vanished before its
		// detach landed gets the reply dropped silently, as for a
		// detached peer above; check that first, since the select below
		// would pick randomly while SendCh still has room.
		select {
		case <-peer.Closed():
			return nil
		default:
		}
		// At HWM: block, racing pipe close, engine close, and the
		// caller's ctx — a bare pipe Send here would ignore Close.
		select {
		case cp.SendCh() <- wire:
		case <-peer.Closed():
			return nil
		case <-e.closeCh:
			return fmt.Errorf("rep send: %w", zmqerr.ErrSocketClosed)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	e.sent.Add(1)
	return nil
}

// splitRepEnvelope separates a REQ-shaped inbound frame into envelope
// (identity frames plus the trailing empty delimiter) and body (the
// remaining frames as a fresh *Msg). Returns ok=false if no empty
// delimiter is present.
//
// Shape: [id0, id1, ..., idN, empty, body...] where the identity stack
// may be empty (for a direct REQ↔REP over a bare pipe). The envelope
// returned by this function always includes at least the empty frame;
// the Send path prepends it verbatim to produce a reply.
func splitRepEnvelope(m *msg.Msg) (envelope [][]byte, body *msg.Msg, ok bool) {
	frames := m.Frames()
	for i, f := range frames {
		if len(f) == 0 {
			// The envelope (including the delimiter) outlives m in
			// e.envelope, so it gets its own slice header; the body *Msg
			// adopts the rest of m's header, which the engine solely owns.
			return slices.Clone(frames[:i+1]), msg.FromFramesOwned(frames[i+1:]), true
		}
	}
	return nil, nil, false
}

// Metrics reports the REP engine's counters. recv is incremented on
// each user-visible Recv (after envelope split); dropped delegates to
// the underlying FairQueue. QueueDepth samples the FairQueue.
func (e *repEngine) Metrics() iface.SocketMetrics {
	return e.snapshot(e.fq.Dropped(), e.fq.QueueDepth())
}

func (e *repEngine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	close(e.closeCh)
	e.currentPeer = nil
	e.envelope = nil
	e.mu.Unlock()
	return e.fq.Close()
}
