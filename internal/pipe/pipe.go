// Package pipe provides the reference implementation of iface.Pipe — a
// bounded bidirectional in-memory message queue between a socket engine
// and a session. Channel-backed; HWM is expressed as channel capacity so
// backpressure and context cancellation fall out of Go's select semantics.
package pipe

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
)

// ErrNilMessage is returned by Send when the caller supplies a nil *Msg.
var ErrNilMessage = errors.New("gozmq/pipe: nil message")

// PeerInfo carries the metadata a session advertises to its engine. Zero
// value = unknown; fields are set at pipe construction and may not change.
type PeerInfo struct {
	Endpoint string
	Identity []byte
}

// NewPair returns two Pipe ends bound to one another. aToB is the maximum
// number of messages buffered in the A→B direction; bToA is the B→A
// maximum. A HWM of 0 means a synchronous (unbuffered) direction; negative
// values are rejected with a panic (programmer error).
//
// aInfo describes the peer visible to the A end (i.e. B's endpoint/identity);
// bInfo describes what A looks like from B's side.
func NewPair(aToB, bToA int, aInfo, bInfo PeerInfo) (a, b iface.Pipe) {
	if aToB < 0 || bToA < 0 {
		panic("gozmq/pipe: negative HWM")
	}
	s := newShared()
	aChan := make(chan *msg.Msg, aToB)
	bChan := make(chan *msg.Msg, bToA)
	endA := &end{
		shared:  s,
		isA:     true,
		sendCh:  aChan, // A writes → B reads
		recvCh:  bChan, // A reads ← B writes
		sendHWM: aToB,
		recvHWM: bToA,
		peer:    aInfo,
	}
	endB := &end{
		shared:  s,
		isA:     false,
		sendCh:  bChan,
		recvCh:  aChan,
		sendHWM: bToA,
		recvHWM: aToB,
		peer:    bInfo,
	}
	return endA, endB
}

type shared struct {
	aClosed    chan struct{}
	bClosed    chan struct{}
	either     chan struct{}
	eitherOnce sync.Once
}

func newShared() *shared {
	s := &shared{
		aClosed: make(chan struct{}),
		bClosed: make(chan struct{}),
		either:  make(chan struct{}),
	}
	go func() {
		select {
		case <-s.aClosed:
		case <-s.bClosed:
		}
		s.eitherOnce.Do(func() { close(s.either) })
	}()
	return s
}

type end struct {
	shared *shared
	isA    bool

	sendCh chan *msg.Msg
	recvCh chan *msg.Msg

	sendHWM int
	recvHWM int
	peer    PeerInfo

	closeOnce sync.Once
}

func (e *end) myClosed() chan struct{} {
	if e.isA {
		return e.shared.aClosed
	}
	return e.shared.bClosed
}

func (e *end) peerClosed() chan struct{} {
	if e.isA {
		return e.shared.bClosed
	}
	return e.shared.aClosed
}

func (e *end) Send(ctx context.Context, m *msg.Msg) error {
	if m == nil {
		return ErrNilMessage
	}
	select {
	case <-e.myClosed():
		return io.ErrClosedPipe
	case <-e.peerClosed():
		return io.ErrClosedPipe
	default:
	}
	select {
	case e.sendCh <- m:
		return nil
	case <-e.myClosed():
		return io.ErrClosedPipe
	case <-e.peerClosed():
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TrySend attempts an immediate send without blocking. It preserves the same
// close checks as Send and returns false when the pipe is full or closed.
func (e *end) TrySend(m *msg.Msg) bool {
	if m == nil {
		return false
	}
	select {
	case <-e.myClosed():
		return false
	case <-e.peerClosed():
		return false
	default:
	}
	select {
	case e.sendCh <- m:
		return true
	default:
		return false
	}
}

func (e *end) Recv(ctx context.Context) (*msg.Msg, error) {
	// Fast path: something already buffered.
	select {
	case m := <-e.recvCh:
		return m, nil
	default:
	}
	select {
	case m := <-e.recvCh:
		return m, nil
	case <-e.myClosed():
		return nil, io.EOF
	case <-e.peerClosed():
		// Peer closed — drain residual, then EOF.
		select {
		case m := <-e.recvCh:
			return m, nil
		default:
			return nil, io.EOF
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TryRecv returns one buffered message if available without blocking.
// It is intentionally not part of iface.Pipe; the session writer uses
// it opportunistically via a local type assertion during linger drains
// to avoid racing a pre-cancelled context against a ready receive.
func (e *end) TryRecv() (*msg.Msg, bool) {
	select {
	case m := <-e.recvCh:
		return m, true
	default:
		return nil, false
	}
}

func (e *end) Close() error {
	e.closeOnce.Do(func() { close(e.myClosed()) })
	return nil
}

func (e *end) Closed() <-chan struct{} { return e.shared.either }

func (e *end) SendHWM() int { return e.sendHWM }
func (e *end) RecvHWM() int { return e.recvHWM }

func (e *end) PeerEndpoint() string { return e.peer.Endpoint }
func (e *end) PeerIdentity() []byte { return e.peer.Identity }

// Compile-time check.
var _ iface.Pipe = (*end)(nil)
