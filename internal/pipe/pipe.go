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
	"sync/atomic"

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
		myCh:    s.aClosed,
		peerCh:  s.bClosed,
	}
	endB := &end{
		shared:  s,
		isA:     false,
		sendCh:  bChan,
		recvCh:  aChan,
		sendHWM: bToA,
		recvHWM: aToB,
		peer:    bInfo,
		myCh:    s.bClosed,
		peerCh:  s.aClosed,
	}
	endA.peerEnd = endB
	endB.peerEnd = endA
	return endA, endB
}

type shared struct {
	aClosed    chan struct{}
	bClosed    chan struct{}
	either     chan struct{}
	eitherOnce sync.Once
}

func newShared() *shared {
	return &shared{
		aClosed: make(chan struct{}),
		bClosed: make(chan struct{}),
		either:  make(chan struct{}),
	}
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

	myCh    chan struct{}  // cached closed channel for this end
	peerCh  chan struct{}  // cached closed channel for peer end
	peerEnd *end          // peer end for atomic flag check
	isClosed atomic.Bool  // set in Close()
}

func (e *end) myClosed() chan struct{} {
	return e.myCh
}

func (e *end) peerClosed() chan struct{} {
	return e.peerCh
}

func (e *end) Send(ctx context.Context, m *msg.Msg) error {
	if m == nil {
		return ErrNilMessage
	}
	if e.isClosed.Load() || e.peerEnd.isClosed.Load() {
		return io.ErrClosedPipe
	}
	select {
	case e.sendCh <- m:
		return nil
	default:
	}
	select {
	case e.sendCh <- m:
		return nil
	case <-e.myCh:
		return io.ErrClosedPipe
	case <-e.peerCh:
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
	if e.isClosed.Load() || e.peerEnd.isClosed.Load() {
		return false
	}
	select {
	case e.sendCh <- m:
		return true
	default:
		return false
	}
}

func (e *end) Recv(ctx context.Context) (*msg.Msg, error) {
	select {
	case m := <-e.recvCh:
		return m, nil
	default:
	}
	select {
	case m := <-e.recvCh:
		return m, nil
	case <-e.myCh:
		return nil, io.EOF
	case <-e.peerCh:
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
	if e.isClosed.Load() {
		return nil, false
	}
	select {
	case m := <-e.recvCh:
		return m, true
	default:
		return nil, false
	}
}

func (e *end) Close() error {
	e.closeOnce.Do(func() {
		e.isClosed.Store(true)
		close(e.myCh)
		e.shared.eitherOnce.Do(func() { close(e.shared.either) })
	})
	return nil
}

func (e *end) Closed() <-chan struct{} { return e.shared.either }

func (e *end) SendHWM() int { return e.sendHWM }
func (e *end) RecvHWM() int { return e.recvHWM }

func (e *end) PeerEndpoint() string { return e.peer.Endpoint }
func (e *end) PeerIdentity() []byte { return e.peer.Identity }

// RecvCh exposes the internal receive channel for direct use in select
// statements. This allows callers to avoid context.WithCancel overhead
// when multiplexing a pipe recv with other signals. The caller is
// responsible for handling pipe-closed semantics (via Closed()).
func (e *end) RecvCh() <-chan *msg.Msg { return e.recvCh }

// SendCh exposes the internal send channel for direct use in select
// statements. This mirrors RecvCh and allows the LoadBalancer to inline
// a channel send into its own select, eliminating a per-Send goroutine.
func (e *end) SendCh() chan<- *msg.Msg { return e.sendCh }

// Compile-time check.
var _ iface.Pipe = (*end)(nil)
