// Package pipe provides the reference implementation of pipeiface.Pipe — a
// bidirectional in-memory message queue between a socket engine and a
// session. Positive HWM directions are channel-backed with HWM expressed as
// channel capacity; HWM 0 directions use an unbounded FIFO queue.
package pipe

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/exergy-dev/gozmq/internal/pipeiface"
	"github.com/exergy-dev/gozmq/msg"
)

// ErrNilMessage is returned by Send when the caller supplies a nil *Msg.
var ErrNilMessage = errors.New("gozmq/pipe: nil message")

// PeerInfo carries the metadata a session advertises to its engine. Zero
// value = unknown; fields are set at pipe construction and may not change.
type PeerInfo struct {
	Identity []byte
}

// NewPair returns two Pipe ends bound to one another. aToB is the maximum
// number of messages buffered in the A→B direction; bToA is the B→A
// maximum. A HWM of 0 disables the limit for that direction; negative values
// are rejected with a panic (programmer error).
//
// aInfo describes the peer visible to the A end (i.e. B's endpoint/identity);
// bInfo describes what A looks like from B's side.
func NewPair(aToB, bToA int, aInfo, bInfo PeerInfo) (a, b pipeiface.Pipe) {
	if aToB < 0 || bToA < 0 {
		panic("gozmq/pipe: negative HWM")
	}
	s := newShared()
	aToBq := newDirection(aToB)
	bToAq := newDirection(bToA)
	endA := &end{
		shared:  s,
		sendCh:  aToBq.in,  // A writes → B reads
		recvCh:  bToAq.out, // A reads ← B writes
		sendDir: aToBq,
		recvDir: bToAq,
		peer:    aInfo,
		myCh:    s.aClosed,
		peerCh:  s.bClosed,
	}
	endB := &end{
		shared:  s,
		sendCh:  bToAq.in,
		recvCh:  aToBq.out,
		sendDir: bToAq,
		recvDir: aToBq,
		peer:    bInfo,
		myCh:    s.bClosed,
		peerCh:  s.aClosed,
	}
	endA.peerEnd = endB
	endB.peerEnd = endA
	// Wire each relay (HWM-0 directions only) to the receiving end's
	// recv-ready gate, then start it. The hooks must be in place before the
	// relay goroutine runs so no out-enqueue can miss an arm.
	aToBq.armOut = endB.armRecvReady
	bToAq.armOut = endA.armRecvReady
	aToBq.start()
	bToAq.start()
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

// direction is one half of the pipe. HWM > 0 directions are a single
// buffered channel (in == out, HWM == capacity). HWM-0 (unbounded)
// directions keep an unbounded FIFO inside a relay goroutine that pumps
// in → q → out, so engines can still select on the out channel; see the
// CAUTION in run for the close/drain ordering.
type direction struct {
	in       chan *msg.Msg
	out      chan *msg.Msg
	stopIn   chan struct{}
	done     chan struct{}
	drained  chan struct{} // relay closed this after flushing q post-stopIn
	armOut   func()        // arms the receiving end's recv-ready gate; set in NewPair
	stopOnce sync.Once
	doneOnce sync.Once
}

func newDirection(hwm int) *direction {
	if hwm > 0 {
		ch := make(chan *msg.Msg, hwm)
		return &direction{in: ch, out: ch}
	}
	return &direction{
		in:      make(chan *msg.Msg, 1024),
		out:     make(chan *msg.Msg, 1),
		stopIn:  make(chan struct{}),
		done:    make(chan struct{}),
		drained: make(chan struct{}),
	}
}

// start launches the relay goroutine for HWM-0 directions. Called by
// NewPair once armOut has been wired; a no-op for channel-backed
// directions.
func (d *direction) start() {
	if d.stopIn != nil {
		go d.run()
	}
}

// run is the HWM-0 relay: it pumps in → q → out until the sending end
// closes (stopIn), at which point it drains in, flushes q to out, and
// closes drained so the receiving end's Recv can distinguish "queue
// flushed, EOF now" from "messages still in flight". If the receiving
// end closes (done) the relay exits immediately, dropping q.
//
// Leak caveat: after the sender closes, the relay parks pushing q into
// out until the receiver consumes or closes. A receiver that neither
// reads nor closes keeps the relay (and q) alive — the same lifetime
// rule as any unclosed pipe end. The socket layer therefore closes a
// detached session pipe's engine end once it is Drained or the socket
// closes.
func (d *direction) run() {
	var q []*msg.Msg
	for {
		var out chan *msg.Msg
		var next *msg.Msg
		if len(q) > 0 {
			out = d.out
			next = q[0]
		}
		select {
		case m := <-d.in:
			q = append(q, m)
		case out <- next:
			q[0] = nil
			q = q[1:]
			d.armOut()
		case <-d.stopIn:
			for {
				select {
				case m := <-d.in:
					q = append(q, m)
				default:
					goto drain
				}
			}
		drain:
			for len(q) > 0 {
				select {
				case d.out <- q[0]:
					q[0] = nil
					q = q[1:]
					d.armOut()
				case <-d.done:
					return
				}
			}
			close(d.drained)
			return
		case <-d.done:
			return
		}
	}
}

// closedChan is a shared pre-closed channel; drainedCh returns it for
// channel-backed (HWM > 0) directions, where every in-flight message is
// already visible in the out channel and there is nothing left to flush.
var closedChan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// drainedCh returns a channel that is closed once every message this
// direction will ever deliver is observable on the out channel.
func (d *direction) drainedCh() <-chan struct{} {
	if d.drained == nil {
		return closedChan
	}
	return d.drained
}

func (d *direction) closeInput() {
	if d.stopIn == nil {
		return
	}
	d.stopOnce.Do(func() { close(d.stopIn) })
}

func (d *direction) close() {
	if d.done == nil {
		return
	}
	d.doneOnce.Do(func() { close(d.done) })
}

type end struct {
	shared *shared

	sendCh  chan *msg.Msg
	recvCh  chan *msg.Msg
	sendDir *direction
	recvDir *direction

	peer PeerInfo

	closeOnce sync.Once

	myCh     chan struct{} // cached closed channel for this end
	peerCh   chan struct{} // cached closed channel for peer end
	peerEnd  *end          // peer end for atomic flag check
	isClosed atomic.Bool   // set in Close()

	// recvReady, when registered via SetRecvReady, is armed (non-blocking
	// send into a cap-1 channel) every time a message becomes available to
	// this end's Recv, and when either end closes. Socket engines hand
	// their Poller-facing readiness gate to the pipe so a blocking
	// Poller.Poll wakes on data arrival without a forwarder goroutine.
	recvReady atomic.Pointer[recvReadyGate]
}

// recvReadyGate wraps the registered gate channel so it can sit behind an
// atomic.Pointer (SetRecvReady may race in-flight sends).
type recvReadyGate struct{ ch chan<- struct{} }

// SetRecvReady registers gate as this end's recv-ready signal (see the
// recvReady field doc). If messages are already buffered the gate is
// armed immediately, preserving the level-trigger property across
// late registration.
func (e *end) SetRecvReady(gate chan<- struct{}) {
	e.recvReady.Store(&recvReadyGate{ch: gate})
	if len(e.recvCh) > 0 {
		e.armRecvReady()
	}
}

// armRecvReady performs the non-blocking arm of the registered gate.
// Safe to call from any goroutine; a cap-1 gate coalesces bursts.
func (e *end) armRecvReady() {
	g := e.recvReady.Load()
	if g == nil {
		return
	}
	select {
	case g.ch <- struct{}{}:
	default:
	}
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
		e.peerEnd.armRecvReady()
		return nil
	default:
	}
	select {
	case e.sendCh <- m:
		e.peerEnd.armRecvReady()
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
		e.peerEnd.armRecvReady()
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
		// Peer closed: hand out whatever is still owed, then EOF.
		return e.DrainRecv(ctx)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// SealRecv declares that the peer will send nothing more towards this
// end, without closing either end. An HWM-0 direction's relay then
// flushes its queue and closes drained, which is what lets DrainRecv tell
// "empty for good" from "relay still refilling the channel". A message
// the peer sends after SealRecv may never be delivered, so call it only
// once the peer has stopped sending (the session writer's linger drain
// runs after engine.Close). A no-op for channel-backed directions.
func (e *end) SealRecv() { e.recvDir.closeInput() }

// DrainRecv returns the next message still owed to this end once no more
// input can arrive: the peer closed, or SealRecv was called. For an HWM-0
// direction it blocks while the relay is still flushing its queue, so a
// momentarily empty channel is not mistaken for the end; for
// channel-backed directions it is a non-blocking receive. It returns
// io.EOF once everything has been delivered or this end is closed, and
// ctx.Err() if ctx ends first. Called before either signal, it just
// waits for a message or ctx.
func (e *end) DrainRecv(ctx context.Context) (*msg.Msg, error) {
	select {
	case m := <-e.recvCh:
		return m, nil
	case <-e.myCh:
		// Our own end closed; the relay exits without flushing, so
		// don't wait on drainedCh.
		return nil, io.EOF
	case <-e.recvDir.drainedCh():
		// Every remaining message is already in recvCh.
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

// Drained returns a channel closed once every message the peer sent
// towards this end is observable on RecvCh: for an HWM-0 direction, after
// the peer closed (or SealRecv) and the relay flushed its queue and
// exited; for channel-backed directions it is always closed. From then
// on closing this end loses nothing — Recv returns buffered messages
// before EOF — and no relay goroutine is left to leak.
func (e *end) Drained() <-chan struct{} { return e.recvDir.drainedCh() }

// TryRecv returns one buffered message if available without blocking.
// It is intentionally not part of pipeiface.Pipe; the session writer uses
// it to top up a write batch. An empty result does not mean the queue is
// finished (an HWM-0 relay may be about to refill the channel); use
// DrainRecv for that.
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
		e.sendDir.closeInput()
		e.recvDir.close()
		// Wake any Poller parked on either end's recv-ready gate so it
		// re-checks and observes the close.
		e.armRecvReady()
		e.peerEnd.armRecvReady()
	})
	return nil
}

func (e *end) Closed() <-chan struct{} { return e.shared.either }

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
var _ pipeiface.Pipe = (*end)(nil)
