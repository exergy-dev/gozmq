// Package pipe provides End, one side of the bidirectional in-memory message
// queue that connects a socket engine to a session, a STREAM adapter, or
// (for inproc direct pipes) another socket's engine.
//
// Each direction has its own high-water mark. A positive HWM is the capacity
// of a plain buffered channel. HWM 0 means unlimited: the direction is a
// channel of unboundedChanCap plus a mutex-guarded overflow slice that the
// receiving End refills the channel from after every receive. No goroutine
// is involved either way, so an End that is never closed leaks nothing but
// memory the GC reclaims.
//
// All I/O goes through End methods (there is no raw channel access). That is
// what keeps the overflow refill, the idle notification and the SendDrained
// signal impossible to skip.
package pipe

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/exergy-dev/gozmq/msg"
)

// ErrNilMessage is returned by Send when the caller supplies a nil *Msg.
var ErrNilMessage = errors.New("gozmq/pipe: nil message")

// ErrAborted is returned by SendOr and RecvOr when their abort channel
// fires before the operation completes.
var ErrAborted = errors.New("gozmq/pipe: aborted")

// PeerInfo carries the metadata a session advertises to its engine. Zero
// value = unknown; fields are set at pipe construction and may not change.
type PeerInfo struct {
	Identity []byte
}

// unboundedChanCap is the channel capacity of an HWM-0 direction. Messages
// beyond it wait in the overflow slice; a larger channel only means fewer
// trips through the overflow lock under a burst.
const unboundedChanCap = 1024

// NewPair returns two Ends bound to one another. aToB is the maximum
// number of messages buffered in the A→B direction; bToA is the B→A
// maximum. A HWM of 0 disables the limit for that direction; negative values
// are rejected with a panic (programmer error).
//
// aInfo describes the peer visible to the A end (i.e. B's endpoint/identity);
// bInfo describes what A looks like from B's side.
func NewPair(aToB, bToA int, aInfo, bInfo PeerInfo) (a, b *End) {
	if aToB < 0 || bToA < 0 {
		panic("gozmq/pipe: negative HWM")
	}
	s := &shared{either: make(chan struct{})}
	ab := newDirection(aToB)
	ba := newDirection(bToA)
	a = &End{shared: s, send: ab, recv: ba, peerInfo: aInfo}
	b = &End{shared: s, send: ba, recv: ab, peerInfo: bInfo}
	a.peer, b.peer = b, a
	return a, b
}

// shared is the close state common to both ends: closing either end closes
// the pair for sending, and wakes everything parked on it.
type shared struct {
	closed     atomic.Bool
	either     chan struct{}
	eitherOnce sync.Once
}

// direction is one half of the pipe.
//
// Unbounded (HWM 0) invariant: while overflow is non-empty the channel is
// full or about to be refilled, so a receiver parked on an empty channel is
// never stranded. Both sides keep it: a sender that appends to overflow
// sets slowOverflow before trying to move messages into the channel, and a
// receiver loads flags after taking from the channel and refills if the bit
// is set. Under Go's sequentially consistent synchronization one of the two
// always sees the other.
type direction struct {
	ch        chan *msg.Msg
	unbounded bool

	// flags holds the slow* bits. It is written only under mu but read
	// without it, so the hot paths cost one atomic load when all are
	// clear: the sender checks slowOverflow, the receiver checks all.
	flags atomic.Uint32

	mu       sync.Mutex
	overflow []*msg.Msg // unbounded only; overflow[head:] is queued, FIFO after ch
	head     int

	drainCh   chan struct{} // closed once empty after SendDrained, or on close
	drainDone bool
}

// Bits of direction.flags.
const (
	slowOverflow uint32 = 1 << iota // overflow holds messages
	slowDraining                    // SendDrained is waiting for the direction to empty
)

func newDirection(hwm int) *direction {
	if hwm > 0 {
		return &direction{ch: make(chan *msg.Msg, hwm)}
	}
	return &direction{ch: make(chan *msg.Msg, unboundedChanCap), unbounded: true}
}

// setFlagLocked sets or clears bit. Caller holds d.mu, which serializes
// every writer of flags.
func (d *direction) setFlagLocked(bit uint32, on bool) {
	f := d.flags.Load()
	if on {
		f |= bit
	} else {
		f &^= bit
	}
	d.flags.Store(f)
}

// trySend enqueues m without blocking. Bounded directions fail when the
// channel is full; unbounded ones always succeed, spilling into overflow
// (and staying there while anything older is) to keep FIFO order.
func (d *direction) trySend(m *msg.Msg) bool {
	if d.flags.Load()&slowOverflow == 0 {
		select {
		case d.ch <- m:
			return true
		default:
		}
	}
	if !d.unbounded {
		return false
	}
	d.mu.Lock()
	d.overflow = append(d.overflow, m)
	d.setFlagLocked(slowOverflow, true)
	d.refillLocked()
	d.mu.Unlock()
	return true
}

// refillLocked moves overflow into the channel, oldest first, until the
// channel is full or overflow is empty. Caller holds d.mu.
func (d *direction) refillLocked() {
	for d.head < len(d.overflow) {
		select {
		case d.ch <- d.overflow[d.head]:
			d.overflow[d.head] = nil
			d.head++
			continue
		default:
		}
		break
	}
	switch {
	case d.head == len(d.overflow):
		d.overflow, d.head = d.overflow[:0], 0
	case d.head > len(d.overflow)/2:
		// Compact so a long-lived backlog does not pin a growing prefix.
		n := copy(d.overflow, d.overflow[d.head:])
		clear(d.overflow[n:])
		d.overflow, d.head = d.overflow[:n], 0
	}
	d.setFlagLocked(slowOverflow, len(d.overflow) > 0)
}

// received runs after every successful receive: it refills the channel
// from overflow and fires the SendDrained signal once the direction is
// empty. Both cost one atomic load when there is nothing to do.
func (d *direction) received() {
	if d.flags.Load() != 0 {
		d.receivedSlow()
	}
}

func (d *direction) receivedSlow() {
	d.mu.Lock()
	defer d.mu.Unlock()
	f := d.flags.Load()
	if f&slowOverflow != 0 {
		d.refillLocked()
	}
	if f&slowDraining != 0 && len(d.ch) == 0 && d.flags.Load()&slowOverflow == 0 {
		d.fireDrainLocked()
	}
}

// tryRecv takes the next queued message without blocking.
func (d *direction) tryRecv() (*msg.Msg, bool) {
	for {
		select {
		case m := <-d.ch:
			d.received()
			return m, true
		default:
		}
		// The channel is empty. A sender may be between setting
		// slowOverflow and moving its message in; do the move ourselves.
		if d.flags.Load()&slowOverflow == 0 {
			return nil, false
		}
		d.mu.Lock()
		d.refillLocked()
		d.mu.Unlock()
	}
}

// pending reports whether a message is ready to receive. flags is loaded
// first: that atomic load orders the plain len read after whatever atomic
// the caller did before (see End.MarkIdle).
func (d *direction) pending() bool {
	return d.flags.Load()&slowOverflow != 0 || len(d.ch) > 0
}

// fireDrainLocked closes drainCh (creating it pre-closed if SendDrained has
// not asked yet) exactly once. Caller holds d.mu.
func (d *direction) fireDrainLocked() {
	if d.drainDone {
		return
	}
	d.drainDone = true
	d.setFlagLocked(slowDraining, false)
	if d.drainCh != nil {
		close(d.drainCh)
	}
}

// End is one side of a pipe pair. All methods are safe for concurrent use.
type End struct {
	shared   *shared
	send     *direction // this end writes, the peer reads
	recv     *direction // the peer writes, this end reads
	peer     *End
	peerInfo PeerInfo

	closeOnce sync.Once

	// idle and notify let a receiver that multiplexes many pipes (or a
	// Poller) stop looking at this end until something arrives, with no
	// goroutine per pipe. The receiver sets idle when it finds the end
	// empty (RecvOrIdle, MarkIdle); the first enqueue or close after that
	// clears it and runs notify on the sending goroutine. While idle is
	// clear a send costs one extra atomic load.
	//
	// No wakeup is lost. The receiver sets idle with an atomic Swap and
	// only then looks at the queue again; the sender publishes its message
	// and only then loads idle. Both idle operations are sequentially
	// consistent, the enqueue is published by the channel lock release (a
	// full barrier in the runtime) or, for an HWM-0 spill, by the
	// slowOverflow flag store, and the receiver's re-check reads the
	// channel count and flags with atomic loads. So as in Dekker's
	// algorithm at least one side sees the other: the receiver finds the
	// message, or the sender finds idle set and notifies. When both happen
	// the receiver clears idle again with a CAS and notify may run once
	// for nothing. Close pairs a closed-flag store with the same idle
	// load, and a receiver checks IsClosed after setting idle.
	//
	// The re-check must not be a bare len(ch): that plain load could be
	// satisfied before the Swap's store on weakly ordered CPUs. RecvOrIdle
	// re-checks with a channel receive; MarkIdle, which must not consume,
	// loads flags atomically before len (direction.pending).
	idle   atomic.Bool
	notify atomic.Pointer[func()]

	// routingID is the identity a ROUTER or STREAM engine assigned this end
	// on attach. It travels with the pipe, so a message received after the
	// engine detached the pipe is still stamped with the right identity
	// without the engine retaining anything per departed peer.
	routingID atomic.Pointer[[]byte]
}

// SetNotify registers fn as this end's idle notification (see the idle
// field doc); nil removes it. fn runs on the goroutine that sends to or
// closes the pipe, possibly with that goroutine's locks held, so it must be
// quick and must not send on or close this pipe.
func (e *End) SetNotify(fn func()) {
	if fn == nil {
		e.notify.Store(nil)
		return
	}
	e.notify.Store(&fn)
}

// woken is the sender side of the idle protocol: it runs the notification
// if the receiver went idle. Callers check idle first so the common case
// stays one inlined atomic load.
func (e *End) woken() {
	if !e.idle.CompareAndSwap(true, false) {
		return
	}
	if fn := e.notify.Load(); fn != nil {
		(*fn)()
	}
}

// RecvOrIdle takes the next queued message without blocking. When there is
// none it marks this end idle, so the next message sent here (or a close)
// runs the notification, and reports false.
func (e *End) RecvOrIdle() (*msg.Msg, bool) {
	d := e.recv
	if m, ok := d.tryRecv(); ok {
		return m, true
	}
	e.idle.Swap(true)
	if m, ok := d.tryRecv(); ok {
		// Raced a sender. If it already cleared idle its notification is
		// merely early; otherwise nothing will notify for m.
		e.idle.CompareAndSwap(true, false)
		return m, true
	}
	return nil, false
}

// MarkIdle is RecvOrIdle without the receive, for readiness checks: it
// reports whether a message is queued and, if not, marks this end idle.
func (e *End) MarkIdle() bool {
	d := e.recv
	if d.pending() {
		return true
	}
	e.idle.Swap(true)
	if d.pending() {
		e.idle.CompareAndSwap(true, false)
		return true
	}
	return false
}

// Len reports how many messages are queued for this end. It is a gauge for
// metrics, not a readiness signal.
func (e *End) Len() int {
	d := e.recv
	n := len(d.ch)
	if d.flags.Load()&slowOverflow != 0 {
		d.mu.Lock()
		n += len(d.overflow) - d.head
		d.mu.Unlock()
	}
	return n
}

// TrySend enqueues m for the peer without blocking. It returns false when
// m is nil, either end is closed, or a bounded direction is full. An
// unbounded (HWM 0) direction never refuses an open pipe.
func (e *End) TrySend(m *msg.Msg) bool {
	if m == nil || e.shared.closed.Load() {
		return false
	}
	if !e.send.trySend(m) {
		return false
	}
	if e.peer.idle.Load() {
		e.peer.woken()
	}
	return true
}

// Send enqueues m for the peer, blocking while a bounded direction is full.
// It returns io.ErrClosedPipe once either end is closed and ctx.Err() if
// ctx ends first.
func (e *End) Send(ctx context.Context, m *msg.Msg) error {
	return e.enqueue(ctx, m, nil)
}

// SendOr is Send that also gives up with ErrAborted when abort fires, so an
// engine can race a blocked send against its own close or peer-set change
// without allocating a context.
func (e *End) SendOr(ctx context.Context, m *msg.Msg, abort <-chan struct{}) error {
	return e.enqueue(ctx, m, abort)
}

func (e *End) enqueue(ctx context.Context, m *msg.Msg, abort <-chan struct{}) error {
	if m == nil {
		return ErrNilMessage
	}
	if e.shared.closed.Load() {
		return io.ErrClosedPipe
	}
	if e.send.trySend(m) {
		if e.peer.idle.Load() {
			e.peer.woken()
		}
		return nil
	}
	select {
	case e.send.ch <- m:
		if e.peer.idle.Load() {
			e.peer.woken()
		}
		return nil
	case <-abort:
		return ErrAborted
	case <-e.shared.either:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TryRecv returns the next queued message without blocking. Messages sent
// before a close stay receivable, so on a closed pipe a false result means
// the queue is exhausted.
func (e *End) TryRecv() (*msg.Msg, bool) {
	return e.recv.tryRecv()
}

// Recv returns the next message from the peer, blocking until one arrives.
// Once either end is closed it hands out whatever is still queued and then
// returns io.EOF; it returns ctx.Err() if ctx ends first.
func (e *End) Recv(ctx context.Context) (*msg.Msg, error) {
	return e.RecvOr(ctx, nil)
}

// RecvOr is Recv that also gives up with ErrAborted when abort fires.
func (e *End) RecvOr(ctx context.Context, abort <-chan struct{}) (*msg.Msg, error) {
	d := e.recv
	select {
	case m := <-d.ch:
		d.received()
		return m, nil
	default:
	}
	return e.recvSlow(ctx, abort)
}

// recvSlow is RecvOr once the channel was found empty: it refills from
// overflow if a sender left something there, then blocks.
func (e *End) recvSlow(ctx context.Context, abort <-chan struct{}) (*msg.Msg, error) {
	d := e.recv
	if m, ok := d.tryRecv(); ok {
		return m, nil
	}
	select {
	case m := <-d.ch:
		d.received()
		return m, nil
	case <-abort:
		return nil, ErrAborted
	case <-e.shared.either:
		if m, ok := d.tryRecv(); ok {
			return m, nil
		}
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Pending reports whether a message is queued for this end right now.
func (e *End) Pending() bool { return e.recv.pending() }

// SendDrained returns a channel closed once the peer has received every
// message this end sent, or either end closed. It is the linger-drain
// signal for an inproc direct pipe, whose peer is another live socket that
// may never close its end. Call it only once this end will send nothing
// more (after engine.Close).
func (e *End) SendDrained() <-chan struct{} {
	d := e.send
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.drainCh == nil {
		d.drainCh = make(chan struct{})
		if d.drainDone {
			close(d.drainCh)
		}
	}
	if !d.drainDone {
		d.setFlagLocked(slowDraining, true)
		if len(d.ch) == 0 && d.flags.Load()&slowOverflow == 0 {
			d.fireDrainLocked()
		}
	}
	return d.drainCh
}

// Close closes the pipe for sending in both directions and wakes every
// blocked Send and Recv. Messages already queued stay receivable from
// either end. Idempotent.
func (e *End) Close() error {
	e.closeOnce.Do(func() {
		e.shared.closed.Store(true)
		e.shared.eitherOnce.Do(func() { close(e.shared.either) })
		for _, d := range [...]*direction{e.send, e.recv} {
			d.mu.Lock()
			d.fireDrainLocked()
			d.mu.Unlock()
		}
		// Notify idle receivers on either end so they observe the close.
		e.woken()
		e.peer.woken()
	})
	return nil
}

// Closed returns a channel that is closed once either end has been closed.
func (e *End) Closed() <-chan struct{} { return e.shared.either }

// IsClosed reports whether either end has been closed.
func (e *End) IsClosed() bool { return e.shared.closed.Load() }

// SetRoutingID records id as this end's routing identity (see the routingID
// field doc). The engine sets it before attaching the end, so every message
// the end yields can read it; id must not be mutated afterwards.
func (e *End) SetRoutingID(id []byte) { e.routingID.Store(&id) }

// RoutingID returns the identity last set by SetRoutingID, or nil.
func (e *End) RoutingID() []byte {
	if id := e.routingID.Load(); id != nil {
		return *id
	}
	return nil
}

// PeerIdentity returns the ZMTP Identity advertised by the peer in its
// READY metadata. Empty when the peer did not set one.
func (e *End) PeerIdentity() []byte { return e.peerInfo.Identity }
