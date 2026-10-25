// Package pipe provides End, one side of the bidirectional in-memory message
// queue that connects a socket engine to a session (ZMTP or STREAM raw
// mode) or, on inproc, to another socket's engine.
//
// Each direction has its own high-water mark, a message count: a positive
// HWM bounds the direction and HWM 0 means unlimited. Storage is a chunked
// FIFO (see queue) that grows with the actual depth, so a large or
// unlimited HWM costs nothing until messages are queued. Producers and
// consumers of a direction never share a lock. A goroutine is never
// involved, so an End that is never closed leaks nothing but memory the GC
// reclaims.
//
// All I/O goes through End methods (there is no raw channel access). That is
// what keeps the wakeups, the idle notification and the SendDrained signal
// impossible to skip.
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

// direction is one half of the pipe: a queue plus the parking protocol for
// goroutines blocked on it.
//
// Parking. A consumer that finds the queue empty registers in rsleepers
// (R2) and only then re-checks the queue (C1 inside pop); a producer
// publishes its message (P2) and only then loads rsleepers (W1). All are
// sequentially consistent, so either the consumer's re-check sees the
// message (no park) or the producer sees the registration and arms rwake.
// The mirror pair guards blocked producers: wsleepers is registered (S1)
// before the producer's retry reloads pubHead (P1), and a consumer stores
// pubHead (C2) before loading wsleepers (R1).
//
// The sleeper counts are counters, not flags. With a flag, consumer A could
// wake, take message 1 and clear the flag while message 2 is queued and a
// second parked consumer B holds no token: a lost wakeup. With counters a
// waker that succeeds passes a baton: while sleepers remain and work (or
// room) is still available it arms the wake channel again. Invariant:
// whenever pending() and rsleepers > 0, a token is in rwake or a woken
// consumer holds it and has not re-checked yet. Stale tokens are harmless,
// a token only ever makes someone re-check. The same holds for room and
// wsleepers. With HWM 0 producers never park, so the producer baton is
// unreachable but stays well defined.
//
// Traps: arm never blocks and the wake channels are never closed (a close
// would wake every future sleeper too); every exit from a park path
// decrements its counter; no queue or drain mutex is held while calling arm
// or End.woken.
type direction struct {
	q *queue

	rsleepers atomic.Int32 // consumers parked or about to park
	wsleepers atomic.Int32 // producers parked or about to park at HWM
	rwake     chan struct{}
	wwake     chan struct{}

	// SendDrained state. draining is set (under dmu) while SendDrained
	// waits for the queue to empty; consume checks it after every pop.
	draining  atomic.Bool
	dmu       sync.Mutex
	drainCh   chan struct{} // closed once empty after SendDrained, or on close
	drainDone bool
}

func newDirection(hwm int) *direction {
	return &direction{
		q:     newQueue(hwm),
		rwake: make(chan struct{}, 1),
		wwake: make(chan struct{}, 1),
	}
}

// arm leaves a token in ch unless one is already there. Never blocks.
func arm(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// produce enqueues m if there is room and wakes a parked consumer.
func (d *direction) produce(m *msg.Msg) bool {
	if !d.q.push(m) {
		return false
	}
	if d.rsleepers.Load() > 0 { // W1
		arm(d.rwake)
	}
	return true
}

// consume takes the next queued message, wakes a parked producer and fires
// the SendDrained signal if this emptied the queue.
func (d *direction) consume() (*msg.Msg, bool) {
	m, ok := d.q.pop()
	if !ok {
		return nil, false
	}
	if d.wsleepers.Load() > 0 { // R1
		arm(d.wwake)
	}
	if d.draining.Load() {
		d.checkDrain()
	}
	return m, true
}

// checkDrain fires the drain signal if SendDrained is waiting and the
// queue is empty.
func (d *direction) checkDrain() {
	d.dmu.Lock()
	if d.draining.Load() && !d.q.pending() {
		d.fireDrainLocked()
	}
	d.dmu.Unlock()
}

// fireDrainLocked closes drainCh (creating it pre-closed if SendDrained has
// not asked yet) exactly once. Caller holds d.dmu.
func (d *direction) fireDrainLocked() {
	if d.drainDone {
		return
	}
	d.drainDone = true
	d.draining.Store(false)
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
	// consistent: the enqueue is published by the pubTail store, and the
	// receiver's re-check (a pop, or the pubTail load in pending) reads it
	// atomically. So as in Dekker's algorithm at least one side sees the other: the receiver finds the
	// message, or the sender finds idle set and notifies. When both happen
	// the receiver clears idle again with a CAS and notify may run once
	// for nothing. Close pairs a closed-flag store with the same idle
	// load, and a receiver checks IsClosed after setting idle.
	//
	// The re-check must be an atomic load (pop's pubTail load, or
	// queue.pending) so it cannot be satisfied before the Swap's store.
	idle   atomic.Bool
	notify atomic.Pointer[func()]

	// routingID is the identity a ROUTER or STREAM engine assigned this end
	// on attach. It travels with the pipe, so a message received after the
	// engine detached the pipe is still stamped with the right identity
	// without the engine retaining anything per departed peer.
	routingID atomic.Pointer[[]byte]

	// owed records that TrySendDeferred pushed messages whose wakeup
	// Flush has not published yet. Only the goroutine that calls
	// TrySendDeferred and Flush touches it.
	owed bool
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
	if m, ok := d.consume(); ok {
		return m, true
	}
	e.idle.Swap(true)
	if m, ok := d.consume(); ok {
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
	if d.q.pending() {
		return true
	}
	e.idle.Swap(true)
	if d.q.pending() {
		e.idle.CompareAndSwap(true, false)
		return true
	}
	return false
}

// Len reports how many messages are queued for this end. It is a gauge for
// metrics, not a readiness signal.
func (e *End) Len() int { return e.recv.q.length() }

// TrySend enqueues m for the peer without blocking. It returns false when
// m is nil, either end is closed, or a bounded direction is full. An
// unbounded (HWM 0) direction never refuses an open pipe.
func (e *End) TrySend(m *msg.Msg) bool {
	if m == nil || e.shared.closed.Load() {
		return false
	}
	if !e.send.produce(m) {
		return false
	}
	if e.peer.idle.Load() {
		e.peer.woken()
	}
	return true
}

// pushDeferred enqueues m exactly like produce but leaves the consumer
// wakeup to Flush.
func (d *direction) pushDeferred(m *msg.Msg) bool { return d.q.push(m) }

// TrySendDeferred is TrySend for a producer that publishes messages in
// batches: it enqueues m (visible to TryRecv and Pending at once) but wakes
// no parked receiver and runs no idle notification. The caller must call
// Flush before it blocks, goes idle or exits; Flush publishes the wakeups
// for the whole batch at once, which is what saves the park/unpark pair per
// message a fast consumer would otherwise cost.
//
// TrySendDeferred and Flush on one End must be called from one goroutine or
// under one external serialization (the session reader is the only user);
// the deferred state is not synchronized. Other send methods may be mixed
// in from other goroutines, they do not touch it.
//
// Why a late wakeup is safe. A consumer parks only after finding the queue
// empty (it registers in rsleepers and re-checks, the protocol of Recv is
// unchanged). Messages pushed after it parked are visible but it stays
// asleep until Flush, so the delay is bounded by the producer's flush
// discipline, one batch. The Dekker argument still holds at Flush time:
// Flush loads rsleepers after the pushes' pubTail stores (all sequentially
// consistent), so either the consumer's re-check saw the data or Flush sees
// the sleeper and arms rwake. The same argument covers the idle flag.
func (e *End) TrySendDeferred(m *msg.Msg) bool {
	if m == nil || e.shared.closed.Load() {
		return false
	}
	if !e.send.pushDeferred(m) {
		return false
	}
	e.owed = true
	return true
}

// Flush publishes the wakeups owed by TrySendDeferred: it arms a parked
// receiver and runs the peer's idle notification. It is a no-op when
// nothing is owed. Call it from the goroutine that calls TrySendDeferred.
func (e *End) Flush() {
	if !e.owed {
		return
	}
	e.owed = false
	if e.send.rsleepers.Load() > 0 { // W1
		arm(e.send.rwake)
	}
	if e.peer.idle.Load() {
		e.peer.woken()
	}
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
	d := e.send
	if d.produce(m) {
		if e.peer.idle.Load() {
			e.peer.woken()
		}
		return nil
	}
	for {
		d.wsleepers.Add(1) // S1
		if d.produce(m) {  // retry reloads pubHead (P1)
			d.wsleepers.Add(-1)
			// Baton: another parked producer may fit too.
			if d.wsleepers.Load() > 0 && d.q.hasRoom() {
				arm(d.wwake)
			}
			if e.peer.idle.Load() {
				e.peer.woken()
			}
			return nil
		}
		if e.shared.closed.Load() {
			d.wsleepers.Add(-1)
			return io.ErrClosedPipe
		}
		select {
		case <-d.wwake:
			d.wsleepers.Add(-1)
		case <-abort:
			d.wsleepers.Add(-1)
			return ErrAborted
		case <-e.shared.either:
			d.wsleepers.Add(-1)
			return io.ErrClosedPipe
		case <-ctx.Done():
			d.wsleepers.Add(-1)
			return ctx.Err()
		}
	}
}

// TryRecv returns the next queued message without blocking. Messages sent
// before a close stay receivable, so on a closed pipe a false result means
// the queue is exhausted.
func (e *End) TryRecv() (*msg.Msg, bool) {
	return e.recv.consume()
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
	// A queued message beats a fired abort or ctx.
	if m, ok := d.consume(); ok {
		return m, nil
	}
	for {
		d.rsleepers.Add(1)            // R2
		if m, ok := d.consume(); ok { // C1 inside
			d.rsleepers.Add(-1)
			// Baton: another parked consumer may have work too.
			if d.rsleepers.Load() > 0 && d.q.pending() {
				arm(d.rwake)
			}
			return m, nil
		}
		if e.shared.closed.Load() {
			d.rsleepers.Add(-1)
			// Close may have raced a final push.
			if m, ok := d.consume(); ok {
				return m, nil
			}
			return nil, io.EOF
		}
		select {
		case <-d.rwake:
			d.rsleepers.Add(-1)
		case <-abort:
			d.rsleepers.Add(-1)
			return nil, ErrAborted
		case <-e.shared.either:
			d.rsleepers.Add(-1)
			if m, ok := d.consume(); ok {
				return m, nil
			}
			return nil, io.EOF
		case <-ctx.Done():
			d.rsleepers.Add(-1)
			return nil, ctx.Err()
		}
	}
}

// Pending reports whether a message is queued for this end right now.
func (e *End) Pending() bool { return e.recv.q.pending() }

// SendDrained returns a channel closed once the peer has received every
// message this end sent, or either end closed. It is the linger-drain
// signal for an inproc pipe, whose peer is another live socket that
// may never close its end. Call it only once this end will send nothing
// more (after engine.Close).
func (e *End) SendDrained() <-chan struct{} {
	d := e.send
	d.dmu.Lock()
	defer d.dmu.Unlock()
	if d.drainCh == nil {
		d.drainCh = make(chan struct{})
		if d.drainDone {
			close(d.drainCh)
		}
	}
	if !d.drainDone {
		// draining is stored before the pending check: it pairs with
		// consume's pubHead store followed by its draining load, so
		// either this check sees the emptied queue or that pop sees
		// draining and fires.
		d.draining.Store(true)
		if !d.q.pending() {
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
			d.dmu.Lock()
			d.fireDrainLocked()
			d.dmu.Unlock()
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
