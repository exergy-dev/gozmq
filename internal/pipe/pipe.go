// Package pipe provides End, one side of the bidirectional in-memory message
// queue that connects a socket engine to a session (ZMTP or STREAM raw
// mode) or, on inproc, to another socket's engine.
//
// Each direction has its own high-water mark, a message count: a positive
// HWM bounds the direction and HWM 0 means unlimited.
//
// Two kinds of pipe exist, differing in how a direction stores messages.
// NewPair builds the queue kind, used for session pipes: a chunked FIFO
// (see queue) that grows with the actual depth, so a large or unlimited HWM
// costs nothing until messages are queued. Its producers and consumers
// never share a lock, and no goroutine is involved, so an End that is never
// closed leaks nothing but memory the GC reclaims. NewChannelPair builds
// the channel kind, used for inproc: a buffered channel of HWM capacity,
// except that HWM 0 has no capacity to size and gets a queue-kind direction.
// The queue wins when a producer batches wakeups or producer and consumer
// contend, as on TCP streaming. The channel wins for the uncontended
// one-hop-per-message inproc patterns (ping-pong, wide fan-out), where the
// runtime's direct handoff to a parked receiver is cheaper than the queue's
// park protocol and nothing ever batches.
//
// Both kinds share End and every guarantee below. Messages are reachable
// only through End methods, which is what keeps the wakeups, the idle
// notification and the SendDrained signal impossible to skip. Producers
// wake a parked consumer per message (TrySend, SendOr), except the session
// reader, which publishes a read batch with TrySendDeferred and wakes once
// with Flush, as libzmq's ypipe does.
package pipe

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/exergy-dev/gozmq/msg"
)

// ErrAborted is returned by SendOr and RecvOr when their abort channel
// fires before the operation completes.
var ErrAborted = errors.New("gozmq/pipe: aborted")

// NewPair returns two Ends bound to one another. aToB is the maximum
// number of messages buffered in the A→B direction; bToA is the B→A
// maximum. A HWM of 0 disables the limit for that direction; negative values
// are rejected with a panic (programmer error).
//
// aIdentity is the ZMTP identity of the peer as seen from the A end (B's),
// bIdentity the one B sees (A's); nil means unknown. They are fixed for the
// life of the pair.
//
// The pair uses the queue kind (see the package doc).
func NewPair(aToB, bToA int, aIdentity, bIdentity []byte) (a, b *End) {
	return newPair(newDirection, aToB, bToA, aIdentity, bIdentity)
}

// NewChannelPair is NewPair for the channel kind (see the package doc),
// which inproc uses. Arguments and semantics are identical.
func NewChannelPair(aToB, bToA int, aIdentity, bIdentity []byte) (a, b *End) {
	return newPair(newChannelDirection, aToB, bToA, aIdentity, bIdentity)
}

func newPair(mk func(int) *direction, aToB, bToA int, aIdentity, bIdentity []byte) (a, b *End) {
	if aToB < 0 || bToA < 0 {
		panic("gozmq/pipe: negative HWM")
	}
	s := &shared{either: make(chan struct{})}
	ab := mk(aToB)
	ba := mk(bToA)
	a = &End{shared: s, send: ab, recv: ba, peerIdentity: aIdentity}
	b = &End{shared: s, send: ba, recv: ab, peerIdentity: bIdentity}
	a.peer, b.peer = b, a
	return a, b
}

// shared is the close state common to both ends: closing either end closes
// the pair for sending, and wakes everything parked on it.
type shared struct {
	closed atomic.Bool
	either chan struct{}
	once   sync.Once // runs the close sequence for whichever end closes first
}

// direction is one half of the pipe, of the queue or the channel kind (see
// the end of this comment).
//
// Parking (queue kind). A consumer that finds the queue empty registers in rsleepers
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
//
// With q set a direction is the queue kind and everything above applies. With q nil it is the channel kind: ch carries
// the messages, the runtime parks and wakes the goroutines, and the
// sleeper counters and wake channels are unused. The channel kind is always
// bounded; an HWM-0 channel pair gets queue-kind directions.
type direction struct {
	q *queue

	ch chan *msg.Msg // channel kind only

	rsleepers atomic.Int32 // consumers parked or about to park
	wsleepers atomic.Int32 // producers parked or about to park at HWM
	rwake     chan struct{}
	wwake     chan struct{}

	// SendDrained state. draining is set (under dmu) while SendDrained
	// waits for the direction to empty; every receive checks it.
	draining  atomic.Bool
	dmu       sync.Mutex
	drainCh   chan struct{} // closed once empty after SendDrained, or on close
	drainDone bool
}

// newDirection makes a queue-kind direction. The direction, its queue and
// the queue's first chunk are one allocation: a TCP peer costs a pair of
// these, and the gate counts allocations per connection.
func newDirection(hwm int) *direction {
	dq := &struct {
		direction
		queue
		first chunk
	}{}
	d := &dq.direction
	d.q = &dq.queue
	initQueue(d.q, hwm, &dq.first)
	d.rwake = make(chan struct{}, 1)
	d.wwake = make(chan struct{}, 1)
	return d
}

// newChannelDirection makes a channel-kind direction. HWM 0 has no bounded
// channel to use, so it gets a queue-kind direction.
func newChannelDirection(hwm int) *direction {
	if hwm == 0 {
		return newDirection(0)
	}
	return &direction{ch: make(chan *msg.Msg, hwm)}
}

// pending reports whether a message is ready to receive.
func (d *direction) pending() bool {
	if d.q == nil {
		return len(d.ch) > 0
	}
	return d.q.pending()
}

// length reports how many messages are queued.
func (d *direction) length() int {
	if d.q == nil {
		return len(d.ch)
	}
	return d.q.length()
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
	if d.q == nil {
		// Channel kind: send without blocking (inlined, so TrySend is one
		// call deep).
		select {
		case d.ch <- m:
			return true
		default:
			return false
		}
	}
	if !d.q.push(m) {
		return false
	}
	if d.rsleepers.Load() > 0 { // W1
		arm(d.rwake)
	}
	return true
}

// consume takes the next queued message, wakes a parked producer and fires
// the SendDrained signal if this emptied the direction.
func (d *direction) consume() (*msg.Msg, bool) {
	if d.q == nil {
		// Channel kind: receive without blocking (inlined, see produce).
		select {
		case m := <-d.ch:
			d.received()
			return m, true
		default:
			return nil, false
		}
	}
	m, ok := d.q.pop()
	if !ok {
		return nil, false
	}
	if d.wsleepers.Load() > 0 { // R1
		arm(d.wwake)
	}
	d.received()
	return m, true
}

// received runs after every successful receive: it fires the SendDrained
// signal if SendDrained is waiting and this emptied the direction. It costs
// one atomic load when nobody waits.
func (d *direction) received() {
	if d.draining.Load() {
		d.checkDrain()
	}
}

// checkDrain fires the drain signal if SendDrained is waiting and the
// direction is empty.
func (d *direction) checkDrain() {
	d.dmu.Lock()
	if d.draining.Load() && !d.pending() {
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
	shared       *shared
	send         *direction // this end writes, the peer reads
	recv         *direction // the peer writes, this end reads
	peer         *End
	peerIdentity []byte

	// idle and notify let a receiver that multiplexes many pipes (or a
	// Poller) stop looking at this end until something arrives, with no
	// goroutine per pipe. The receiver sets idle when it finds the end
	// empty (RecvOrIdle, MarkIdle); the first enqueue or close after that
	// clears it and runs notify on the sending goroutine. While idle is
	// clear a send costs one extra atomic load.
	//
	// No wakeup is lost, for either kind, by Dekker's argument. The
	// receiver sets idle with an atomic Swap and only then looks at the
	// direction again; the sender publishes its message and only then loads
	// idle. So at least one side sees the other: the receiver finds the
	// message, or the sender finds idle set and notifies. When both happen
	// the receiver clears idle again with a CAS and notify may run once for
	// nothing. Close pairs a closed-flag store with the same idle load, and
	// a receiver checks IsClosed after setting idle.
	//
	// The receiver's re-check must not be satisfied before the Swap's store.
	// Queue kind: the re-check is an atomic load (pop's pubTail load, or
	// queue.pending), and the enqueue is published by the pubTail store.
	// Channel kind: the re-check is the plain read len(ch). idle.Swap is an
	// atomic read-modify-write, which on every Go target is a full barrier,
	// so that read cannot be satisfied before the Swap's store; and the
	// sender's channel send publishes the message under the channel lock
	// before its idle.Load, so a len(ch) that misses the message means the
	// send, and therefore the sender's idle.Load, comes after the Swap.
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

// wakePeer is the sender's half of the idle protocol: it runs the peer's
// notification if the peer went idle. One inlined atomic load when it did
// not.
func (e *End) wakePeer() {
	if e.peer.idle.Load() {
		e.peer.woken()
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
func (e *End) Len() int { return e.recv.length() }

// TrySend enqueues m, which must not be nil, for the peer without blocking.
// It returns false when either end is closed or a bounded direction is
// full. An unbounded (HWM 0) direction never refuses an open pipe.
func (e *End) TrySend(m *msg.Msg) bool {
	if e.shared.closed.Load() {
		return false
	}
	if !e.send.produce(m) {
		return false
	}
	e.wakePeer()
	return true
}

// TrySendDeferred is TrySend for a producer that publishes messages in
// batches, on a queue-kind End only (it panics on a channel-kind one): it enqueues m (visible to TryRecv and Pending at once) but wakes
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
	if e.send.q == nil {
		panic("gozmq/pipe: TrySendDeferred on a channel-kind pipe")
	}
	if e.shared.closed.Load() || !e.send.q.push(m) {
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
	e.wakePeer()
}

// Send enqueues m, which must not be nil, for the peer, blocking while a
// bounded direction is full. It returns io.ErrClosedPipe once either end is
// closed and ctx.Err() if ctx ends first.
func (e *End) Send(ctx context.Context, m *msg.Msg) error {
	return e.SendOr(ctx, m, nil)
}

// SendOr is Send that also gives up with ErrAborted when abort fires, so an
// engine can race a blocked send against its own close or peer-set change
// without allocating a context. A nil abort never fires.
func (e *End) SendOr(ctx context.Context, m *msg.Msg, abort <-chan struct{}) error {
	if e.shared.closed.Load() {
		return io.ErrClosedPipe
	}
	d := e.send
	if d.produce(m) {
		e.wakePeer()
		return nil
	}
	if d.q == nil {
		return e.enqueueChan(ctx, m, abort)
	}
	for {
		d.wsleepers.Add(1)   // S1
		sent := d.produce(m) // retry reloads pubHead (P1)
		var err error
		if !sent {
			if e.shared.closed.Load() {
				err = io.ErrClosedPipe
			} else {
				select {
				case <-d.wwake:
				case <-abort:
					err = ErrAborted
				case <-e.shared.either:
					err = io.ErrClosedPipe
				case <-ctx.Done():
					err = ctx.Err()
				}
			}
		}
		d.wsleepers.Add(-1)
		if sent {
			// Baton: another parked producer may fit too.
			if d.wsleepers.Load() > 0 && d.q.hasRoom() {
				arm(d.wwake)
			}
			e.wakePeer()
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// enqueueChan is SendOr's blocking path for the channel kind, entered once
// a non-blocking send found the bounded channel full.
func (e *End) enqueueChan(ctx context.Context, m *msg.Msg, abort <-chan struct{}) error {
	select {
	case e.send.ch <- m:
		e.wakePeer()
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
	return e.recv.consume()
}

// RecvOr returns the next message from the peer, blocking until one arrives.
// Once either end is closed it hands out whatever is still queued and then
// returns io.EOF; it returns ctx.Err() if ctx ends first and ErrAborted if
// abort fires first. A nil abort never fires.
func (e *End) RecvOr(ctx context.Context, abort <-chan struct{}) (*msg.Msg, error) {
	d := e.recv
	if d.q == nil {
		select {
		case m := <-d.ch:
			d.received()
			return m, nil
		default:
		}
		return e.recvSlowChan(ctx, abort)
	}
	// A queued message beats a fired abort or ctx.
	if m, ok := d.consume(); ok {
		return m, nil
	}
	// Stay registered in rsleepers across wakeups: a woken consumer
	// re-checks the queue under the same registration instead of
	// deregistering and registering again (two atomic RMWs per wake). A
	// producer that sees the registration may arm a stale token, which only
	// makes someone re-check.
	d.rsleepers.Add(1) // R2
	for {
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
			return d.recvAfterClose() // close may have raced a final push
		}
		select {
		case <-d.rwake:
		case <-abort:
			d.rsleepers.Add(-1)
			return nil, ErrAborted
		case <-e.shared.either:
			d.rsleepers.Add(-1)
			return d.recvAfterClose()
		case <-ctx.Done():
			d.rsleepers.Add(-1)
			return nil, ctx.Err()
		}
	}
}

// recvSlowChan is RecvOr for the channel kind once the channel was found
// empty: it blocks.
func (e *End) recvSlowChan(ctx context.Context, abort <-chan struct{}) (*msg.Msg, error) {
	d := e.recv
	select {
	case m := <-d.ch:
		d.received()
		return m, nil
	case <-abort:
		return nil, ErrAborted
	case <-e.shared.either:
		return d.recvAfterClose()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// recvAfterClose is a receive on a closed pipe: one last queued message if
// there is one, else io.EOF.
func (d *direction) recvAfterClose() (*msg.Msg, error) {
	if m, ok := d.consume(); ok {
		return m, nil
	}
	return nil, io.EOF
}

// Pending reports whether a message is queued for this end right now.
func (e *End) Pending() bool { return e.recv.pending() }

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
		// the receive's removal of the message followed by its draining
		// load, so either this check sees the emptied direction or that
		// receive sees draining and fires.
		d.draining.Store(true)
		if !d.pending() {
			d.fireDrainLocked()
		}
	}
	return d.drainCh
}

// Close closes the pipe for sending in both directions and wakes every
// blocked Send and Recv. Messages already queued stay receivable from
// either end. Idempotent.
func (e *End) Close() error {
	e.shared.once.Do(func() {
		e.shared.closed.Store(true)
		close(e.shared.either)
		for _, d := range [...]*direction{e.send, e.recv} {
			d.dmu.Lock()
			d.fireDrainLocked()
			d.dmu.Unlock()
		}
		// Notify idle receivers on either end so they observe the close.
		// A later Close from the other end skips this: a receiver marks
		// itself idle and then re-checks IsClosed, so it cannot sleep
		// through a close that was already published.
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
func (e *End) PeerIdentity() []byte { return e.peerIdentity }
