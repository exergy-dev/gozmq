package pipe

import (
	"sync"
	"sync/atomic"

	"github.com/exergy-dev/gozmq/msg"
)

// chunkSize is the number of slots in one queue chunk. A chunk is about
// 2 KiB, so an idle direction costs one chunk and a deep one grows by a
// chunk per chunkSize messages.
const chunkSize = 256

// spareSlots is the capacity of the spare-chunk ring.
const spareSlots = 8

type chunk struct {
	vals [chunkSize]*msg.Msg
	next atomic.Pointer[chunk]
}

// queue is a FIFO of messages built from a linked list of fixed-size
// chunks, with a producer side and a consumer side that share no lock and,
// on the hot path, no cache line.
//
// Producers serialize on pmu and consumers on cmu. That is deliberate: the
// pipe is single-producer/single-consumer on the hot paths, but several
// entry points are genuinely multi-producer (PUB fan-out, ROUTER/STREAM
// SendOr after the engine lock is released) or multi-consumer (FairQueue's
// single-pipe fast path, PAIR, REQ TryRecv). The mutexes make those correct;
// being uncontended on the SPSC paths, they cost one atomic each.
//
// Cache-line rule (the [128]byte pads are load-bearing, and so is which
// side's mutex sits with which words): a word written on every operation by
// side X shares a cache line only with other words written on every
// operation by side X. pmu lives with the producer words, cmu with the
// consumer words. Check unsafe.Offsetof before concluding a design failed
// for lack of speed.
//
// Memory-ordering argument. Go's sync/atomic operations are sequentially
// consistent. The producer writes the slot and then stores pubTail (P2); a
// consumer that loads pubTail (C1) and observes the new value therefore
// sees the slot. Symmetrically the consumer nils the slot and then stores
// pubHead (C2); a producer that loads pubHead (P1) and observes it may
// reuse the slot (and the chunk). cachedHead and cachedTail are private
// copies of the other side's counter. They only ever lag, so they are
// conservative: a stale cachedHead can make a push report full when there
// is room (the caller then reloads), never overfill; a stale cachedTail can
// report empty when it is not, never invent an element. A failed push or
// pop leaves its cache in the "looks full/empty" state, so the next attempt
// reloads from the atomic.
//
// Chunk linkage rules:
//
//	RULE 1: the producer stores tailChunk.next = c BEFORE it publishes any
//	        tail that lands in c.
//	RULE 2: the consumer crosses a chunk boundary only AFTER proving
//	        head < tail, so the next pointer is non-nil (RULE 1 plus
//	        sequential consistency).
//	RULE 3: the consumer clears old.next BEFORE parking old in spares, so a
//	        recycled chunk never links to a stale successor.
type queue struct {
	hwm uint64 // 0 = unbounded

	// Producer private (written on every push).
	pmu        sync.Mutex
	tailChunk  *chunk
	tailPos    uint32
	tail       uint64
	cachedHead uint64
	getIdx     uint32 // next spares slot to take from; guarded by pmu
	_          [128]byte

	pubTail atomic.Uint64 // == tail, stored after the slot write
	_       [128]byte

	// Consumer private (written on every pop).
	cmu        sync.Mutex
	headChunk  *chunk
	headPos    uint32
	head       uint64
	cachedTail uint64
	putIdx     uint32 // next spares slot to fill; guarded by cmu
	_          [128]byte

	pubHead atomic.Uint64 // == head, stored after the slot is nil-ed
	_       [128]byte

	// spares is a small ring of drained chunks the consumer parks for the
	// producer to reuse before it allocates. The consumer fills slots in
	// ring order (CAS nil->chunk at putIdx, advancing only on success, so a
	// full ring drops the chunk to the GC) and the producer empties them in
	// the same order (Swap nil at getIdx, advancing only on success). Both
	// walk the ring in step, so a burst of chunk turnover is absorbed by up
	// to len(spares) parked chunks instead of one.
	spares [spareSlots]atomic.Pointer[chunk]
	_      [128]byte
}

// initQueue sets q up empty with first as its only chunk. first may live
// inside a larger allocation (newDirection); once drained it enters the
// spare ring like any other chunk, which is fine because the queue never
// outlives it.
func initQueue(q *queue, hwm int, first *chunk) {
	q.hwm = uint64(hwm)
	q.tailChunk, q.headChunk = first, first
}

// push appends m and reports whether there was room. Safe for concurrent
// producers.
func (q *queue) push(m *msg.Msg) bool {
	q.pmu.Lock()
	if q.hwm != 0 && q.tail-q.cachedHead >= q.hwm {
		q.cachedHead = q.pubHead.Load() // P1
		if q.tail-q.cachedHead >= q.hwm {
			q.pmu.Unlock()
			return false
		}
	}
	if q.tailPos == chunkSize {
		var c *chunk
		if c = q.spares[q.getIdx].Swap(nil); c != nil {
			q.getIdx = (q.getIdx + 1) % spareSlots
		} else {
			c = new(chunk)
		}
		q.tailChunk.next.Store(c) // RULE 1
		q.tailChunk, q.tailPos = c, 0
	}
	q.tailChunk.vals[q.tailPos] = m
	q.tailPos++
	q.tail++
	q.pubTail.Store(q.tail) // P2
	q.pmu.Unlock()
	return true
}

// pop removes and returns the oldest message. Safe for concurrent
// consumers.
func (q *queue) pop() (*msg.Msg, bool) {
	q.cmu.Lock()
	if q.head == q.cachedTail {
		q.cachedTail = q.pubTail.Load() // C1
		if q.head == q.cachedTail {
			q.cmu.Unlock()
			return nil, false
		}
	}
	if q.headPos == chunkSize { // RULE 2: head < tail is proven above
		old := q.headChunk
		q.headChunk, q.headPos = old.next.Load(), 0
		old.next.Store(nil) // RULE 3
		if q.spares[q.putIdx].CompareAndSwap(nil, old) {
			q.putIdx = (q.putIdx + 1) % spareSlots
		}
	}
	m := q.headChunk.vals[q.headPos]
	q.headChunk.vals[q.headPos] = nil // let the GC have it
	q.headPos++
	q.head++
	q.pubHead.Store(q.head) // C2
	q.cmu.Unlock()
	return m, true
}

// length reports the number of queued messages without locking. It loads
// pubHead first, so a concurrent consumer can only make the result too
// large, never negative; it is clamped to hwm for the same reason.
func (q *queue) length() int {
	h := q.pubHead.Load()
	n := q.pubTail.Load() - h
	if q.hwm != 0 && n > q.hwm {
		n = q.hwm
	}
	return int(n)
}

// pending reports whether a message is queued. Like length it loads
// pubHead first; a racing pop can make it spuriously true, which every
// caller tolerates (it re-checks with pop).
func (q *queue) pending() bool {
	h := q.pubHead.Load()
	return q.pubTail.Load() > h
}

// hasRoom reports whether a push would currently succeed.
func (q *queue) hasRoom() bool {
	return q.hwm == 0 || uint64(q.length()) < q.hwm
}
