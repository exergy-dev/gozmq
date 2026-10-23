package socket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// FairQueue is a fair-queued inbound multiplexer over a dynamic set of
// *pipe.End values, after libzmq's fq_t. It runs no goroutines: receivers
// read the pipes themselves.
//
// The attached pipes live in entries, whose first nActive elements are the
// active ones, which may hold a message. A receiver walks them round-robin
// from cur under mu. It deactivates a pipe it finds empty, after marking the
// pipe idle (pipe.End.RecvOrIdle), so the next message sent to that pipe
// runs the pipe's notification, which reactivates it and arms gate. That
// keeps the invariant a parked receiver relies on: every attached pipe is
// active, or idle, or has a notification in flight that will reactivate it.
//
// While exactly one pipe is attached, single publishes it and Recv blocks in
// that pipe directly without taking mu, which is what a single-peer socket
// costs anyway. Every change of single wakes gen, whose generation single
// was published with: that aborts receivers blocked in the old pipe and wakes
// receivers parked on gate, and both re-evaluate.
type FairQueue struct {
	mu      sync.Mutex
	closed  bool
	closeCh chan struct{}
	entries []*fqEntry
	byPipe  map[*pipe.End]*fqEntry
	nActive int
	cur     int
	gen     broadcast // woken whenever single changes

	// gate is the owning engine's in-gate: the single wake-up token shared
	// by receivers parked with nothing active and by the Poller. A
	// receiver that takes a message passes the token on, since someone
	// else may be waiting for what is left.
	gate chan struct{}

	single atomic.Pointer[fqSingle]

	// pending mirrors nActive > 0 for lock-free checks. False is exact:
	// nothing is receivable until a notification sets it again.
	pending atomic.Bool

	recv atomic.Uint64
}

// fqEntry is an attached pipe and its index in entries (-1 once removed).
type fqEntry struct {
	p   *pipe.End
	pos int
}

// fqSingle is the single-pipe fast path: p is the only attached pipe until
// gen closes.
type fqSingle struct {
	p   *pipe.End
	gen <-chan struct{}
}

// NewFairQueue returns an empty FairQueue that arms gate, a cap-1 channel,
// whenever a message may have become receivable.
func NewFairQueue(gate chan struct{}) *FairQueue {
	fq := &FairQueue{}
	fq.init(gate)
	return fq
}

// init readies a zero FairQueue, for owners that embed one by value.
func (fq *FairQueue) init(gate chan struct{}) {
	fq.closeCh = make(chan struct{})
	fq.byPipe = make(map[*pipe.End]*fqEntry)
	fq.gate = gate
}

// Recvd reports the number of messages handed to receivers.
func (fq *FairQueue) Recvd() uint64 { return fq.recv.Load() }

// QueueDepth reports the number of messages queued in the attached pipes.
func (fq *FairQueue) QueueDepth() uint64 {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	n := 0
	for _, e := range fq.entries {
		n += e.p.Len()
	}
	return uint64(n)
}

// Pending reports, without locking, whether a message may be queued. A
// false answer is exact until the next message arrives.
func (fq *FairQueue) Pending() bool { return fq.pending.Load() }

// CanRecv reports whether a Recv would find a message right now. A false
// answer leaves every pipe idle, so the next message arms the gate: a
// Poller may park on it after this.
func (fq *FairQueue) CanRecv() bool {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	if fq.closed {
		return false
	}
	for fq.nActive > 0 {
		e := fq.entries[0]
		if e.p.MarkIdle() {
			return true
		}
		fq.deactivateLocked(e)
	}
	return false
}

// Attach registers p. Idempotent. Returns zmqerr.ErrSocketClosed (wrapped)
// after Close.
func (fq *FairQueue) Attach(p *pipe.End) error {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	if fq.closed {
		return fmt.Errorf("fairqueue attach: %w", zmqerr.ErrSocketClosed)
	}
	if _, ok := fq.byPipe[p]; ok {
		return nil
	}
	e := &fqEntry{p: p, pos: len(fq.entries)}
	fq.entries = append(fq.entries, e)
	fq.byPipe[p] = e
	// Start inactive and idle, so a message already queued or sent from
	// now on activates the pipe. The notification needs mu, so it waits
	// until the entry is complete.
	p.SetNotify(func() { fq.notified(e) })
	if p.MarkIdle() {
		fq.activateLocked(e)
		arm(fq.gate)
	}
	fq.updateSingleLocked()
	return nil
}

// notified is the pipes' idle notification: a message (or a close) arrived
// on an idle pipe.
func (fq *FairQueue) notified(e *fqEntry) {
	fq.mu.Lock()
	live := e.pos >= 0
	if live {
		fq.activateLocked(e)
	}
	fq.mu.Unlock()
	if live {
		arm(fq.gate)
	}
}

// Detach removes p. A pipe that has already closed with messages queued
// stays until a receiver drains it to EOF: the peer delivered them before
// leaving, and libzmq keeps them receivable.
func (fq *FairQueue) Detach(p *pipe.End) error {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	e, ok := fq.byPipe[p]
	if !ok || p.IsClosed() && p.Pending() {
		return nil
	}
	fq.removeLocked(e)
	return nil
}

// Recv blocks until a message arrives from any attached pipe, ctx cancels,
// or Close is called. Returns zmqerr.ErrSocketClosed (wrapped) on close.
func (fq *FairQueue) Recv(ctx context.Context) (*msg.Msg, error) {
	m, _, err := fq.RecvWithSource(ctx)
	return m, err
}

// RecvWithSource blocks like Recv and additionally reports the pipe from
// which the message was received. ROUTER uses this to stamp the leading
// peer-identity frame onto each inbound message; REP uses it to capture
// the delimiter envelope for the next Send.
func (fq *FairQueue) RecvWithSource(ctx context.Context) (*msg.Msg, *pipe.End, error) {
	for {
		if s := fq.single.Load(); s != nil {
			m, err := s.p.RecvOr(ctx, s.gen)
			switch {
			case err == nil:
				fq.recv.Add(1)
				return m, s.p, nil
			case errors.Is(err, pipe.ErrAborted): // single changed
			case errors.Is(err, io.EOF):
				fq.dropClosed(s.p)
			default:
				return nil, nil, err
			}
			continue
		}

		fq.mu.Lock()
		if fq.closed {
			fq.mu.Unlock()
			return nil, nil, fmt.Errorf("fairqueue recv: %w", zmqerr.ErrSocketClosed)
		}
		if fq.single.Load() != nil {
			fq.mu.Unlock()
			continue
		}
		m, p := fq.takeLocked()
		gen := fq.gen.wait()
		fq.mu.Unlock()
		if m != nil {
			fq.recv.Add(1)
			return m, p, nil
		}
		select {
		case <-fq.gate:
		case <-gen:
		case <-fq.closeCh:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
}

// TryRecvWithSource is RecvWithSource without blocking: ok is false when
// no message is queued (the pipes are then idle, so the next one sets
// Pending and arms the gate) or the queue is closed.
func (fq *FairQueue) TryRecvWithSource() (m *msg.Msg, p *pipe.End, ok bool) {
	fq.mu.Lock()
	if !fq.closed {
		m, p = fq.takeLocked()
	}
	fq.mu.Unlock()
	if m == nil {
		return nil, nil, false
	}
	fq.recv.Add(1)
	return m, p, true
}

// takeLocked returns the next message round-robin from the active pipes,
// deactivating the empty ones it passes and removing the closed and empty
// ones, or nil once nothing is active. Caller holds fq.mu.
func (fq *FairQueue) takeLocked() (*msg.Msg, *pipe.End) {
	for fq.nActive > 0 {
		if fq.cur >= fq.nActive {
			fq.cur = 0
		}
		e := fq.entries[fq.cur]
		if m, ok := e.p.RecvOrIdle(); ok {
			fq.cur++
			arm(fq.gate)
			return m, e.p
		}
		// Checked after RecvOrIdle marked the pipe idle, so a close that
		// lands later still notifies and reactivates it.
		if e.p.IsClosed() && !e.p.Pending() {
			fq.removeLocked(e)
		} else {
			fq.deactivateLocked(e)
		}
	}
	return nil, nil
}

// dropClosed removes p after a single-pipe receiver read EOF from it.
func (fq *FairQueue) dropClosed(p *pipe.End) {
	fq.mu.Lock()
	if e, ok := fq.byPipe[p]; ok && !p.Pending() {
		fq.removeLocked(e)
	}
	fq.mu.Unlock()
}

func (fq *FairQueue) swapLocked(i, j int) {
	fq.entries[i], fq.entries[j] = fq.entries[j], fq.entries[i]
	fq.entries[i].pos, fq.entries[j].pos = i, j
}

func (fq *FairQueue) activateLocked(e *fqEntry) {
	if e.pos < fq.nActive {
		return
	}
	fq.swapLocked(e.pos, fq.nActive)
	fq.nActive++
	fq.pending.Store(true)
}

// deactivateLocked moves the active entry e out of the active prefix. The
// last active entry takes its slot, so a walk at e's index visits it next.
func (fq *FairQueue) deactivateLocked(e *fqEntry) {
	fq.nActive--
	fq.swapLocked(e.pos, fq.nActive)
	if fq.nActive == 0 {
		fq.pending.Store(false)
	}
}

func (fq *FairQueue) removeLocked(e *fqEntry) {
	if e.pos < fq.nActive {
		fq.deactivateLocked(e)
	}
	last := len(fq.entries) - 1
	fq.swapLocked(e.pos, last)
	fq.entries[last] = nil
	fq.entries = fq.entries[:last]
	e.pos = -1
	delete(fq.byPipe, e.p)
	e.p.SetNotify(nil)
	fq.updateSingleLocked()
}

// updateSingleLocked publishes the single-pipe fast path when exactly one
// pipe is attached and withdraws it otherwise, closing the old generation
// so receivers on either path re-evaluate. Caller holds fq.mu.
func (fq *FairQueue) updateSingleLocked() {
	var p *pipe.End
	if len(fq.entries) == 1 {
		p = fq.entries[0].p
	}
	old := fq.single.Load()
	if old == nil && p == nil || old != nil && old.p == p {
		return
	}
	fq.gen.wake()
	if p == nil {
		fq.single.Store(nil)
	} else {
		fq.single.Store(&fqSingle{p: p, gen: fq.gen.wait()})
	}
}

// Close releases the FairQueue and unblocks pending Recv with
// zmqerr.ErrSocketClosed. The pipes are left open (their owners close
// them). Idempotent.
func (fq *FairQueue) Close() error {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	if fq.closed {
		return nil
	}
	fq.closed = true
	close(fq.closeCh)
	for _, e := range fq.entries {
		e.p.SetNotify(nil)
		e.pos = -1
	}
	fq.entries, fq.byPipe, fq.nActive = nil, nil, 0
	fq.pending.Store(false)
	fq.single.Store(nil)
	fq.gen.wake()
	return nil
}
