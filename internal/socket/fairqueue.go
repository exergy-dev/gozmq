package socket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// --------------------------------------------------------------------------
// FairQueue
// --------------------------------------------------------------------------

// FairQueue is a fair-queued inbound multiplexer over a dynamic set of
// *pipe.End values. Each attached pipe gets a forwarder goroutine that
// pumps its Recv into a single shared inbound channel; Recv on the
// FairQueue returns the next message from any peer.
//
// Goroutine accounting: exactly one forwarder goroutine per attached pipe.
// Detach signals its forwarder to stop; the forwarder may still be parked
// in Pipe.Recv at the time of Detach — it will exit on the next message,
// on pipe close, or on FairQueue.Close. Close blocks until every
// forwarder has exited.
type FairQueue struct {
	mu      sync.Mutex
	closed  bool
	closeCh chan struct{}
	inbound chan fairItem
	// readyR is the Poller-facing read-ready gate. It is armed from the
	// data path itself: every attached pipe gets the gate registered via
	// SetRecvReady (so direct-mode deliveries arm it with no forwarder in
	// the loop) and forwarders re-arm it on each inbound handoff.
	readyR readyGate
	pipes  map[*pipe.End]*fairForwarder
	wg     sync.WaitGroup

	// direct is an atomically-swapped snapshot of the direct-pipe state.
	// RecvWithSource reads it without holding mu; Attach/Detach/Close
	// write it under mu. The indirection eliminates a per-Recv mutex
	// acquisition on the hot path.
	direct atomic.Pointer[directSnapshot]

	// directActive counts receivers currently inside the direct-pipe read
	// path. The 1→2 promotion in Attach quiesces on it (after swapping
	// the snapshot out and closing wake) before starting the forwarder,
	// so a receiver holding the old snapshot can never race the fresh
	// forwarder on the pipe's recv channel and invert per-peer FIFO.
	directActive atomic.Int32

	// recvNotify is signalled when directPipe transitions between nil and
	// non-nil, waking any RecvWithSource goroutine that is blocked in
	// the multi-pipe select so it can re-check the direct-pipe path.
	recvNotify chan struct{}

	// recv counts messages successfully handed off from a forwarder to
	// the inbound channel (i.e. dequeued by Recv eventually). dropped
	// counts messages a forwarder pulled from a pipe but had to discard
	// because its context cancelled (Detach) or the FairQueue closed
	// before the handoff could complete.
	recv    atomic.Uint64
	dropped atomic.Uint64
}

// Recvd reports the number of messages successfully handed off to the
// inbound channel. Safe to call concurrently with the forwarders.
func (fq *FairQueue) Recvd() uint64 { return fq.recv.Load() }

// Dropped reports the number of messages a forwarder pulled from a pipe
// but had to discard because its context cancelled or the queue closed
// mid-handoff. Safe to call concurrently with the forwarders.
func (fq *FairQueue) Dropped() uint64 { return fq.dropped.Load() }

// QueueDepth returns the current length of the inbound handoff channel
// (gauge). Capacity is 1, so this is 0 or 1 except for very brief
// windows. Provided for parity with the Engine metrics surface.
// In direct-pipe mode (single peer), the handoff channel is bypassed
// entirely and the depth is always 0.
func (fq *FairQueue) QueueDepth() uint64 {
	if fq.direct.Load() != nil {
		return 0
	}
	return uint64(len(fq.inbound))
}

// directSnapshot holds the direct-pipe state read atomically by
// RecvWithSource. A nil *directSnapshot means no direct pipe.
type directSnapshot struct {
	pipe *pipe.End
	wake chan struct{}
}

type fairForwarder struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// fairItem pairs an inbound message with the pipe that produced it. ROUTER
// and REP need the source pipe to prepend the peer identity envelope; PULL
// and DEALER only want the message, so FairQueue exposes both shapes
// (Recv returns the message only; RecvWithSource returns both).
type fairItem struct {
	m *msg.Msg
	p *pipe.End
}

// NewFairQueue returns an empty FairQueue.
func NewFairQueue() *FairQueue {
	return &FairQueue{
		closeCh: make(chan struct{}),
		// Capacity 1 keeps a small handoff buffer so the forwarder isn't
		// blocked the moment the receiver pulls. Larger buffers would
		// undermine HWM, since HWM lives in the Pipe itself.
		inbound:    make(chan fairItem, 1),
		readyR:     newReadyGate(),
		recvNotify: make(chan struct{}, 1),
		pipes:      make(map[*pipe.End]*fairForwarder),
	}
}

// ReadyRecv returns a channel that is non-blocking-readable whenever
// the FairQueue has at least one buffered inbound message. The
// Poller uses this as the PollIn readiness signal. Reads from the
// channel do not consume the message; the gate is level-armed here and
// edge-armed by the data path (pipe enqueue / forwarder handoff), so a
// message arriving while the Poller is parked still wakes it. Callers
// should invoke HasPending alongside to handle the case where readyR
// was drained by a prior Poll but the buffered item was not yet
// consumed.
func (fq *FairQueue) ReadyRecv() <-chan struct{} {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	if fq.closed {
		return fq.readyR.ch
	}
	ready := len(fq.inbound) > 0
	if ds := fq.direct.Load(); ds != nil {
		ready = ready || ds.pipe.Pending()
	}
	return fq.readyR.signal(ready)
}

// HasPending reports whether the FairQueue has at least one inbound
// message buffered and ready to deliver. Used by the Poller for the
// immediate non-blocking PollIn check.
func (fq *FairQueue) HasPending() bool {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	if fq.closed {
		return false
	}
	if ds := fq.direct.Load(); ds != nil {
		return ds.pipe.Pending()
	}
	return len(fq.inbound) > 0
}

// Attach registers p and starts its forwarder goroutine. Idempotent.
// Returns zmqerr.ErrSocketClosed (wrapped) after Close.
//
// When attaching the very first pipe (0→1 transition), the pipe is stored
// as directPipe and no forwarder goroutine is started — RecvWithSource
// reads from it directly. On the 1→2 transition the existing directPipe
// is promoted to a forwarder-backed entry before the new pipe is added.
func (fq *FairQueue) Attach(p *pipe.End) error {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	if fq.closed {
		return fmt.Errorf("fairqueue attach: %w", zmqerr.ErrSocketClosed)
	}
	// Duplicate check: covers both direct and forwarder-backed pipes.
	ds := fq.direct.Load()
	if ds != nil && ds.pipe == p {
		return nil
	}
	if _, ok := fq.pipes[p]; ok {
		return nil
	}

	// Arm the Poller gate straight from the pipe's data path, so a
	// message arriving in direct mode (no forwarder) still wakes a
	// blocked Poll. Forwarder-mode pipes keep the hook too — the early
	// arm is a harmless level-trigger spurious wake at worst.
	p.SetRecvReady(fq.readyR.ch)

	// First pipe: use the direct path (no forwarder goroutine).
	if ds == nil && len(fq.pipes) == 0 {
		fq.direct.Store(&directSnapshot{pipe: p, wake: make(chan struct{})})
		select {
		case fq.recvNotify <- struct{}{}:
		default:
		}
		return nil
	}

	// Transitioning 1→N: promote the existing directPipe to a
	// forwarder-backed entry before adding the new one.
	if ds != nil {
		fq.demoteDirectLocked(ds)
		fq.startForwarderLocked(ds.pipe)
	}
	fq.startForwarderLocked(p)
	return nil
}

// demoteDirectLocked leaves direct mode: it swaps the snapshot out, wakes
// parked direct receivers, and waits for in-flight ones to finish. The
// quiesce matters whenever a forwarder is about to take over ds.pipe: a
// receiver that loaded the old snapshot just before the swap could
// otherwise race the forwarder on the pipe's recv channel and invert
// per-peer FIFO order. recvDirect re-checks the snapshot after publishing
// its activity, so no new read can start with the stale snapshot. Caller
// must hold fq.mu.
func (fq *FairQueue) demoteDirectLocked(ds *directSnapshot) {
	fq.direct.Store(nil)
	close(ds.wake)
	for fq.directActive.Load() != 0 {
		runtime.Gosched()
	}
}

// startForwarderLocked registers p in fq.pipes and starts its forwarder.
// Caller must hold fq.mu.
func (fq *FairQueue) startForwarderLocked(p *pipe.End) {
	ctx, cancel := context.WithCancel(context.Background())
	fwd := &fairForwarder{ctx: ctx, cancel: cancel}
	fq.pipes[p] = fwd
	fq.wg.Add(1)
	go fq.forward(p, fwd)
}

// Detach removes p and signals its forwarder to stop promptly. A pipe
// that has already closed is instead left to drain its buffered messages
// to EOF: the peer delivered them before leaving, and libzmq keeps them
// receivable. The draining forwarder stays registered in fq.pipes until it
// exits, which keeps a pipe attached meanwhile out of direct mode (direct
// receivers never read fq.inbound, so they would strand the drained
// messages).
func (fq *FairQueue) Detach(p *pipe.End) error {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	closed := false
	select {
	case <-p.Closed():
		closed = true
	default:
	}
	ds := fq.direct.Load()
	if ds != nil && ds.pipe == p {
		if closed {
			fq.demoteDirectLocked(ds)
			fq.startForwarderLocked(p)
		} else {
			fq.direct.Store(nil)
			close(ds.wake)
		}
		select {
		case fq.recvNotify <- struct{}{}:
		default:
		}
		return nil
	}
	fwd, ok := fq.pipes[p]
	if !ok || closed {
		return nil
	}
	delete(fq.pipes, p)
	fwd.cancel()
	return nil
}

// Recv blocks until a message arrives from any attached pipe, ctx
// cancels, or Close is called. Returns zmqerr.ErrSocketClosed (wrapped)
// on close. The source pipe is discarded; callers that need it should
// use RecvWithSource.
func (fq *FairQueue) Recv(ctx context.Context) (*msg.Msg, error) {
	it, err := fq.recvItem(ctx)
	return it.m, err
}

// RecvWithSource blocks like Recv and additionally reports the pipe from
// which the message was received. ROUTER uses this to stamp the leading
// peer-identity frame onto each inbound message; REP uses it to capture
// the delimiter envelope for the next Send. A closed pipe yields
// io.EOF inside the forwarder and is never surfaced here — the forwarder
// simply exits.
func (fq *FairQueue) RecvWithSource(ctx context.Context) (*msg.Msg, *pipe.End, error) {
	it, err := fq.recvItem(ctx)
	return it.m, it.p, err
}

// recvItem is the shared receive path behind Recv and RecvWithSource.
//
// Fast path: when exactly one pipe is attached (direct mode), it reads
// from that pipe directly — bypassing both the forwarder goroutine and the
// inbound channel — racing the read against the snapshot's wake channel
// instead of allocating a context. wake is closed by Detach/Close/promotion
// to unblock a parked read. Direct mode earns its complexity: against an
// always-forwarder FairQueue it measured 4.3x inproc PUSH/PULL throughput,
// 2.2x over TCP (32B), 12x inproc PUB/SUB fanout, and 22-36% lower
// REQ/REP and ROUTER/DEALER round-trip latency (perf/, 2026-09).
func (fq *FairQueue) recvItem(ctx context.Context) (fairItem, error) {
	for {
		if ds := fq.direct.Load(); ds != nil {
			it, retry, err := fq.recvDirect(ctx, ds)
			if retry {
				continue
			}
			return it, err
		}

		// Multi-pipe path: read from the shared inbound channel. The
		// non-blocking closeCh pre-check guards the close-with-buffered-item
		// drain: on close we must hand back any item already sitting in
		// fq.inbound before reporting ErrSocketClosed.
		select {
		case <-fq.closeCh:
			return fq.closedRecv()
		default:
		}

		select {
		case it := <-fq.inbound:
			return it, nil
		case <-fq.recvNotify:
			continue
		case <-fq.closeCh:
			return fq.closedRecv()
		case <-ctx.Done():
			return fairItem{}, ctx.Err()
		}
	}
}

// closedRecv hands back an item still parked in fq.inbound after Close,
// or reports ErrSocketClosed.
func (fq *FairQueue) closedRecv() (fairItem, error) {
	select {
	case it := <-fq.inbound:
		return it, nil
	default:
		return fairItem{}, fmt.Errorf("fairqueue recv: %w", zmqerr.ErrSocketClosed)
	}
}

// recvDirect performs one direct-mode read attempt against the snapshot
// ds. retry=true means the snapshot is stale or was swapped out mid-read —
// the caller loops and re-loads. The directActive counter brackets the
// whole attempt so Attach's promotion quiesce can wait out in-flight
// readers (see Attach). Close closes ds.wake, so the retry lands on the
// multi-pipe path's close handling.
func (fq *FairQueue) recvDirect(ctx context.Context, ds *directSnapshot) (fairItem, bool, error) {
	fq.directActive.Add(1)
	defer fq.directActive.Add(-1)
	// Re-check after publishing activity: a promotion that swapped the
	// snapshot out before our Add must not see us touch the pipe.
	if fq.direct.Load() != ds {
		return fairItem{}, true, nil
	}
	m, err := ds.pipe.RecvOr(ctx, ds.wake)
	switch {
	case err == nil:
		fq.recv.Add(1)
		return fairItem{m: m, p: ds.pipe}, false, nil
	case errors.Is(err, pipe.ErrAborted):
		return fairItem{}, true, nil
	case errors.Is(err, io.EOF):
		// The pipe closed and is empty. Detach will swap the snapshot
		// out (closing wake); wait for that rather than spin.
		select {
		case <-ds.wake:
			return fairItem{}, true, nil
		case <-ctx.Done():
			return fairItem{}, false, ctx.Err()
		}
	default:
		return fairItem{}, false, err
	}
}

// Close releases the FairQueue, signals all forwarders to stop, unblocks
// pending Recv with zmqerr.ErrSocketClosed, and waits for every forwarder
// to exit. Idempotent.
func (fq *FairQueue) Close() error {
	fq.mu.Lock()
	if fq.closed {
		fq.mu.Unlock()
		return nil
	}
	fq.closed = true
	ds := fq.direct.Load()
	fq.direct.Store(nil)
	if ds != nil {
		close(ds.wake)
	}
	close(fq.closeCh)
	forwarders := make([]*fairForwarder, 0, len(fq.pipes))
	for _, fwd := range fq.pipes {
		forwarders = append(forwarders, fwd)
	}
	fq.pipes = nil
	fq.mu.Unlock()
	for _, fwd := range forwarders {
		fwd.cancel()
	}
	fq.wg.Wait()
	return nil
}

// forward pumps p.Recv into fq.inbound until p closes, the forwarder is
// cancelled, or the FairQueue closes.
func (fq *FairQueue) forward(p *pipe.End, fwd *fairForwarder) {
	defer fq.wg.Done()
	defer func() {
		fq.mu.Lock()
		if cur, ok := fq.pipes[p]; ok && cur == fwd {
			delete(fq.pipes, p)
		}
		fq.mu.Unlock()
	}()
	for {
		m, err := p.RecvOr(fwd.ctx, nil)
		if err != nil {
			// ctx cancelled, pipe EOF, or other terminal error —
			// forwarder is done.
			return
		}
		// Hand off the message; race against stop and close so we never
		// block forever holding a message no one will consume.
		select {
		case fq.inbound <- fairItem{m: m, p: p}:
			fq.recv.Add(1)
			// Re-arm the Poller-facing read-ready gate now that the item
			// is actually consumable from fq.inbound.
			fq.readyR.arm()
		case <-fwd.ctx.Done():
			fq.dropped.Add(1)
			m.Release()
			return
		case <-fq.closeCh:
			fq.dropped.Add(1)
			m.Release()
			return
		}
	}
}
