// Package socket holds the per-pattern SocketEngine implementations that
// back the public gozmq Socket types. Each ZMTP 3.1 socket pattern — PAIR,
// PUSH, PULL, PUB, SUB, XPUB, XSUB, REQ, REP, DEALER, ROUTER, STREAM — is
// realised by its own engine in this package.
//
// # SocketEngine contract
//
// The per-pattern engines implement iface.SocketEngine. That interface
// binds a narrow lifecycle (AttachPipe, DetachPipe, Close) to a two-method
// dispatch surface (Send, Recv). Pipes are handed to the engine by the
// session layer post-handshake; the engine owns all routing, subscription,
// and FSM state associated with those pipes. Close unblocks any pending
// Send/Recv with zmqerr.ErrSocketClosed and detaches every pipe.
//
// Error sentinels are stable: engines surface zmqerr.ErrSocketClosed after
// Close, zmqerr.ErrFSMViolation for patterns that disallow a direction
// (e.g. PUSH.Recv, SUB.Send) or a mis-ordered FSM step (REQ/REP), and
// zmqerr.ErrRouteUnknown on ROUTER with the mandatory option set.
//
// # Reusable primitives
//
// Two primitives defined here are shared across engines so each pattern
// can stay focused on its policy:
//
//   - LoadBalancer — strict round-robin outbound dispatcher over the
//     attached pipe set. Used by PUSH, DEALER, and REQ.
//   - FairQueue — fair-queued inbound multiplexer that pumps every
//     attached pipe into a single shared channel. Used by PULL, DEALER,
//     REP, ROUTER, SUB, and XSUB.
//
// Both depend only on iface.Pipe so engine-specific logic (identity
// prefixing, subscription filtering, FSM state, sticky peer selection)
// stays in the per-engine files rather than leaking into the primitive.
//
// A third shared primitive, SubTree, is a refcounted byte-prefix trie used
// by PUB/XPUB (one per peer) and SUB/XSUB (one per socket) for subscription
// matching.
//
// # Thread-safety model
//
// Engine method calls are serialized by the calling Socket's mutex: the
// public Socket takes its own lock before invoking Send/Recv/Attach/Detach/
// Close, so engines can assume single-threaded entry along each of those
// paths except where explicitly noted. The exceptions are the internal
// goroutines an engine spawns (FairQueue forwarders, pub/xpub per-peer
// readers, REQ sticky forwarders) — those must use the engine's own
// mutex to coordinate with the caller-driven methods.
//
// LoadBalancer, FairQueue, and SubTree are themselves goroutine-safe so
// tests and future patterns can use them outside the Socket mutex.
//
// # Ownership model
//
// Each engine owns the per-peer bookkeeping for every pipe it has been
// handed: subscription trees, routing identity tables, sticky peer state,
// FSM bits, reply envelopes. It does not own the pipes themselves — the
// session layer creates and closes those, and calls AttachPipe/DetachPipe
// to let the engine react. The Socket owns the engine and is responsible
// for calling Close exactly once.
package socket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// --------------------------------------------------------------------------
// LoadBalancer
// --------------------------------------------------------------------------

// Pollable is the optional interface a SocketEngine implements to expose
// non-consuming readiness signals to the public Poller API. The two
// channels are level-armed: a chan with a buffered value (or that is
// closed) means "ready right now"; a chan that blocks means "not ready".
// Either channel may be nil — Poller skips nil channels. Engines that
// have no recv direction (PUSH) return nil from ReadyRecv; engines with
// no send direction (PULL, SUB, XSUB) return nil from ReadyWrite.
//
// PollableRecv/PollableSend are separate so engines that support only
// one direction can omit the other entirely. The combined Pollable
// interface is what the Poller asserts against; the per-direction
// methods are convenience for engines that compose primitives.
type Pollable interface {
	// ReadyRecv returns a channel that is armed when at least one
	// message is buffered and immediately available to Recv. Reads
	// from the channel do not consume the message; the engine
	// re-arms the signal as long as data remains. Returns nil for
	// send-only engines.
	ReadyRecv() <-chan struct{}
	// ReadyWrite returns a channel that is armed when at least one
	// peer is attached and a Send would not block on the peer-set
	// being empty. (HWM-induced blocking is not surfaced; this
	// mirrors libzmq's poll semantics where POLLOUT means "a peer
	// exists," not "the next byte is guaranteed to land instantly.")
	// Returns nil for recv-only engines.
	ReadyWrite() <-chan struct{}
	// CanRecvNow is a non-consuming level check: true iff a Recv
	// would not block on inbound queue emptiness right now. Always
	// false for send-only engines.
	CanRecvNow() bool
	// CanSendNow is a non-consuming level check: true iff a Send
	// would not block on peer-set emptiness right now (HWM blocking
	// is not surfaced; see ReadyWrite). Always false for recv-only.
	CanSendNow() bool
}

// LoadBalancer is a round-robin outbound dispatcher across a dynamic set of
// iface.Pipe values. It is goroutine-safe: Attach, Detach, Send, and Close
// may be called concurrently from any goroutine.
//
// Send delivers the message to the peer at the current round-robin cursor
// and advances the cursor on success. If that peer's pipe is at HWM, Send
// blocks until the peer drains, ctx cancels, or the LoadBalancer is
// closed. This matches libzmq's lb_t semantics (strict cursor; if the
// chosen peer is full, the sender backpressures rather than rotating).
//
// If a peer disappears mid-send (its pipe closed), the cursor advances
// past it and Send retries with the next peer. This keeps load-balancing
// liveness in the presence of churn.
type LoadBalancer struct {
	mu      sync.Mutex
	pipes   []iface.Pipe
	cursor  int
	notify  chan struct{} // signalled by Attach/Detach
	readyW  chan struct{} // level-armed while len(pipes) > 0; Poller-facing
	closeCh chan struct{}
	closed  bool
}

type nonBlockingSender interface {
	TrySend(*msg.Msg) bool
}

// NewLoadBalancer returns an empty LoadBalancer.
func NewLoadBalancer() *LoadBalancer {
	return &LoadBalancer{
		notify:  make(chan struct{}, 1),
		readyW:  make(chan struct{}, 1),
		closeCh: make(chan struct{}),
	}
}

// ReadyWrite returns a channel that is non-blocking-readable whenever
// the LoadBalancer has at least one attached peer pipe. The Poller
// uses this as the PollOut readiness signal. Reads from the channel
// do not change LoadBalancer state; if peers remain attached, the
// next attach/detach cycle re-arms the channel via signalReadyLocked.
// To support polling that runs faster than peer churn, the channel is
// also re-armed by HasPeers callers checking the level state.
func (lb *LoadBalancer) ReadyWrite() <-chan struct{} {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	// Re-arm if currently ready (level-trigger): the chan may have
	// been drained by a previous Poll call.
	if len(lb.pipes) > 0 && !lb.closed {
		select {
		case lb.readyW <- struct{}{}:
		default:
		}
	}
	return lb.readyW
}

// HasPeers reports whether at least one peer is attached. Used by the
// Poller for the immediate non-blocking PollOut check.
func (lb *LoadBalancer) HasPeers() bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return len(lb.pipes) > 0 && !lb.closed
}

// signalReadyWriteLocked re-arms the write-ready channel when peers
// are present. Caller must hold lb.mu.
func (lb *LoadBalancer) signalReadyWriteLocked() {
	if lb.readyW == nil {
		return
	}
	if len(lb.pipes) > 0 && !lb.closed {
		select {
		case lb.readyW <- struct{}{}:
		default:
		}
	}
}

// Attach registers p with the dispatcher. Idempotent: re-attaching an
// already-tracked pipe is a no-op. Returns zmqerr.ErrSocketClosed (wrapped)
// after Close.
func (lb *LoadBalancer) Attach(p iface.Pipe) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.closed {
		return fmt.Errorf("loadbalancer attach: %w", zmqerr.ErrSocketClosed)
	}
	for _, existing := range lb.pipes {
		if existing == p {
			return nil
		}
	}
	lb.pipes = append(lb.pipes, p)
	lb.signalLocked()
	lb.signalReadyWriteLocked()
	return nil
}

// Detach removes p from the dispatcher. No-op if p was never attached.
// Adjusts the cursor so round-robin continues sensibly.
func (lb *LoadBalancer) Detach(p iface.Pipe) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	for i, existing := range lb.pipes {
		if existing == p {
			lb.pipes = append(lb.pipes[:i], lb.pipes[i+1:]...)
			if len(lb.pipes) == 0 {
				lb.cursor = 0
			} else {
				// Keep cursor pointing at the next peer in RR order. If we
				// removed an element before cursor, cursor must shift left.
				if i < lb.cursor {
					lb.cursor--
				}
				lb.cursor %= len(lb.pipes)
			}
			lb.signalLocked()
			lb.signalReadyWriteLocked()
			return nil
		}
	}
	return nil
}

// Send dispatches m to the peer at the current cursor and advances on
// success. Blocks if that peer's pipe is at HWM until the peer drains,
// ctx cancels, or Close is called.
//
// Returns zmqerr.ErrSocketClosed (wrapped) after Close, ctx.Err() on
// cancellation, or the underlying Pipe.Send error.
func (lb *LoadBalancer) Send(ctx context.Context, m *msg.Msg) error {
	for {
		lb.mu.Lock()
		if lb.closed {
			lb.mu.Unlock()
			return fmt.Errorf("loadbalancer send: %w", zmqerr.ErrSocketClosed)
		}
		if len(lb.pipes) == 0 {
			notify := lb.notify
			closeCh := lb.closeCh
			lb.mu.Unlock()
			select {
			case <-notify:
			case <-closeCh:
				return fmt.Errorf("loadbalancer send: %w", zmqerr.ErrSocketClosed)
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		idx := lb.cursor % len(lb.pipes)
		p := lb.pipes[idx]
		closeCh := lb.closeCh
		notify := lb.notify
		// Drain any stale notification that fired while we were idle —
		// otherwise it would race with a successful Send and force a
		// spurious retry that re-delivers the same message.
		select {
		case <-notify:
		default:
		}
		lb.mu.Unlock()

		if nb, ok := p.(nonBlockingSender); ok && nb.TrySend(m) {
			lb.mu.Lock()
			if idx < len(lb.pipes) && lb.pipes[idx] == p {
				lb.cursor = (idx + 1) % len(lb.pipes)
			}
			lb.mu.Unlock()
			return nil
		}

		// Race the blocking Send against close and the notify (peer-set
		// change). We need cancellation, so use a child context that we
		// cancel when close fires or notify fires.
		err := lb.sendOne(ctx, p, m, closeCh, notify)
		if err == nil {
			lb.mu.Lock()
			// Advance only if p is still at idx — defends against Detach
			// racing with success.
			if idx < len(lb.pipes) && lb.pipes[idx] == p {
				lb.cursor = (idx + 1) % len(lb.pipes)
			}
			lb.mu.Unlock()
			return nil
		}
		if errors.Is(err, errRetry) {
			continue
		}
		if errors.Is(err, io.ErrClosedPipe) {
			// Peer pipe closed mid-send — drop it and retry.
			_ = lb.Detach(p)
			continue
		}
		return err
	}
}

var errRetry = errors.New("loadbalancer: retry")

// sendOne performs a single blocking Pipe.Send racing against close,
// peer-set change, and parent ctx. If a non-success wakeup wins the race,
// we cancel the child context and then await the goroutine's actual
// outcome — if Send already succeeded before cancel, we honor that
// success rather than retrying (which would re-deliver the message).
func (lb *LoadBalancer) sendOne(parent context.Context, p iface.Pipe, m *msg.Msg, closeCh, notify <-chan struct{}) error {
	childCtx, cancel := context.WithCancel(parent)
	defer cancel()

	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		done <- result{err: p.Send(childCtx, m)}
	}()

	// finalize awaits the goroutine, distinguishing "succeeded before
	// cancel" from "cancelled cleanly" from "real error."
	finalize := func(fallback error) error {
		r := <-done
		if r.err == nil {
			return nil
		}
		if errors.Is(r.err, context.Canceled) && parent.Err() == nil {
			// Cancelled by us — return the fallback (retry / closed).
			return fallback
		}
		// If parent ctx was cancelled, parent.Err() trumps.
		if parent.Err() != nil {
			return parent.Err()
		}
		return r.err
	}

	select {
	case r := <-done:
		// Direct outcome.
		if r.err == nil {
			return nil
		}
		return r.err
	case <-closeCh:
		cancel()
		return finalize(fmt.Errorf("loadbalancer send: %w", zmqerr.ErrSocketClosed))
	case <-notify:
		cancel()
		return finalize(errRetry)
	case <-parent.Done():
		cancel()
		return finalize(parent.Err())
	}
}

// signalLocked wakes any waiter on the notify channel. Caller must hold
// lb.mu. Non-blocking; capacity-1 channel coalesces consecutive signals.
func (lb *LoadBalancer) signalLocked() {
	select {
	case lb.notify <- struct{}{}:
	default:
	}
}

// Close releases the LoadBalancer. Subsequent Attach returns
// zmqerr.ErrSocketClosed; pending Send unblocks with the same. Idempotent.
func (lb *LoadBalancer) Close() error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.closed {
		return nil
	}
	lb.closed = true
	close(lb.closeCh)
	lb.signalLocked()
	return nil
}

// --------------------------------------------------------------------------
// FairQueue
// --------------------------------------------------------------------------

// FairQueue is a fair-queued inbound multiplexer over a dynamic set of
// iface.Pipe values. Each attached pipe gets a forwarder goroutine that
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
	readyR  chan struct{} // level-armed while inbound holds an item
	pipes   map[iface.Pipe]*fairForwarder
	wg      sync.WaitGroup
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
	p iface.Pipe
}

// NewFairQueue returns an empty FairQueue.
func NewFairQueue() *FairQueue {
	return &FairQueue{
		closeCh: make(chan struct{}),
		// Capacity 1 keeps a small handoff buffer so the forwarder isn't
		// blocked the moment the receiver pulls. Larger buffers would
		// undermine HWM, since HWM lives in the Pipe itself.
		inbound: make(chan fairItem, 1),
		readyR:  make(chan struct{}, 1),
		pipes:   make(map[iface.Pipe]*fairForwarder),
	}
}

// ReadyRecv returns a channel that is non-blocking-readable whenever
// the FairQueue has at least one buffered inbound message. The
// Poller uses this as the PollIn readiness signal. Reads from the
// channel do not consume the message; the engine re-arms the signal
// whenever items remain. Callers should invoke HasPending alongside
// to handle the case where readyR was drained by a prior Poll but
// the buffered item was not yet consumed.
func (fq *FairQueue) ReadyRecv() <-chan struct{} {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	// Re-arm if data is still pending in inbound but readyR has
	// been drained by a previous Poll (level-trigger semantics).
	if len(fq.inbound) > 0 && !fq.closed {
		select {
		case fq.readyR <- struct{}{}:
		default:
		}
	}
	return fq.readyR
}

// HasPending reports whether the FairQueue has at least one inbound
// message buffered and ready to deliver. Used by the Poller for the
// immediate non-blocking PollIn check.
func (fq *FairQueue) HasPending() bool {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	return !fq.closed && len(fq.inbound) > 0
}

// Attach registers p and starts its forwarder goroutine. Idempotent.
// Returns zmqerr.ErrSocketClosed (wrapped) after Close.
func (fq *FairQueue) Attach(p iface.Pipe) error {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	if fq.closed {
		return fmt.Errorf("fairqueue attach: %w", zmqerr.ErrSocketClosed)
	}
	if _, ok := fq.pipes[p]; ok {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	fwd := &fairForwarder{ctx: ctx, cancel: cancel}
	fq.pipes[p] = fwd
	fq.wg.Add(1)
	go fq.forward(p, fwd)
	return nil
}

// Detach removes p and signals its forwarder to stop promptly.
func (fq *FairQueue) Detach(p iface.Pipe) error {
	fq.mu.Lock()
	fwd, ok := fq.pipes[p]
	if !ok {
		fq.mu.Unlock()
		return nil
	}
	delete(fq.pipes, p)
	fq.mu.Unlock()
	select {
	case <-p.Closed():
		// Let a closed pipe drain any already-buffered messages to EOF.
		return nil
	default:
	}
	fwd.cancel()
	return nil
}

// Recv blocks until a message arrives from any attached pipe, ctx
// cancels, or Close is called. Returns zmqerr.ErrSocketClosed (wrapped)
// on close. The source pipe is discarded; callers that need it should
// use RecvWithSource.
func (fq *FairQueue) Recv(ctx context.Context) (*msg.Msg, error) {
	m, _, err := fq.RecvWithSource(ctx)
	return m, err
}

// RecvWithSource blocks like Recv and additionally reports the pipe from
// which the message was received. ROUTER uses this to stamp the leading
// peer-identity frame onto each inbound message; REP uses it to capture
// the delimiter envelope for the next Send. A closed pipe yields
// io.EOF inside the forwarder and is never surfaced here — the forwarder
// simply exits.
func (fq *FairQueue) RecvWithSource(ctx context.Context) (*msg.Msg, iface.Pipe, error) {
	select {
	case it := <-fq.inbound:
		return it.m, it.p, nil
	case <-fq.closeCh:
		// Drain any in-flight message that beat the close signal.
		select {
		case it := <-fq.inbound:
			return it.m, it.p, nil
		default:
			return nil, nil, fmt.Errorf("fairqueue recv: %w", zmqerr.ErrSocketClosed)
		}
	case <-ctx.Done():
		return nil, nil, ctx.Err()
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
// cancelled, or the
// FairQueue closes.
func (fq *FairQueue) forward(p iface.Pipe, fwd *fairForwarder) {
	defer fq.wg.Done()
	defer func() {
		fq.mu.Lock()
		if cur, ok := fq.pipes[p]; ok && cur == fwd {
			delete(fq.pipes, p)
		}
		fq.mu.Unlock()
	}()
	for {
		m, err := p.Recv(fwd.ctx)
		if err != nil {
			// ctx cancelled, pipe EOF, or other terminal error —
			// forwarder is done.
			return
		}
		// Hand off the message; race against stop and close so we never
		// block forever holding a message no one will consume.
		select {
		case fq.inbound <- fairItem{m: m, p: p}:
			// Arm the Poller-facing read-ready signal. Non-blocking;
			// cap-1 chan coalesces multiple producers, and the Poller
			// re-arms on the next call if items remain.
			select {
			case fq.readyR <- struct{}{}:
			default:
			}
		case <-fwd.ctx.Done():
			return
		case <-fq.closeCh:
			return
		}
	}
}

// --------------------------------------------------------------------------
// Simple engine combinators
// --------------------------------------------------------------------------

type sendOnlyEngine struct {
	name string
	lb   *LoadBalancer
}

func newSendOnlyEngine(name string) iface.SocketEngine {
	return &sendOnlyEngine{name: name, lb: NewLoadBalancer()}
}

func (e *sendOnlyEngine) AttachPipe(p iface.Pipe) error { return e.lb.Attach(p) }
func (e *sendOnlyEngine) DetachPipe(p iface.Pipe) error { return e.lb.Detach(p) }
func (e *sendOnlyEngine) Send(ctx context.Context, m *msg.Msg) error {
	return e.lb.Send(ctx, m)
}
func (e *sendOnlyEngine) Recv(context.Context) (*msg.Msg, error) {
	return nil, fmt.Errorf("%s recv: %w", e.name, zmqerr.ErrFSMViolation)
}
func (e *sendOnlyEngine) Close() error                { return e.lb.Close() }
func (e *sendOnlyEngine) ReadyRecv() <-chan struct{}  { return nil }
func (e *sendOnlyEngine) ReadyWrite() <-chan struct{} { return e.lb.ReadyWrite() }
func (e *sendOnlyEngine) CanRecvNow() bool            { return false }
func (e *sendOnlyEngine) CanSendNow() bool            { return e.lb.HasPeers() }

type recvOnlyEngine struct {
	name string
	fq   *FairQueue
}

func newRecvOnlyEngine(name string) iface.SocketEngine {
	return &recvOnlyEngine{name: name, fq: NewFairQueue()}
}

func (e *recvOnlyEngine) AttachPipe(p iface.Pipe) error { return e.fq.Attach(p) }
func (e *recvOnlyEngine) DetachPipe(p iface.Pipe) error { return e.fq.Detach(p) }
func (e *recvOnlyEngine) Send(context.Context, *msg.Msg) error {
	return fmt.Errorf("%s send: %w", e.name, zmqerr.ErrFSMViolation)
}
func (e *recvOnlyEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	return e.fq.Recv(ctx)
}
func (e *recvOnlyEngine) Close() error                { return e.fq.Close() }
func (e *recvOnlyEngine) ReadyRecv() <-chan struct{}  { return e.fq.ReadyRecv() }
func (e *recvOnlyEngine) ReadyWrite() <-chan struct{} { return nil }
func (e *recvOnlyEngine) CanRecvNow() bool            { return e.fq.HasPending() }
func (e *recvOnlyEngine) CanSendNow() bool            { return false }

type queuePairEngine struct {
	lb *LoadBalancer
	fq *FairQueue
}

func newQueuePairEngine() iface.SocketEngine {
	return &queuePairEngine{lb: NewLoadBalancer(), fq: NewFairQueue()}
}

func (e *queuePairEngine) AttachPipe(p iface.Pipe) error {
	if err := e.lb.Attach(p); err != nil {
		return err
	}
	if err := e.fq.Attach(p); err != nil {
		_ = e.lb.Detach(p)
		return err
	}
	return nil
}

func (e *queuePairEngine) DetachPipe(p iface.Pipe) error {
	_ = e.lb.Detach(p)
	_ = e.fq.Detach(p)
	return nil
}

func (e *queuePairEngine) Send(ctx context.Context, m *msg.Msg) error {
	return e.lb.Send(ctx, m)
}

func (e *queuePairEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	return e.fq.Recv(ctx)
}

func (e *queuePairEngine) Close() error {
	_ = e.lb.Close()
	_ = e.fq.Close()
	return nil
}

func (e *queuePairEngine) ReadyRecv() <-chan struct{}  { return e.fq.ReadyRecv() }
func (e *queuePairEngine) ReadyWrite() <-chan struct{} { return e.lb.ReadyWrite() }
func (e *queuePairEngine) CanRecvNow() bool            { return e.fq.HasPending() }
func (e *queuePairEngine) CanSendNow() bool            { return e.lb.HasPeers() }
