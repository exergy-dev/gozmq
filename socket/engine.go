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

	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
)

// --------------------------------------------------------------------------
// LoadBalancer
// --------------------------------------------------------------------------

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
	closeCh chan struct{}
	closed  bool
}

// NewLoadBalancer returns an empty LoadBalancer.
func NewLoadBalancer() *LoadBalancer {
	return &LoadBalancer{
		notify:  make(chan struct{}, 1),
		closeCh: make(chan struct{}),
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
	pipes   map[iface.Pipe]chan struct{} // pipe → its forwarder's stop channel
	wg      sync.WaitGroup
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
		pipes:   make(map[iface.Pipe]chan struct{}),
	}
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
	stop := make(chan struct{})
	fq.pipes[p] = stop
	fq.wg.Add(1)
	go fq.forward(p, stop)
	return nil
}

// Detach removes p and signals its forwarder to stop. The forwarder may
// still be parked in Pipe.Recv at the time of Detach; it will exit when
// the pipe yields a message (which is dropped) or when the pipe closes.
// To guarantee prompt exit in tests, Close the FairQueue.
func (fq *FairQueue) Detach(p iface.Pipe) error {
	fq.mu.Lock()
	stop, ok := fq.pipes[p]
	if !ok {
		fq.mu.Unlock()
		return nil
	}
	delete(fq.pipes, p)
	fq.mu.Unlock()
	close(stop)
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
	stops := make([]chan struct{}, 0, len(fq.pipes))
	for _, s := range fq.pipes {
		stops = append(stops, s)
	}
	fq.pipes = nil
	fq.mu.Unlock()
	for _, s := range stops {
		select {
		case <-s:
			// already closed (raced with Detach)
		default:
			close(s)
		}
	}
	fq.wg.Wait()
	return nil
}

// forward pumps p.Recv into fq.inbound until p closes, stop fires, or the
// FairQueue closes.
func (fq *FairQueue) forward(p iface.Pipe, stop <-chan struct{}) {
	defer fq.wg.Done()
	for {
		// Use a context that cancels when stop or close fires so
		// Pipe.Recv unblocks promptly. The unblock-watcher goroutine
		// terminates either when its watched channel fires or when Recv
		// returns and we close watcherDone.
		ctx, cancel := context.WithCancel(context.Background())
		watcherDone := make(chan struct{})
		var watcherWg sync.WaitGroup
		watcherWg.Add(1)
		go func() {
			defer watcherWg.Done()
			select {
			case <-stop:
				cancel()
			case <-fq.closeCh:
				cancel()
			case <-watcherDone:
			}
		}()
		m, err := p.Recv(ctx)
		close(watcherDone)
		watcherWg.Wait()
		cancel()
		if err != nil {
			// ctx cancelled (stop/close), pipe EOF, or other terminal
			// error — forwarder is done.
			return
		}
		// Hand off the message; race against stop and close so we never
		// block forever holding a message no one will consume.
		select {
		case fq.inbound <- fairItem{m: m, p: p}:
		case <-stop:
			return
		case <-fq.closeCh:
			return
		}
	}
}
