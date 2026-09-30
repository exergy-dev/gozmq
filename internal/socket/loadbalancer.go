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

// LoadBalancer is a round-robin outbound dispatcher across a dynamic set of
// *pipe.End values. It is goroutine-safe: Attach, Detach, Send, and Close
// may be called concurrently from any goroutine.
//
// Send tries the peer at the round-robin cursor first and advances the
// cursor on success. A peer whose pipe is at HWM is skipped: Send rotates
// through the remaining peers and only blocks when EVERY attached peer is
// full (until one drains, the peer set changes, ctx cancels, or the
// LoadBalancer closes). This matches libzmq's lb_t semantics, where a
// full pipe is deactivated and the sender moves on, backpressuring only
// when no active pipe remains.
//
// If a peer disappears mid-send (its pipe closed), the cursor advances
// past it and Send retries with the next peer. This keeps load-balancing
// liveness in the presence of churn.
type LoadBalancer struct {
	mu   sync.Mutex
	ring pipeRing
	// notifyCh is a generation channel: it is closed (broadcast) and
	// replaced with a fresh one whenever the peer set changes and on
	// Close, so every Send parked on the old generation wakes and re-reads
	// the set (or sees closed). A
	// cap-1 token channel was tried first and lost wakeups: a mid-loop
	// drain could swallow the token destined for another parked sender.
	notifyCh chan struct{}
	readyW   readyGate // level-armed while ring.len() > 0; Poller-facing
	closed   bool

	// There is deliberately no lock-free single-peer fast path (unlike
	// FairQueue's direct mode): measured against this locked path it was
	// worth under 9% on inproc PUSH/PULL throughput and nothing on the
	// other perf/ benchmarks, which does not pay for a second send path.

	// sent is the count of messages successfully dispatched. Incremented
	// after the pipe accepts the message. Send-side errors do NOT
	// increment this — they are surfaced to the caller.
	sent atomic.Uint64
}

// Sent reports the number of messages this LoadBalancer has successfully
// dispatched. Safe to call concurrently with Send.
func (lb *LoadBalancer) Sent() uint64 { return lb.sent.Load() }

// NewLoadBalancer returns an empty LoadBalancer.
func NewLoadBalancer() *LoadBalancer {
	return &LoadBalancer{
		notifyCh: make(chan struct{}),
		readyW:   newReadyGate(),
	}
}

// ReadyWrite returns a channel that is non-blocking-readable whenever
// the LoadBalancer has at least one attached peer pipe. The Poller
// uses this as the PollOut readiness signal. Reads from the channel
// do not change LoadBalancer state; the gate is level-armed — each
// ReadyWrite call re-arms it while peers remain attached.
func (lb *LoadBalancer) ReadyWrite() <-chan struct{} {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.readyW.signal(lb.ring.len() > 0 && !lb.closed)
}

// HasPeers reports whether at least one peer is attached. Used by the
// Poller for the immediate non-blocking PollOut check.
func (lb *LoadBalancer) HasPeers() bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.ring.len() > 0 && !lb.closed
}

// signalReadyWriteLocked re-arms the write-ready gate when peers are
// present. Caller must hold lb.mu.
func (lb *LoadBalancer) signalReadyWriteLocked() {
	if lb.ring.len() > 0 && !lb.closed {
		lb.readyW.arm()
	}
}

// Attach registers p with the dispatcher. Idempotent: re-attaching an
// already-tracked pipe is a no-op. Returns zmqerr.ErrSocketClosed (wrapped)
// after Close.
func (lb *LoadBalancer) Attach(p *pipe.End) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.closed {
		return fmt.Errorf("loadbalancer attach: %w", zmqerr.ErrSocketClosed)
	}
	if !lb.ring.add(p) {
		return nil
	}
	lb.signalLocked()
	lb.signalReadyWriteLocked()
	return nil
}

// Detach removes p from the dispatcher. No-op if p was never attached.
// Adjusts the cursor so round-robin continues sensibly.
func (lb *LoadBalancer) Detach(p *pipe.End) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.ring.remove(p) {
		lb.signalLocked()
		lb.signalReadyWriteLocked()
	}
	return nil
}

// Send dispatches m to the first peer with room, starting at the
// round-robin cursor, and advances the cursor past the winner. When every
// attached peer is at HWM, Send blocks on the cursor peer until it drains,
// the peer set changes, ctx cancels, or Close is called.
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
		if lb.ring.len() == 0 {
			notify := lb.notifyCh
			lb.mu.Unlock()
			select {
			case <-notify: // attach or Close; re-check under the lock
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		// libzmq lb_t parity: try each peer once, non-blocking, starting
		// at the cursor. The first peer with room wins; full (or closed)
		// peers are rotated past instead of head-of-line blocking the
		// ring. TrySend is non-blocking and lock-free, so holding mu for
		// the rotation is cheap.
		for i := lb.ring.len(); i > 0; i-- {
			idx, p := lb.ring.current()
			lb.ring.advancePast(idx, p)
			if p.TrySend(m) {
				lb.mu.Unlock()
				lb.sent.Add(1)
				return nil
			}
		}

		// Every peer is at HWM (the rotation left the cursor where it
		// started). Block on the cursor peer until it drains, racing
		// close, peer-set change, and the caller's ctx. libzmq blocks
		// until ANY pipe reactivates; blocking on one peer is a close
		// approximation since a drain elsewhere usually accompanies
		// peer-set churn or further Sends re-rotating.
		// notify is closed by a peer-set change and by Close; either way
		// the loop re-checks under the lock.
		idx, p := lb.ring.current()
		notify := lb.notifyCh
		lb.mu.Unlock()

		switch err := p.SendOr(ctx, m, notify); {
		case err == nil:
			lb.mu.Lock()
			lb.ring.advancePast(idx, p)
			lb.mu.Unlock()
			lb.sent.Add(1)
			return nil
		case errors.Is(err, io.ErrClosedPipe):
			// Peer pipe closed mid-send — drop it and retry.
			_ = lb.Detach(p)
		case errors.Is(err, pipe.ErrAborted):
			// Peer set changed or Close; re-rotate over the new set.
		default:
			return err
		}
	}
}

// signalLocked broadcasts a peer-set change by closing the current
// notify generation and installing a fresh one. Caller must hold lb.mu.
// Every parked Send holds some generation and
// wakes when it closes; senders that arrive later grab the new, open
// generation and are unaffected.
func (lb *LoadBalancer) signalLocked() {
	close(lb.notifyCh)
	lb.notifyCh = make(chan struct{})
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
	lb.signalLocked()
	return nil
}
