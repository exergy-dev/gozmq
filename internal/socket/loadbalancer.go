package socket

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// LoadBalancer is a round-robin outbound dispatcher across a dynamic set of
// iface.Pipe values. It is goroutine-safe: Attach, Detach, Send, and Close
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
	// replaced with a fresh one whenever the peer set changes, so every
	// Send parked on the old generation wakes and re-reads the set. A
	// cap-1 token channel was tried first and lost wakeups: a mid-loop
	// drain could swallow the token destined for another parked sender.
	notifyCh chan struct{}
	readyW   readyGate // level-armed while ring.len() > 0; Poller-facing
	closeCh  chan struct{}
	closed   bool

	// single is an atomically-swapped snapshot of the sole attached peer.
	// When exactly one pipe is attached it holds that peer; Send reads it
	// without holding mu, inlines the blocking select, and skips both the
	// peer-pick lock and the cursor-advance lock. It is nil whenever 0 or
	// 2+ pipes are attached (forcing the locked round-robin path) and is
	// cleared atomically on Detach/Close so a Send can never use a stale
	// pipe. Mirrors FairQueue.direct on the recv side. Round-robin
	// fairness with a single peer is trivial, so no cursor math is needed.
	single atomic.Pointer[singleSnapshot]

	// sent is the count of messages successfully dispatched. Incremented
	// after the pipe accepts the message. Send-side errors do NOT
	// increment this — they are surfaced to the caller.
	sent atomic.Uint64
}

// singleSnapshot holds the lock-free send state for the single-peer
// LoadBalancer fast path. A nil *singleSnapshot means the locked
// round-robin path must be used (0 or 2+ peers). notify is the
// generation channel current when the snapshot was taken: any later
// peer-set change closes it, invalidating the snapshot for parked
// senders.
type singleSnapshot struct {
	pipe   chanPipe
	notify <-chan struct{}
}

// Sent reports the number of messages this LoadBalancer has successfully
// dispatched. Safe to call concurrently with Send.
func (lb *LoadBalancer) Sent() uint64 { return lb.sent.Load() }

// NewLoadBalancer returns an empty LoadBalancer.
func NewLoadBalancer() *LoadBalancer {
	return &LoadBalancer{
		notifyCh: make(chan struct{}),
		readyW:   newReadyGate(),
		closeCh:  make(chan struct{}),
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

// refreshSingleLocked recomputes the single-peer send snapshot. Caller
// must hold lb.mu, and must call signalLocked FIRST when the peer set
// changed so the snapshot captures the fresh notify generation. When
// exactly one pipe is attached (and not closed) the snapshot is
// populated so Send can take the lock-free fast path; otherwise it is
// cleared, forcing the locked round-robin path.
func (lb *LoadBalancer) refreshSingleLocked() {
	if lb.closed || lb.ring.len() != 1 {
		lb.single.Store(nil)
		return
	}
	lb.single.Store(&singleSnapshot{
		pipe:   asChanPipe(lb.ring.pipes[0]),
		notify: lb.notifyCh,
	})
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
func (lb *LoadBalancer) Attach(p iface.Pipe) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.closed {
		return fmt.Errorf("loadbalancer attach: %w", zmqerr.ErrSocketClosed)
	}
	if !lb.ring.add(p) {
		return nil
	}
	lb.signalLocked()
	lb.refreshSingleLocked()
	lb.signalReadyWriteLocked()
	return nil
}

// Detach removes p from the dispatcher. No-op if p was never attached.
// Adjusts the cursor so round-robin continues sensibly.
func (lb *LoadBalancer) Detach(p iface.Pipe) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.ring.remove(p) {
		lb.signalLocked()
		lb.refreshSingleLocked()
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
	// Single-peer fast path: when exactly one peer is attached we read it
	// from an atomic snapshot and inline the blocking select, skipping BOTH
	// the peer-pick lock and the cursor-advance lock. The closeCh arm
	// preserves the Close-unblocks-a-blocked-Send guarantee; the notify arm
	// wakes us if the peer set changes (Attach/Detach) so we re-read the
	// current set via the locked path rather than blocking on a stale pipe.
	if s := lb.single.Load(); s != nil {
		// Try the non-blocking fast send first to avoid arming a select at
		// all when the pipe has room.
		if s.pipe.TrySend(m) {
			lb.sent.Add(1)
			return nil
		}
		select {
		case s.pipe.SendCh() <- m:
			lb.sent.Add(1)
			return nil
		case <-lb.closeCh:
			return fmt.Errorf("loadbalancer send: %w", zmqerr.ErrSocketClosed)
		case <-s.notify:
			// Peer set changed. notify is closed (broadcast), so every
			// parked sender wakes; fall through to the locked round-robin
			// path which re-reads the current peer set under the mutex.
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for {
		lb.mu.Lock()
		if lb.closed {
			lb.mu.Unlock()
			return fmt.Errorf("loadbalancer send: %w", zmqerr.ErrSocketClosed)
		}
		if lb.ring.len() == 0 {
			notify := lb.notifyCh
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

		// libzmq lb_t parity: try each peer once, non-blocking, starting
		// at the cursor. The first peer with room wins; full (or closed)
		// peers are rotated past instead of head-of-line blocking the
		// ring. TrySend is non-blocking and lock-free, so holding mu for
		// the rotation is cheap.
		for i := lb.ring.len(); i > 0; i-- {
			idx, p := lb.ring.current()
			lb.ring.advancePast(idx, p)
			if asChanPipe(p).TrySend(m) {
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
		idx, p := lb.ring.current()
		notify := lb.notifyCh
		closeCh := lb.closeCh
		lb.mu.Unlock()

		select {
		case asChanPipe(p).SendCh() <- m:
			lb.mu.Lock()
			lb.ring.advancePast(idx, p)
			lb.mu.Unlock()
			lb.sent.Add(1)
			return nil
		case <-p.Closed():
			// Peer pipe closed mid-send — drop it and retry.
			_ = lb.Detach(p)
			continue
		case <-closeCh:
			return fmt.Errorf("loadbalancer send: %w", zmqerr.ErrSocketClosed)
		case <-notify:
			// Peer set changed; re-rotate over the new set.
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// signalLocked broadcasts a peer-set change by closing the current
// notify generation and installing a fresh one. Caller must hold lb.mu.
// Every parked Send (fast path or locked path) holds some generation and
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
	lb.single.Store(nil)
	close(lb.closeCh)
	lb.signalLocked()
	return nil
}
