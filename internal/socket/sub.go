package socket

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
	"github.com/exergy-dev/gozmq/msg"
)

// SubEngine implements Engine for the SUB socket type and
// also exposes subscription control to the public gozmq layer.
//
// SUB fair-queues inbound messages across all attached peer pipes using
// a shared FairQueue and applies the local subscription filter after
// Recv: a message whose topic frame (first frame) does not match any
// subscription is discarded and the engine loops for the next one.
//
// The local subscription list is refcounted: duplicate Subscribe calls
// require a matching number of Unsubscribe calls before the prefix is
// actually removed from the filter. Every net change (first Subscribe
// for a prefix, final Unsubscribe) is forwarded as a ZMTP SUBSCRIBE or
// CANCEL command to every attached peer via a synthetic single-frame
// message on the pipe's outbound side (leading byte 0x01 / 0x00 +
// prefix). Forwarding is best-effort — a brief per-peer deadline avoids
// blocking the subscriber against a congested PUB. New peers attached
// after Subscribe replay the current subscription list.
//
// Send on a plain SUB returns zmqerr.ErrFSMViolation (wrapped); XSUB
// (see xsub.go) overrides Send to treat the message as a subscription
// command from the application.
//
// *SubEngine implements Engine; the public gozmq package type-asserts it
// back to reach Subscribe and Unsubscribe, which stay out of the Engine
// interface.
type SubEngine struct {
	mu     sync.Mutex
	closed bool

	// peers holds every currently-attached pipe. We need to remember
	// them so new subscriptions can be forwarded to every existing peer,
	// and so new peers (AttachPipe) can be replayed every pre-existing
	// subscription.
	peers map[*pipe.End]struct{}

	// tree is the refcounted local filter (duplicate Subscribe calls are
	// legal and need matching Unsubscribes), matched on Recv.
	tree *SubTree

	// fq fair-queues Recv across all attached pipes. Matching is done
	// AFTER FairQueue returns — messages whose topic frame doesn't match
	// are dropped by Recv and we go around again.
	fq      *FairQueue
	surface bool // XSUB surfaces subscription commands sent by the app; SUB does not

	gates

	// recv counts messages SUB/XSUB returned to the caller (matches the
	// local subscription filter). Filter-mismatch drops are NOT counted
	// as "dropped" in metrics — they are routine SUB behavior, not a
	// backpressure/close event. counters.sent counts XSUB subscription-
	// control messages accepted from the application (zero for plain SUB,
	// which returns ErrFSMViolation on Send).
	counters
}

// NewSubEngine returns a new SUB socket engine and a handle for driving
// subscriptions.
func NewSubEngine() *SubEngine { return newSubEngineBase(false) }

func newSubEngineBase(surface bool) *SubEngine {
	g := newGates()
	if surface {
		// XSUB can always take a subscription command, so its out-gate
		// starts armed; a Poller hands the token on after each wake.
		g.armOut()
	}
	return &SubEngine{
		peers:   make(map[*pipe.End]struct{}),
		tree:    NewSubTree(),
		fq:      NewFairQueue(g.in),
		surface: surface,
		gates:   g,
	}
}

// CanRecvNow reports the FairQueue's level. A queued message the filter
// then drops is at worst a spurious wake (PUB peers filter upstream, so it
// is rare).
func (e *SubEngine) CanRecvNow() bool { return e.fq.CanRecv() }

// CanSendNow reports whether XSUB can take a subscription command; SUB
// never can.
func (e *SubEngine) CanSendNow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.surface && !e.closed
}

// AttachPipe registers p. Every current subscription is replayed to the
// new peer as a synthetic SUBSCRIBE message so the peer's PUB engine
// (or XPUB) sees the same filter state an existing peer does.
func (e *SubEngine) AttachPipe(p *pipe.End) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("sub attach: %w", zmqerr.ErrSocketClosed)
	}
	if _, ok := e.peers[p]; ok {
		e.mu.Unlock()
		return nil
	}
	e.peers[p] = struct{}{}
	// Snapshot the subscriptions under the lock so a concurrent Subscribe
	// cannot race the replay.
	var snapshot [][]byte
	e.tree.Walk(func(prefix []byte, _ int) { snapshot = append(snapshot, slices.Clone(prefix)) })
	e.mu.Unlock()

	if err := e.fq.Attach(p); err != nil {
		return err
	}
	// Replay subscriptions on a non-blocking best-effort basis. If the
	// peer's pipe is full we silently drop; libzmq behaves similarly
	// (subscription forwarding is best-effort; the application is
	// expected to resubscribe on reconnect via the socket option).
	for _, prefix := range snapshot {
		e.forwardSubscription(context.Background(), p, true, prefix)
	}
	return nil
}

func (e *SubEngine) DetachPipe(p *pipe.End) error {
	e.mu.Lock()
	delete(e.peers, p)
	e.mu.Unlock()
	return e.fq.Detach(p)
}

// Subscribe adds prefix to the local filter and forwards a SUBSCRIBE to
// every attached peer. Duplicate Subscribe calls are refcounted: an equal
// number of Unsubscribe calls removes the filter.
func (e *SubEngine) Subscribe(prefix []byte) error { return e.subscribe(prefix, true) }

// Unsubscribe decrements prefix's refcount and, on the final decrement,
// forwards a CANCEL to every attached peer.
func (e *SubEngine) Unsubscribe(prefix []byte) error { return e.subscribe(prefix, false) }

func (e *SubEngine) subscribe(prefix []byte, subscribe bool) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("sub subscribe: %w", zmqerr.ErrSocketClosed)
	}
	// Only the first add or last remove is forwarded, so peers see one
	// command per unique prefix; refcount duplicates stay local.
	forward := e.applySubLocked(prefix, subscribe)
	peers := slices.Collect(maps.Keys(e.peers))
	e.mu.Unlock()
	if forward {
		for _, p := range peers {
			e.forwardSubscription(context.Background(), p, subscribe, prefix)
		}
	}
	return nil
}

// applySubLocked applies one subscribe or unsubscribe of prefix to the
// filter and reports whether it was the first add or the last remove, i.e.
// whether a SUBSCRIBE/CANCEL must go upstream. An unsubscribe of an absent
// prefix is a no-op. The caller holds e.mu.
func (e *SubEngine) applySubLocked(prefix []byte, subscribe bool) bool {
	if subscribe {
		return e.tree.Add(prefix)
	}
	_, gone := e.tree.Remove(prefix)
	return gone
}

// forwardSubscription sends a synthetic single-frame subscription
// message on p. Best-effort with a short deadline: libzmq subscription
// forwarding never back-pressures the subscriber against a slow PUB,
// but we want the send to land reliably when the pipe has room. A
// deadline (rather than a pre-cancelled ctx) avoids Go's select
// randomly preferring ctx.Done() over a ready send channel, which would
// drop subscriptions even when the pipe has capacity.
//
// The deadline is layered onto the caller's ctx, so the effective
// deadline is min(ctx deadline, subForwardTimeout) and an
// already-cancelled caller does not wait out the internal budget.
func (e *SubEngine) forwardSubscription(ctx context.Context, p *pipe.End, subscribe bool, prefix []byte) {
	sendCtx, cancel := context.WithTimeout(ctx, subForwardTimeout)
	defer cancel()
	_ = p.Send(sendCtx, msg.FromBytes(zmtp.SubscriptionFrame(subscribe, prefix)))
}

// subForwardTimeout bounds how long Subscribe/Unsubscribe will wait on
// a single peer's pipe before giving up. 100ms is conservative: the
// common case is an immediate send (<1µs) and we want to avoid
// silently losing subscriptions on a momentarily-congested pipe.
const subForwardTimeout = 100 * time.Millisecond

// Send on a SUB socket is an FSM violation. XSUB overrides this to
// treat the message as a subscription command.
func (e *SubEngine) Send(ctx context.Context, m *msg.Msg) error {
	if e.surface {
		return e.xsubSend(ctx, m)
	}
	return fmt.Errorf("sub send: %w", zmqerr.ErrFSMViolation)
}

// xsubSend is the XSUB semantics: the application sends a synthetic
// subscribe/unsubscribe message, which we both update our local filter
// with and forward to every peer.
func (e *SubEngine) xsubSend(ctx context.Context, m *msg.Msg) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("xsub send: %w", zmqerr.ErrSocketClosed)
	}
	isSub, subscribe, prefix := isSubscriptionMsg(m)
	if !isSub {
		e.mu.Unlock()
		// XSUB accepts only subscription commands from the app.
		return fmt.Errorf("xsub send: %w: not a subscription message", zmqerr.ErrFSMViolation)
	}
	// Update local filter state. XSUB forwards every app-issued command
	// upstream regardless of refcount (the app is explicitly driving the
	// subscription), so the shouldForward result is intentionally ignored
	// here — unlike Subscribe/Unsubscribe which dedup forwarding.
	e.applySubLocked(prefix, subscribe)
	peers := slices.Collect(maps.Keys(e.peers))
	e.mu.Unlock()

	for _, p := range peers {
		e.forwardSubscription(ctx, p, subscribe, prefix)
	}
	m.Release() // prefix was copied wherever it is kept
	e.sent.Add(1)
	return nil
}

// Recv fair-queues across attached pipes and applies the local
// subscription filter. Messages whose topic frame (first frame) is not
// matched by any local subscription are discarded and Recv loops. The
// payload is never sniffed: data beginning with 0x00/0x01 is ordinary data
// (the session drops upstream SUBSCRIBE/CANCEL commands for SUB/XSUB, as
// libzmq does).
func (e *SubEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	for {
		m, err := e.fq.Recv(ctx)
		if err != nil {
			return nil, err
		}
		if m.NumFrames() == 0 || !e.tree.Match(m.Frames()[0]) {
			m.Release()
			continue
		}
		e.recv.Add(1)
		return m, nil
	}
}

// Metrics reports the SUB/XSUB engine's counters. recv counts user-
// visible Recv returns (filter-matched messages). QueueDepth samples the
// FairQueue.
func (e *SubEngine) Metrics() Metrics {
	return e.snapshot(0, e.fq.QueueDepth())
}

func (e *SubEngine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.peers = nil
	e.mu.Unlock()
	return e.fq.Close()
}

var _ Engine = (*SubEngine)(nil)
