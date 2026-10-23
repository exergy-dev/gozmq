package socket

import (
	"context"
	"fmt"
	"sync"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
	"github.com/exergy-dev/gozmq/msg"
)

// NewXPubEngineV returns a new XPUB socket engine. When verbose is true
// every SUBSCRIBE/CANCEL command observed on any attached peer is
// forwarded to the application via Recv. When verbose is false the
// engine applies libzmq-style deduplication: only the first SUBSCRIBE
// and the last CANCEL for each distinct prefix, across the set of
// attached peers, surfaces to the application. Duplicate SUBSCRIBEs
// from the same peer, and SUBSCRIBEs for a prefix already tracked by
// another peer, are applied to the routing tables silently.
//
// libzmq default is verbose=false (dedup enabled); callers that want to
// observe every subscriber churn event must opt in with verbose=true.
// Mirrors the semantics of ZMQ_XPUB_VERBOSE on a classic libzmq XPUB.
func NewXPubEngineV(verbose bool) Engine {
	e := &xpubEngine{
		verbose:  verbose,
		prefRefs: make(map[string]int),
		peerSubs: make(map[*pipe.End]map[string]int),
	}
	e.pub = newPubEngineBase(e.onSubscription)
	return e
}

// xpubEngine decorates pubEngine with the XPUB-specific subscribe-event
// surface. The embedded pub engine applies each queued command to the
// peer's SubTree (so outbound fanout filtering is always current) on the
// next Send, Recv or readiness check, and then hands it to onSubscription,
// which settles the refcounts and decides right there whether the event
// surfaces. Surfacing events are queued for Recv; the rest are released.
//
// Deciding at apply time, rather than when the application reads, keeps
// the refcounts in step with the SubTrees: a peer that subscribes and
// detaches before the application calls Recv is still accounted for, so
// DetachPipe emits its CANCEL and later SUBSCRIBEs are not suppressed.
//
// The event queue is unbounded, as in libzmq (its XPUB pending queue):
// Send applies commands too, so an application that publishes without
// reading events still keeps every SubTree current. Memory is bounded by
// the peers' command rate against the application's Recv rate; in dedup
// mode only prefix-presence transitions are queued.
//
// Blocked receivers and the Poller share the pub engine's in-gate (its
// FairQueue's gate), which a command arriving on an idle pipe arms, and so
// does every enqueue here.
type xpubEngine struct {
	pub     *pubEngine
	verbose bool

	mu sync.Mutex
	// pending holds the surfaced events not yet returned by Recv.
	pending  []*msg.Msg
	prefRefs map[string]int               // prefix -> subscriptions across all peers
	peerSubs map[*pipe.End]map[string]int // pipe -> prefix -> subscriptions from that pipe
}

// onSubscription is the pub engine's onSub hook; see xpubEngine.
//
// A CANCEL only counts if the peer itself holds the prefix: otherwise a
// peer could release another peer's subscription and surface a CANCEL
// while that peer is still subscribed.
func (e *xpubEngine) onSubscription(p *pipe.End, subscribe bool, prefix []byte, m *msg.Msg) (kept bool) {
	key := string(prefix)
	e.mu.Lock()
	surface := e.verbose
	ps := e.peerSubs[p]
	switch {
	case subscribe:
		if ps == nil {
			ps = make(map[string]int)
			e.peerSubs[p] = ps
		}
		ps[key]++
		e.prefRefs[key]++
		surface = surface || e.prefRefs[key] == 1
	case ps[key] > 0:
		if ps[key]--; ps[key] == 0 {
			delete(ps, key)
		}
		if e.prefRefs[key]--; e.prefRefs[key] == 0 {
			delete(e.prefRefs, key)
			surface = true
		}
	}
	if surface {
		e.pending = append(e.pending, m)
	}
	e.mu.Unlock()
	if surface {
		e.pub.armIn()
	}
	return surface
}

// AttachPipe delegates to the embedded pub engine.
func (e *xpubEngine) AttachPipe(p *pipe.End) error { return e.pub.AttachPipe(p) }

// DetachPipe removes a peer and releases every subscription it still
// held. For each prefix whose aggregate count hits zero, a CANCEL is
// queued — matching libzmq, where a terminating pipe generates
// unsubscribe events so upstream proxy stages (XSUB side) can drop dead
// topics. These CANCELs surface in both verbose and dedup mode.
//
// The pub engine first applies the commands p queued and detaches it, so
// every SUBSCRIBE from p is queued before its CANCEL and no command from
// p can be applied after its subscriptions are released.
func (e *xpubEngine) DetachPipe(p *pipe.End) error {
	err := e.pub.DetachPipe(p)
	e.mu.Lock()
	queued := false
	for prefix, count := range e.peerSubs[p] {
		if e.prefRefs[prefix] -= count; e.prefRefs[prefix] <= 0 {
			delete(e.prefRefs, prefix)
			e.pending = append(e.pending, msg.FromBytes(zmtp.SubscriptionFrame(false, []byte(prefix))))
			queued = true
		}
	}
	delete(e.peerSubs, p)
	e.mu.Unlock()
	if queued {
		e.pub.armIn()
	}
	return err
}

// Send delegates to the embedded pub engine's fanout path.
func (e *xpubEngine) Send(ctx context.Context, m *msg.Msg) error { return e.pub.Send(ctx, m) }

// Close delegates to the embedded pub engine. Events already queued stay
// receivable, as with other engines' close-time drain.
func (e *xpubEngine) Close() error { return e.pub.Close() }

// Metrics reports the embedded pub engine's counters (sent per Send call,
// dropped per HWM-rejected peer, recv per event returned by Recv) with the
// event queue length as QueueDepth.
func (e *xpubEngine) Metrics() Metrics {
	e.mu.Lock()
	depth := uint64(len(e.pending))
	e.mu.Unlock()
	return e.pub.snapshot(e.pub.dropped.Load(), depth)
}

func (e *xpubEngine) Gates() (in, out chan struct{}) { return e.pub.Gates() }
func (e *xpubEngine) CanSendNow() bool               { return e.pub.CanSendNow() }

// CanRecvNow applies the queued commands and reports whether an event is
// queued. When none is, the next command arms the gate.
func (e *xpubEngine) CanRecvNow() bool {
	e.pub.applyPending()
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.pending) > 0
}

// Recv returns the next queued SUBSCRIBE/CANCEL event. Which events are
// queued (all, or only prefix-presence transitions) was decided when the
// command was applied; see onSubscription.
func (e *xpubEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	for {
		e.pub.applyPending()
		if m, ok := e.pop(); ok {
			return m, nil
		}
		select {
		case <-e.pub.in:
		case <-e.pub.fq.closeCh:
			if m, ok := e.pop(); ok {
				return m, nil
			}
			return nil, fmt.Errorf("xpub recv: %w", zmqerr.ErrSocketClosed)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// pop dequeues the oldest pending event, re-arming the gate if more
// remain so another blocked Recv can take the next one.
func (e *xpubEngine) pop() (*msg.Msg, bool) {
	e.mu.Lock()
	if len(e.pending) == 0 {
		e.mu.Unlock()
		return nil, false
	}
	m := e.pending[0]
	e.pending[0] = nil
	e.pending = e.pending[1:]
	more := len(e.pending) > 0
	e.mu.Unlock()
	if more {
		e.pub.armIn()
	}
	e.pub.recv.Add(1)
	return m, true
}
