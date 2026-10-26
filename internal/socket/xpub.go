package socket

import (
	"context"
	"sync"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmtp"
	"github.com/exergy-dev/gozmq/msg"
)

// NewXPubEngine returns a new XPUB socket engine. When verbose is true
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
func NewXPubEngine(verbose bool) Engine {
	e := &xpubEngine{
		pubEngine: newPubEngineBase(),
		verbose:   verbose,
		prefRefs:  make(map[string]int),
	}
	e.onSub = e.onSubscription
	e.onDetach = e.releasePeer
	return e
}

// xpubEngine is pubEngine with the XPUB-specific subscribe-event surface.
// The embedded pub engine applies each queued command to the
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
	*pubEngine
	verbose bool

	evMu sync.Mutex
	// pending holds the surfaced events not yet returned by Recv.
	pending  []*msg.Msg
	prefRefs map[string]int // prefix -> subscriptions across all peers
}

// onSubscription is the pub engine's onSub hook; see xpubEngine.
//
// A CANCEL only counts if the peer itself held the prefix: otherwise a
// peer could release another peer's subscription and surface a CANCEL
// while that peer is still subscribed.
func (e *xpubEngine) onSubscription(_ *pipe.End, subscribe, held bool, prefix []byte, m *msg.Msg) (kept bool) {
	key := string(prefix)
	e.evMu.Lock()
	surface := e.verbose
	switch {
	case subscribe:
		e.prefRefs[key]++
		surface = surface || e.prefRefs[key] == 1
	case held:
		if e.prefRefs[key]--; e.prefRefs[key] == 0 {
			delete(e.prefRefs, key)
			surface = true
		}
	}
	if surface {
		e.pending = append(e.pending, m)
	}
	e.evMu.Unlock()
	if surface {
		e.armIn()
	}
	return surface
}

// releasePeer is the pub engine's onDetach hook: it releases every
// subscription a departing peer still held. For each prefix whose
// aggregate count hits zero, a CANCEL is queued — matching libzmq, where a
// terminating pipe generates unsubscribe events so upstream proxy stages
// (XSUB side) can drop dead topics. These CANCELs surface in both verbose
// and dedup mode. The pub engine applies the peer's queued commands first,
// so every SUBSCRIBE from it is queued before its CANCEL.
func (e *xpubEngine) releasePeer(tree *SubTree) {
	e.evMu.Lock()
	queued := false
	tree.Walk(func(prefix []byte, refs int) {
		key := string(prefix)
		if e.prefRefs[key] -= refs; e.prefRefs[key] <= 0 {
			delete(e.prefRefs, key)
			e.pending = append(e.pending, msg.FromBytes(zmtp.SubscriptionFrame(false, prefix)))
			queued = true
		}
	})
	e.evMu.Unlock()
	if queued {
		e.armIn()
	}
}

// Metrics reports the pub engine's counters (sent per Send call, dropped
// per HWM-rejected peer, recv per event returned by Recv) with the event
// queue length as QueueDepth.
func (e *xpubEngine) Metrics() Metrics {
	e.evMu.Lock()
	depth := uint64(len(e.pending))
	e.evMu.Unlock()
	return e.snapshot(depth)
}

// CanRecvNow applies the queued commands and reports whether an event is
// queued. When none is, the next command arms the gate.
func (e *xpubEngine) CanRecvNow() bool {
	e.applyPending()
	e.evMu.Lock()
	defer e.evMu.Unlock()
	return len(e.pending) > 0
}

// Recv returns the next queued SUBSCRIBE/CANCEL event. Which events are
// queued (all, or only prefix-presence transitions) was decided when the
// command was applied; see onSubscription.
func (e *xpubEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	for {
		e.applyPending()
		if m, ok := e.pop(); ok {
			return m, nil
		}
		select {
		case <-e.in:
		case <-e.fq.closeCh:
			if m, ok := e.pop(); ok {
				return m, nil
			}
			return nil, errClosed("xpub recv")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// pop dequeues the oldest pending event, re-arming the gate if more
// remain so another blocked Recv can take the next one.
func (e *xpubEngine) pop() (*msg.Msg, bool) {
	e.evMu.Lock()
	if len(e.pending) == 0 {
		e.evMu.Unlock()
		return nil, false
	}
	m := e.pending[0]
	e.pending[0] = nil
	e.pending = e.pending[1:]
	more := len(e.pending) > 0
	e.evMu.Unlock()
	if more {
		e.armIn()
	}
	e.recv.Add(1)
	return m, true
}
