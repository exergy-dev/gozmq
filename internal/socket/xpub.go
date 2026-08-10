package socket

import (
	"context"
	"fmt"
	"sync"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
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
func NewXPubEngineV(verbose bool) iface.SocketEngine {
	return &xpubEngine{
		pub:      newPubEngineBase(true),
		verbose:  verbose,
		prefRefs: make(map[string]int),
		peerSubs: make(map[iface.Pipe]map[string]int),
	}
}

// xpubEngine decorates pubEngine with the XPUB-specific subscribe-event
// surface. In verbose mode Recv passes every synthetic subscription
// message straight through from the embedded pub engine's subInbound
// channel. In dedup mode Recv keeps a refcount per prefix across all
// attached peers and suppresses messages that do not change a prefix's
// presence in the aggregate subscription set (i.e. only the first
// subscriber to a prefix and the last unsubscriber surface).
//
// The peer-side subscription tree (SubTree per peer) lives in the
// embedded pubEngine and is updated unconditionally by the peer reader
// regardless of verbose mode — that way outbound fanout filtering stays
// correct even when the application never sees the SUBSCRIBE event.
type xpubEngine struct {
	pub     *pubEngine
	verbose bool

	mu       sync.Mutex
	prefRefs map[string]int                // prefix -> number of peers currently subscribed
	peerSubs map[iface.Pipe]map[string]int // pipe -> prefix -> sub count from that pipe
}

// AttachPipe delegates to the embedded pub engine.
func (e *xpubEngine) AttachPipe(p iface.Pipe) error { return e.pub.AttachPipe(p) }

// DetachPipe removes a peer and decrements the aggregate prefRefs for
// every prefix that peer was subscribed to, preventing a leak where
// departed peers leave stale refcounts that suppress future surface events.
//
// For each prefix whose aggregate refcount hits zero, a synthetic CANCEL
// (unsubscribe) message is pushed onto the subscription-event queue —
// matching libzmq, where a terminating pipe generates unsubscribe events
// so upstream proxy stages (XSUB side) can drop dead topics. The
// synthesized events bypass the Recv dedup filter (their refcounts were
// already settled here) and are delivered in both verbose and dedup mode.
func (e *xpubEngine) DetachPipe(p iface.Pipe) error {
	e.mu.Lock()
	var cancels []string
	if subs, ok := e.peerSubs[p]; ok {
		for prefix, count := range subs {
			e.prefRefs[prefix] -= count
			if e.prefRefs[prefix] <= 0 {
				delete(e.prefRefs, prefix)
				cancels = append(cancels, prefix)
			}
		}
		delete(e.peerSubs, p)
	}
	e.mu.Unlock()

	for _, prefix := range cancels {
		cm := msg.FromBytes(subscriptionFrame(false, []byte(prefix)))
		cm.MarkSubscription()
		// Non-blocking: a detach driven by the session layer must not
		// stall behind an application that is slow to drain Recv. On
		// overflow (or close) the event is dropped, mirroring the peer
		// reader's overflow policy.
		select {
		case e.pub.subInbound <- subInboundItem{m: cm}: // pipe == nil ⇒ synthesized
			e.pub.recvGate.arm()
		case <-e.pub.closeCh:
		default:
		}
	}
	return e.pub.DetachPipe(p)
}

// Send delegates to the embedded pub engine's fanout path.
func (e *xpubEngine) Send(ctx context.Context, m *msg.Msg) error { return e.pub.Send(ctx, m) }

// Close delegates to the embedded pub engine.
func (e *xpubEngine) Close() error { return e.pub.Close() }

// Metrics delegates to the embedded pub engine. The pub engine tracks
// sent (per Send call), dropped (per HWM-rejected peer), and recv (per
// surfaced subscription event consumed by this engine's Recv below).
func (e *xpubEngine) Metrics() iface.SocketMetrics { return e.pub.Metrics() }

func (e *xpubEngine) ReadyRecv() <-chan struct{}  { return e.pub.ReadyRecv() }
func (e *xpubEngine) ReadyWrite() <-chan struct{} { return e.pub.ReadyWrite() }
func (e *xpubEngine) CanRecvNow() bool            { return e.pub.CanRecvNow() }
func (e *xpubEngine) CanSendNow() bool            { return e.pub.CanSendNow() }

var _ Pollable = (*xpubEngine)(nil)

// Recv delivers the next SUBSCRIBE/CANCEL event to the application.
//
// Verbose mode: forward every synthetic subscription message the pub
// engine emits. Dedup mode: only forward the first SUBSCRIBE and last
// CANCEL for each distinct prefix across the union of attached peers.
// The filter spins internally until a surfaced event is produced or the
// engine closes — so a caller that blocks on Recv during a burst of
// duplicate SUBSCRIBEs sees at most one event per prefix transition.
func (e *xpubEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	for {
		// Read directly from the pub engine's internal sub channel.
		// We share the package, so this is legal and lets us interpose
		// a dedup filter without threading a new hook through pub.go.
		var item subInboundItem
		select {
		case item = <-e.pub.subInbound:
		case <-e.pub.closeCh:
			select {
			case item = <-e.pub.subInbound:
			default:
				return nil, fmt.Errorf("xpub recv: %w", zmqerr.ErrSocketClosed)
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if item.pipe == nil {
			// Synthesized by DetachPipe: refcounts are already settled,
			// so bypass the dedup filter and surface directly.
			e.pub.recv.Add(1)
			return item.m, nil
		}
		if e.verbose {
			// Surface everything, but still maintain the aggregate and
			// per-peer refcounts so DetachPipe can synthesize CANCELs for
			// the prefixes a departing peer leaves orphaned.
			e.shouldSurface(item.m, item.pipe)
			e.pub.recv.Add(1)
			return item.m, nil
		}
		if e.shouldSurface(item.m, item.pipe) {
			e.pub.recv.Add(1)
			return item.m, nil
		}
		// Silently dropped; loop to await the next event.
	}
}

// shouldSurface applies libzmq-style XPUB dedup: maintain an aggregate
// refcount per prefix, and only surface the first SUBSCRIBE for a given
// prefix and the last CANCEL (when the refcount returns to zero). A
// CANCEL for a prefix the engine has never surfaced is dropped.
func (e *xpubEngine) shouldSurface(m *msg.Msg, p iface.Pipe) bool {
	isSub, subscribe, prefix := isSubscriptionMsg(m)
	if !isSub {
		// Not a subscription command — pub.runPeerReader never pushes
		// anything else onto subInbound, so this shouldn't happen in
		// practice. Surface defensively: better than dropping on an
		// unexpected shape.
		return true
	}
	key := string(prefix)
	e.mu.Lock()
	defer e.mu.Unlock()
	if subscribe {
		prev := e.prefRefs[key]
		e.prefRefs[key] = prev + 1
		// Track per-peer subscription for cleanup on DetachPipe.
		if e.peerSubs[p] == nil {
			e.peerSubs[p] = make(map[string]int)
		}
		e.peerSubs[p][key]++
		return prev == 0
	}
	// Cancel.
	prev := e.prefRefs[key]
	if prev <= 0 {
		// Unknown prefix — drop the spurious cancel.
		return false
	}
	// Track per-peer unsubscription.
	if ps := e.peerSubs[p]; ps != nil {
		ps[key]--
		if ps[key] <= 0 {
			delete(ps, key)
		}
	}
	if prev == 1 {
		delete(e.prefRefs, key)
		return true
	}
	e.prefRefs[key] = prev - 1
	return false
}
