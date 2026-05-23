package socket

import (
	"context"
	"fmt"
	"sync"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/zmqerr"
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
	prefRefs map[string]int // prefix -> number of peers currently subscribed
}

// AttachPipe delegates to the embedded pub engine.
func (e *xpubEngine) AttachPipe(p iface.Pipe) error { return e.pub.AttachPipe(p) }

// DetachPipe delegates to the embedded pub engine.
func (e *xpubEngine) DetachPipe(p iface.Pipe) error { return e.pub.DetachPipe(p) }

// Send delegates to the embedded pub engine's fanout path.
func (e *xpubEngine) Send(ctx context.Context, m *msg.Msg) error { return e.pub.Send(ctx, m) }

// Close delegates to the embedded pub engine.
func (e *xpubEngine) Close() error { return e.pub.Close() }

// Metrics delegates to the embedded pub engine. The pub engine tracks
// sent (per Send call), dropped (per HWM-rejected peer), and recv (per
// surfaced subscription event consumed by this engine's Recv below).
func (e *xpubEngine) Metrics() iface.SocketMetrics { return e.pub.Metrics() }

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
		var m *msg.Msg
		select {
		case m = <-e.pub.subInbound:
		case <-e.pub.closeCh:
			select {
			case m = <-e.pub.subInbound:
			default:
				return nil, fmt.Errorf("xpub recv: %w", zmqerr.ErrSocketClosed)
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if e.verbose {
			e.pub.recv.Add(1)
			return m, nil
		}
		if e.shouldSurface(m) {
			e.pub.recv.Add(1)
			return m, nil
		}
		// Silently dropped; loop to await the next event.
	}
}

// shouldSurface applies libzmq-style XPUB dedup: maintain an aggregate
// refcount per prefix, and only surface the first SUBSCRIBE for a given
// prefix and the last CANCEL (when the refcount returns to zero). A
// CANCEL for a prefix the engine has never surfaced is dropped.
func (e *xpubEngine) shouldSurface(m *msg.Msg) bool {
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
		return prev == 0
	}
	// Cancel.
	prev := e.prefRefs[key]
	if prev <= 0 {
		// Unknown prefix — drop the spurious cancel.
		return false
	}
	if prev == 1 {
		delete(e.prefRefs, key)
		return true
	}
	e.prefRefs[key] = prev - 1
	return false
}
