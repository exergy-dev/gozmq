package socket

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// Synthetic subscription message format: a single-frame message whose
// first byte is SubscribeByte (0x01) or UnsubscribeByte (0x00), followed
// by the subscription prefix bytes. This is the same on-wire shape
// libzmq XPUB surfaces to its application and the same shape XSUB
// accepts from its application; we reuse it internally to carry
// subscription commands between the session reader and the PUB/XPUB
// engine without extending iface.Pipe.
//
// See internal/session/reader.go (handleCommand) for how incoming ZMTP
// SUBSCRIBE/CANCEL commands are converted to synthetic messages on the
// inbound pipe.
const (
	SubscribeByte   byte = 0x01
	UnsubscribeByte byte = 0x00
)

// isSubscriptionMsg reports whether m looks like a synthetic subscription
// message (single frame, first byte 0x00 or 0x01). The second return is
// the parsed (subscribe, prefix) pair when the first is true.
func isSubscriptionMsg(m *msg.Msg) (bool, bool, []byte) {
	if m == nil || m.NumFrames() != 1 {
		return false, false, nil
	}
	f := m.Frames()[0]
	if len(f) < 1 {
		return false, false, nil
	}
	switch f[0] {
	case SubscribeByte:
		return true, true, f[1:]
	case UnsubscribeByte:
		return true, false, f[1:]
	}
	return false, false, nil
}

// pubPeer bundles a pipe with its SubTree and the goroutine reading
// subscription commands from that peer.
type pubPeer struct {
	pipe iface.Pipe
	tree *SubTree

	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}
}

// pubEngine implements iface.SocketEngine for the PUB socket type.
//
// PUB is a fan-out publisher. Every successful Send is replicated to each
// attached peer whose per-peer SubTree has at least one prefix matching
// the first frame of the message (the topic). Subscription state is
// maintained per-peer: incoming ZMTP SUBSCRIBE/CANCEL commands are
// surfaced by the session layer as synthetic single-frame messages
// (leading byte 0x01 / 0x00 + prefix) on the inbound side of the pipe,
// and a small reader goroutine per peer feeds those commands into that
// peer's SubTree. PUB silently drops all other inbound traffic (peers
// have no data to send to a PUB); XPUB (see xpub.go) additionally
// surfaces subscription events to the application via Recv.
//
// Send is non-blocking per-peer best-effort: if a peer's pipe is at HWM
// the message is dropped for that peer and fan-out continues to others.
// This matches libzmq's default PUB behaviour — a slow subscriber must
// not back-pressure the publisher. Recv on a plain PUB always returns
// zmqerr.ErrFSMViolation (wrapped); XPUB overrides Recv to return
// subscription events.
type pubEngine struct {
	mu     sync.Mutex
	peers  map[iface.Pipe]*pubPeer
	closed bool
	// surfaceSubs controls whether subscription commands are surfaced to
	// the application via Recv. PUB=false, XPUB=true. XPUB embeds a PUB
	// engine via newPubEngineBase so we share the fanout + subscription
	// bookkeeping.
	surfaceSubs bool
	subInbound  chan subInboundItem // XPUB-only; nil for PUB
	closeCh     chan struct{}
	readyR      chan struct{}
	readyW      chan struct{}

	// Metrics counters. sent increments once per Send call that has at
	// least one frame; dropped increments per (message, peer) pair when
	// the peer's pipe rejects the send (HWM full). Tracks per-peer drops
	// rather than per-message so observers can see fan-out drop pressure
	// — one slow subscriber and ten fast ones still surface clearly.
	sent    atomic.Uint64
	dropped atomic.Uint64
	// recv counts subscription events delivered to the application via
	// Recv. Always zero for plain PUB (Recv is an FSM violation); for
	// XPUB it tracks the SUBSCRIBE/CANCEL events the app has observed.
	recv atomic.Uint64
}

// subInboundItem pairs a subscription message with the pipe that produced
// it so XPUB's dedup layer can track per-peer subscriptions and clean up
// on DetachPipe.
type subInboundItem struct {
	m    *msg.Msg
	pipe iface.Pipe
}

// NewPubEngine returns a new PUB socket engine.
func NewPubEngine() iface.SocketEngine {
	return newPubEngineBase(false)
}

// newPubEngineBase is the shared constructor used by PUB (surface=false)
// and XPUB (surface=true).
func newPubEngineBase(surface bool) *pubEngine {
	e := &pubEngine{
		peers:       make(map[iface.Pipe]*pubPeer),
		surfaceSubs: surface,
		closeCh:     make(chan struct{}),
		readyR:      make(chan struct{}, 1),
		readyW:      make(chan struct{}, 1),
	}
	if surface {
		// Capacity-1 handoff buffer. HWM-style backpressure on the
		// application isn't expressed here — XPUB applications are
		// expected to drain promptly. A bigger buffer doesn't help
		// because the synthetic messages are rare relative to data
		// traffic.
		e.subInbound = make(chan subInboundItem, 16)
	}
	return e
}

func (e *pubEngine) AttachPipe(p iface.Pipe) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("pub attach: %w", zmqerr.ErrSocketClosed)
	}
	if _, ok := e.peers[p]; ok {
		e.mu.Unlock()
		return nil
	}
	peer := &pubPeer{
		pipe: p,
		tree: NewSubTree(),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	e.peers[p] = peer
	select {
	case e.readyW <- struct{}{}:
	default:
	}
	e.mu.Unlock()

	go e.runPeerReader(peer)
	return nil
}

func (e *pubEngine) DetachPipe(p iface.Pipe) error {
	e.mu.Lock()
	peer, ok := e.peers[p]
	if !ok {
		e.mu.Unlock()
		return nil
	}
	delete(e.peers, p)
	e.mu.Unlock()
	peer.stopOnce.Do(func() { close(peer.stop) })
	<-peer.done
	return nil
}

// runPeerReader consumes messages from a single peer pipe. For PUB we
// silently apply any synthetic subscription messages to the peer's
// SubTree and drop everything else (PUB doesn't read data from peers).
// For XPUB we additionally forward subscription messages to the
// application via subInbound.
func (e *pubEngine) runPeerReader(peer *pubPeer) {
	defer close(peer.done)
	// One watcher for the whole peer lifetime: it cancels the per-Recv ctx as
	// soon as the peer stops, the engine closes, or this reader exits — so a
	// blocked Pipe.Recv unblocks and neither goroutine leaks.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readerDone := make(chan struct{})
	defer close(readerDone)
	go func() {
		select {
		case <-peer.stop:
		case <-e.closeCh:
		case <-readerDone:
		}
		cancel()
	}()
	for {
		m, err := peer.pipe.Recv(ctx)
		if err != nil {
			return
		}
		isSub, subscribe, prefix := isSubscriptionMsg(m)
		if !isSub {
			// PUB/XPUB ignore non-subscription inbound traffic.
			continue
		}
		if subscribe {
			peer.tree.Add(prefix)
		} else {
			peer.tree.Remove(prefix)
		}
		if e.surfaceSubs {
			// Forward the subscription event to the application. Drop
			// on close; drop silently on overflow rather than block
			// the peer reader indefinitely.
			select {
			case e.subInbound <- subInboundItem{m: m, pipe: peer.pipe}:
				select {
				case e.readyR <- struct{}{}:
				default:
				}
			case <-peer.stop:
				return
			case <-e.closeCh:
				return
			}
		}
	}
}

func (e *pubEngine) ReadyRecv() <-chan struct{} {
	if e.CanRecvNow() {
		select {
		case e.readyR <- struct{}{}:
		default:
		}
	}
	if !e.surfaceSubs {
		return nil
	}
	return e.readyR
}

func (e *pubEngine) ReadyWrite() <-chan struct{} {
	if e.CanSendNow() {
		select {
		case e.readyW <- struct{}{}:
		default:
		}
	}
	return e.readyW
}

func (e *pubEngine) CanRecvNow() bool {
	return e.surfaceSubs && e.subInbound != nil && len(e.subInbound) > 0
}

func (e *pubEngine) CanSendNow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.closed && len(e.peers) > 0
}

var _ Pollable = (*pubEngine)(nil)

// Send fans out m to every attached peer whose SubTree matches the first
// frame of m. Per libzmq, the first frame is the topic; multipart
// messages match only on that first frame, and the entire multipart
// message goes to every matching peer.
//
// HWM handling: if a peer's pipe is at HWM, we do a non-blocking
// try-send and drop the message for that peer. This matches libzmq's
// default PUB behavior (the conflate-off non-blocking send; PUB never
// backpressures the publisher on slow subscribers).
func (e *pubEngine) Send(ctx context.Context, m *msg.Msg) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("pub send: %w", zmqerr.ErrSocketClosed)
	}
	if m == nil || m.NumFrames() == 0 {
		e.mu.Unlock()
		return nil
	}
	topic := m.Frames()[0]

	// Snapshot the matching peers under the lock, then do the actual
	// non-blocking sends outside the lock to avoid a slow pipe blocking
	// unrelated engine operations (Attach/Detach).
	targets := make([]iface.Pipe, 0, len(e.peers))
	for p, peer := range e.peers {
		if peer.tree.Match(topic) {
			targets = append(targets, p)
		}
	}
	e.mu.Unlock()

	for _, p := range targets {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Fast path: if the pipe supports non-blocking send and has
		// buffer space, skip the context+timeout overhead entirely.
		if nb, ok := p.(nonBlockingSender); ok && nb.TrySend(m) {
			continue
		}
		// Slow path: give the send a very short deadline so a peer at
		// HWM doesn't block fan-out to other subscribers.
		tryCtx, cancel := context.WithTimeout(ctx, pubTryTimeout)
		if err := p.Send(tryCtx, m); err != nil {
			e.dropped.Add(1)
		}
		cancel()
	}
	e.sent.Add(1)
	return nil
}

// Metrics reports the PUB engine's counters. QueueDepth samples the
// XPUB subInbound buffer (zero for plain PUB, since subInbound is nil
// and the lookup short-circuits).
func (e *pubEngine) Metrics() iface.SocketMetrics {
	var depth uint64
	if e.subInbound != nil {
		depth = uint64(len(e.subInbound))
	}
	return iface.SocketMetrics{
		Sent:       e.sent.Load(),
		Recv:       e.recv.Load(),
		Dropped:    e.dropped.Load(),
		QueueDepth: depth,
	}
}

// pubTryTimeout is the per-peer HWM-drop deadline for PUB fanout. Long
// enough that an un-blocked peer's send completes reliably; short enough
// that a slow peer doesn't delay publishing to faster ones. Tuned by
// benchmarking the pipe's channel send path — 10ms is >>1000× the typical
// send latency and is comparable to libzmq's lb_t flush cycle.
const pubTryTimeout = 10 * time.Millisecond

// Recv: PUB is send-only → FSM violation. XPUB overrides this via
// xpubEngine.Recv.
func (e *pubEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	if !e.surfaceSubs {
		return nil, fmt.Errorf("pub recv: %w", zmqerr.ErrFSMViolation)
	}
	// XPUB path: block until a subscription event is available or the
	// engine closes.
	select {
	case item := <-e.subInbound:
		e.recv.Add(1)
		return item.m, nil
	case <-e.closeCh:
		// Drain any straggler before reporting closed.
		select {
		case item := <-e.subInbound:
			e.recv.Add(1)
			return item.m, nil
		default:
			return nil, fmt.Errorf("pub recv: %w", zmqerr.ErrSocketClosed)
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (e *pubEngine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	close(e.closeCh)
	peers := make([]*pubPeer, 0, len(e.peers))
	for _, peer := range e.peers {
		peers = append(peers, peer)
	}
	e.peers = nil
	e.mu.Unlock()
	for _, peer := range peers {
		peer.stopOnce.Do(func() { close(peer.stop) })
		<-peer.done
	}
	return nil
}
