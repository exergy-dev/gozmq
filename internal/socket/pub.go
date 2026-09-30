package socket

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
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
//
// This content-sniffing classifier coexists with msg.IsSubscription (the
// out-of-band flag the session reader sets on genuine SUBSCRIBE/CANCEL
// commands). They serve different sides: the PUB/XPUB receive path (and
// XSUB's app-send path) must use content sniffing because legacy ZMTP 3.0
// peers send subscriptions as plain 0x00/0x01 data frames that never carry
// the flag; the SUB/XSUB receive path uses the flag instead, so ordinary
// data beginning with 0x00/0x01 is never misclassified as a subscription.
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
// surfaces subscription events to the application via its onSub hook.
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
	// snap is a copy-on-write snapshot of the attached peers. It is
	// rebuilt under mu whenever peer membership changes (AttachPipe /
	// DetachPipe) and read lock-free by Send, which would otherwise pay
	// a mutex plus a slice allocation on every message. The *pubPeer
	// entries are immutable after construction (per-message subscription
	// state lives in each peer's goroutine-safe SubTree), so a loaded
	// snapshot is always safe to iterate. nil after Close — Send uses
	// that as its closed check.
	snap atomic.Pointer[[]*pubPeer]
	// onSub, when set, receives every subscription command from peer p
	// right after it has been applied to p's SubTree. It runs on p's
	// reader goroutine, never after DetachPipe(p) or Close returns. XPUB
	// installs it to maintain its refcounts and event queue; nil for PUB.
	onSub    func(p iface.Pipe, subscribe bool, prefix []byte, m *msg.Msg)
	closeCh  chan struct{}
	sendGate readyGate

	// Metrics counters (embedded). sent increments once per Send call that
	// has at least one frame; dropped increments per (message, peer) pair
	// when the peer's pipe rejects the send (HWM full) — per-peer rather
	// than per-message so observers see fan-out drop pressure. recv counts
	// subscription events delivered to the application via Recv (zero for
	// plain PUB; XPUB tracks observed SUBSCRIBE/CANCEL events).
	counters
}

// subscriptionFrame builds the single-frame synthetic subscription
// payload: SubscribeByte/UnsubscribeByte followed by the prefix bytes.
// Shared by SUB/XSUB's upstream forwarding (sub.go) and XPUB's
// detach-synthesized CANCELs (xpub.go).
func subscriptionFrame(subscribe bool, prefix []byte) []byte {
	buf := make([]byte, 1+len(prefix))
	if subscribe {
		buf[0] = SubscribeByte
	} else {
		buf[0] = UnsubscribeByte
	}
	copy(buf[1:], prefix)
	return buf
}

// NewPubEngine returns a new PUB socket engine.
func NewPubEngine() iface.SocketEngine {
	return newPubEngineBase(nil)
}

// newPubEngineBase is the shared constructor used by PUB (onSub nil) and
// XPUB.
func newPubEngineBase(onSub func(iface.Pipe, bool, []byte, *msg.Msg)) *pubEngine {
	e := &pubEngine{
		peers:    make(map[iface.Pipe]*pubPeer),
		onSub:    onSub,
		closeCh:  make(chan struct{}),
		sendGate: newReadyGate(),
	}
	e.snap.Store(&[]*pubPeer{})
	return e
}

// rebuildSnapLocked refreshes the copy-on-write peer snapshot from
// e.peers. Caller must hold e.mu; must not be called after Close (which
// leaves the snapshot nil as the closed sentinel).
func (e *pubEngine) rebuildSnapLocked() {
	s := make([]*pubPeer, 0, len(e.peers))
	for _, peer := range e.peers {
		s = append(s, peer)
	}
	e.snap.Store(&s)
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
	e.rebuildSnapLocked()
	e.sendGate.arm()
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
	e.rebuildSnapLocked()
	e.mu.Unlock()
	peer.stopOnce.Do(func() { close(peer.stop) })
	<-peer.done
	return nil
}

// runPeerReader consumes messages from a single peer pipe, applying
// subscription commands to the peer's SubTree and dropping everything
// else (PUB doesn't read data from peers). It never blocks on the
// application: XPUB's onSub hook only queues. Once the pipe closes, the
// commands the peer sent before leaving are still applied, so XPUB sees
// every SUBSCRIBE its DetachPipe will later CANCEL.
func (e *pubEngine) runPeerReader(peer *pubPeer) {
	defer close(peer.done)
	cp := asChanPipe(peer.pipe)
	recvCh := cp.RecvCh()
	for {
		select {
		case m := <-recvCh:
			e.applyPeerMsg(peer, m)
		case <-cp.Closed():
			// Draining a closed pipe is finite: Recv returns what is
			// buffered, then EOF.
			for {
				m, err := peer.pipe.Recv(context.Background())
				if err != nil {
					return
				}
				e.applyPeerMsg(peer, m)
			}
		case <-peer.stop:
			return
		case <-e.closeCh:
			return
		}
	}
}

// applyPeerMsg applies one inbound message from peer. PUB/XPUB ignore
// non-subscription inbound traffic.
func (e *pubEngine) applyPeerMsg(peer *pubPeer, m *msg.Msg) {
	isSub, subscribe, prefix := isSubscriptionMsg(m)
	if !isSub {
		return
	}
	if subscribe {
		peer.tree.Add(prefix)
	} else {
		peer.tree.Remove(prefix)
	}
	if e.onSub != nil {
		e.onSub(peer.pipe, subscribe, prefix, m)
	}
}

// ReadyRecv: PUB has no recv direction. XPUB provides its own.
func (e *pubEngine) ReadyRecv() <-chan struct{} { return nil }

func (e *pubEngine) ReadyWrite() <-chan struct{} {
	return e.sendGate.signal(e.CanSendNow())
}

// CanRecvNow: PUB has no recv direction. XPUB provides its own.
func (e *pubEngine) CanRecvNow() bool { return false }

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
// HWM handling: if a peer's pipe is at HWM, the non-blocking try-send
// fails and the message is dropped for that peer immediately. This
// matches libzmq's default PUB behavior (conflate-off non-blocking send;
// PUB never backpressures — or even stalls — the publisher on slow
// subscribers).
//
// ONE *Msg value is fanned out to every matching pipe; the session
// writers only read its frames, so sharing it needs no copies.
func (e *pubEngine) Send(ctx context.Context, m *msg.Msg) error {
	snap := e.snap.Load()
	if snap == nil {
		return fmt.Errorf("pub send: %w", zmqerr.ErrSocketClosed)
	}
	if m == nil || m.NumFrames() == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	topic := m.Frames()[0]

	// Single pass, no allocation: each peer's SubTree is goroutine-safe,
	// and the snapshot entries are immutable, so matching needs no engine
	// lock. A peer detached concurrently may still appear in a loaded
	// snapshot; its pipe rejects the TrySend, which counts as a drop.
	//
	// Drop-on-HWM: a peer whose pipe has no room loses the message
	// immediately (libzmq parity — no grace deadline, so one slow
	// subscriber never delays delivery to the others).
	for _, peer := range *snap {
		if peer.tree.Match(topic) && !asChanPipe(peer.pipe).TrySend(m) {
			e.dropped.Add(1)
		}
	}
	e.sent.Add(1)
	return nil
}

// Metrics reports the PUB engine's counters. PUB has no inbound queue,
// so QueueDepth is always zero.
func (e *pubEngine) Metrics() iface.SocketMetrics {
	return e.snapshot(e.dropped.Load(), 0)
}

// Recv: PUB is send-only → FSM violation. XPUB never delegates here.
func (e *pubEngine) Recv(context.Context) (*msg.Msg, error) {
	return nil, fmt.Errorf("pub recv: %w", zmqerr.ErrFSMViolation)
}

func (e *pubEngine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	close(e.closeCh)
	// nil snapshot is the lock-free closed sentinel Send checks.
	e.snap.Store(nil)
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
