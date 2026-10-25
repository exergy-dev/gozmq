package socket

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// Synthetic subscription message format: a single-frame message whose
// first byte is SubscribeByte (0x01) or UnsubscribeByte (0x00), followed
// by the subscription prefix bytes. This is the same on-wire shape
// libzmq XPUB surfaces to its application and the same shape XSUB
// accepts from its application; we reuse it internally to carry
// subscription commands between the session reader and the PUB/XPUB
// engine without extending *pipe.End.
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
// Only the PUB/XPUB receive path and XSUB's app-send path classify by
// content: legacy ZMTP 3.0 peers send subscriptions as plain 0x00/0x01 data
// frames, and the session turns ZMTP 3.1 SUBSCRIBE/CANCEL commands into the
// same shape for PUB/XPUB only. SUB/XSUB never sniff their inbound data.
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

// pubPeer bundles a pipe with the SubTree its subscriptions build.
type pubPeer struct {
	pipe *pipe.End
	tree *SubTree
}

// pubEngine implements Engine for the PUB socket type.
//
// PUB is a fan-out publisher. Every successful Send is replicated to each
// attached peer whose per-peer SubTree has at least one prefix matching
// the first frame of the message (the topic). Subscription state is
// maintained per-peer: incoming ZMTP SUBSCRIBE/CANCEL commands are
// surfaced by the session layer as synthetic single-frame messages
// (leading byte 0x01 / 0x00 + prefix) on the inbound side of the pipe.
// A FairQueue over those inbound sides flags when any are queued, and the
// next Send applies them to the peers' SubTrees before fanning out, as
// libzmq applies subscriptions on its next send or recv; no goroutine runs
// per peer. PUB silently drops all other inbound traffic (peers have no
// data to send to a PUB); XPUB (see xpub.go) additionally surfaces
// subscription events to the application via its onSub hook.
//
// Send is non-blocking per-peer best-effort: if a peer's pipe is at HWM
// the message is dropped for that peer and fan-out continues to others.
// This matches libzmq's default PUB behaviour — a slow subscriber must
// not back-pressure the publisher. Recv on a plain PUB always returns
// zmqerr.ErrFSMViolation (wrapped); XPUB overrides Recv to return
// subscription events.
type pubEngine struct {
	// mu guards peers and serialises subscription processing, so a peer's
	// commands apply in order and none applies after DetachPipe(p) or
	// Close returns.
	mu    sync.Mutex
	peers map[*pipe.End]*pubPeer
	// fq holds the peers' inbound pipes; only applySubs reads it. Its
	// Pending flag is the one atomic load Send pays for subscriptions. It
	// is held by value so that load needs no pointer chase, which cost ~5%
	// on XPubXSubThroughput.
	fq FairQueue
	// snap is a copy-on-write snapshot of the attached peers. It is
	// rebuilt under mu whenever peer membership changes (AttachPipe /
	// DetachPipe) and read lock-free by Send, which would otherwise pay
	// a mutex plus a slice allocation on every message. The *pubPeer
	// entries are immutable after construction (per-message subscription
	// state lives in each peer's goroutine-safe SubTree), so a loaded
	// snapshot is always safe to iterate. nil after Close: it is the
	// closed flag.
	snap atomic.Pointer[[]*pubPeer]
	// onSub, when set, receives every subscription command from peer p
	// right after it has been applied to p's SubTree (held reports whether
	// a CANCEL found the prefix there), and reports whether it kept m
	// (taking over its reference). onDetach, when set, receives a
	// departing peer's tree once its queued commands have been applied.
	// Both run under mu, never after DetachPipe(p) or Close returns. XPUB
	// installs them to maintain its refcounts and event queue; nil for PUB.
	onSub    func(p *pipe.End, subscribe, held bool, prefix []byte, m *msg.Msg) (kept bool)
	onDetach func(tree *SubTree)

	// The in-gate is fq's (XPUB's events; PUB has no recv direction).
	gates

	// Metrics counters (embedded). sent increments once per Send call that
	// has at least one frame; dropped increments per (message, peer) pair
	// when the peer's pipe rejects the send (HWM full) — per-peer rather
	// than per-message so observers see fan-out drop pressure. recv counts
	// subscription events delivered to the application via Recv (zero for
	// plain PUB; XPUB tracks observed SUBSCRIBE/CANCEL events).
	counters
}

// NewPubEngine returns a new PUB socket engine.
func NewPubEngine() Engine {
	return newPubEngineBase()
}

// newPubEngineBase is the shared constructor used by PUB and XPUB, which
// sets its hooks afterwards.
func newPubEngineBase() *pubEngine {
	e := &pubEngine{
		peers: make(map[*pipe.End]*pubPeer),
		gates: newGates(),
	}
	e.fq.init(e.in)
	e.snap.Store(&[]*pubPeer{})
	return e
}

// rebuildSnapLocked refreshes the copy-on-write peer snapshot from
// e.peers. Caller must hold e.mu; must not be called after Close (which
// leaves the snapshot nil as the closed sentinel).
func (e *pubEngine) rebuildSnapLocked() {
	s := slices.Collect(maps.Values(e.peers))
	e.snap.Store(&s)
}

func (e *pubEngine) AttachPipe(p *pipe.End) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.snap.Load() == nil {
		return errClosed("pub attach")
	}
	if _, ok := e.peers[p]; ok {
		return nil
	}
	e.peers[p] = &pubPeer{pipe: p, tree: NewSubTree()}
	e.rebuildSnapLocked()
	e.armOut()
	return e.fq.Attach(p)
}

// DetachPipe removes p. The commands p queued before leaving are applied
// first, so XPUB sees every SUBSCRIBE its DetachPipe then CANCELs.
func (e *pubEngine) DetachPipe(p *pipe.End) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	peer, ok := e.peers[p]
	if !ok {
		return nil
	}
	// Bounded by the backlog on entry (exact once p is closed), so a peer
	// still flooding an open pipe cannot keep DetachPipe here.
	for n := p.Len(); n > 0; n-- {
		m, ok := p.TryRecv()
		if !ok {
			break
		}
		e.applyLocked(peer, m)
	}
	if e.onDetach != nil {
		e.onDetach(peer.tree)
	}
	delete(e.peers, p)
	e.rebuildSnapLocked()
	return e.fq.Detach(p)
}

// applyPending applies the queued subscription commands, if any, for one
// atomic load when there are none. A false Pending is exact and leaves
// every pipe primed to set it, and arm the in-gate, on the next
// command.
func (e *pubEngine) applyPending() {
	if e.fq.Pending() {
		e.applySubs()
	}
}

// applySubs applies every queued inbound message to its peer's SubTree.
func (e *pubEngine) applySubs() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for {
		m, p, ok := e.fq.TryRecvWithSource()
		if !ok {
			return
		}
		// A pipe detached while closed with a backlog stays in fq until
		// drained, with no peer left to apply its messages to.
		e.applyLocked(e.peers[p], m)
	}
}

// applyLocked applies one inbound message from peer (nil once detached)
// and releases it unless onSub kept it. PUB/XPUB ignore non-subscription
// inbound traffic. Caller holds e.mu.
func (e *pubEngine) applyLocked(peer *pubPeer, m *msg.Msg) {
	isSub, subscribe, prefix := isSubscriptionMsg(m)
	if !isSub || peer == nil {
		m.Release()
		return
	}
	held := true
	if subscribe {
		peer.tree.Add(prefix)
	} else {
		held, _ = peer.tree.Remove(prefix)
	}
	if e.onSub == nil || !e.onSub(peer.pipe, subscribe, held, prefix, m) {
		m.Release()
	}
}

// CanRecvNow: PUB has no recv direction. XPUB provides its own.
func (e *pubEngine) CanRecvNow() bool { return false }

func (e *pubEngine) CanSendNow() bool {
	s := e.snap.Load()
	return s != nil && len(*s) > 0
}

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
// A plain *Msg is fanned out as ONE value to every matching pipe; the
// session writers only read its frames, so sharing it needs no copies. A
// lent message instead gets a derived copy per pipe, each owning one
// reference, so every receiver (a writer, or an inproc peer's Release)
// gives up exactly its own.
func (e *pubEngine) Send(ctx context.Context, m *msg.Msg) error {
	snap := e.snap.Load()
	if snap == nil {
		return errClosed("pub send")
	}
	if m == nil || m.NumFrames() == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	topic := m.Frames()[0]
	// Subscriptions that arrived before this Send filter it. Applying them
	// changes no peer's membership, so snap stays current.
	e.applyPending()

	// Single pass, no allocation and no engine lock: each peer's SubTree
	// is goroutine-safe (an atomic flag answers subscribe-to-all), and the
	// snapshot entries are immutable. A peer detached concurrently may
	// still appear in a loaded snapshot; its pipe rejects the TrySend,
	// which counts as a drop.
	//
	// Drop-on-HWM: a peer whose pipe has no room loses the message
	// immediately (libzmq parity — no grace deadline, so one slow
	// subscriber never delays delivery to the others).
	lent := m.Lent()
	for _, peer := range *snap {
		if !peer.tree.Match(topic) {
			continue
		}
		out := m
		if lent {
			out = m.Derive(m.Frames()...)
		}
		if !peer.pipe.TrySend(out) {
			out.Release()
			e.dropped.Add(1)
		}
	}
	m.Release()
	e.sent.Add(1)
	return nil
}

// Metrics reports the PUB engine's counters. PUB has no inbound queue,
// so QueueDepth is always zero.
func (e *pubEngine) Metrics() Metrics {
	return e.snapshot(0)
}

// Recv: PUB is send-only → FSM violation. XPUB never delegates here.
func (e *pubEngine) Recv(context.Context) (*msg.Msg, error) {
	return nil, fmt.Errorf("pub recv: %w", zmqerr.ErrFSMViolation)
}

// Close detaches every peer; the pipes are left to their owners. Commands
// still queued are never applied.
func (e *pubEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.snap.Load() == nil {
		return nil
	}
	// nil snapshot is the lock-free closed sentinel Send checks.
	e.snap.Store(nil)
	e.peers = nil
	return e.fq.Close()
}
