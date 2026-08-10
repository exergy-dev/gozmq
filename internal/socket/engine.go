// Package socket holds the per-pattern SocketEngine implementations that
// back the public gozmq Socket types. Each ZMTP 3.1 socket pattern — PAIR,
// PUSH, PULL, PUB, SUB, XPUB, XSUB, REQ, REP, DEALER, ROUTER, STREAM — is
// realised by its own engine in this package.
//
// # SocketEngine contract
//
// The per-pattern engines implement iface.SocketEngine. That interface
// binds a narrow lifecycle (AttachPipe, DetachPipe, Close) to a two-method
// dispatch surface (Send, Recv). Pipes are handed to the engine by the
// session layer post-handshake; the engine owns all routing, subscription,
// and FSM state associated with those pipes. Close unblocks any pending
// Send/Recv with zmqerr.ErrSocketClosed and detaches every pipe.
//
// Error sentinels are stable: engines surface zmqerr.ErrSocketClosed after
// Close, zmqerr.ErrFSMViolation for patterns that disallow a direction
// (e.g. PUSH.Recv, SUB.Send) or a mis-ordered FSM step (REQ/REP), and
// zmqerr.ErrRouteUnknown on ROUTER with the mandatory option set.
//
// # Reusable primitives
//
// Two primitives (in sibling files loadbalancer.go and fairqueue.go) are
// shared across engines so each pattern can stay focused on its policy:
//
//   - LoadBalancer — round-robin outbound dispatcher over the attached
//     pipe set, rotating past peers at HWM and blocking only when every
//     peer is full. Used by PUSH and DEALER. (REQ does its own
//     round-robin with sticky-peer semantics via the shared pipeRing
//     helper, not LoadBalancer.)
//   - FairQueue — fair-queued inbound multiplexer that pumps every
//     attached pipe into a single shared channel. Used by PULL, DEALER,
//     REP, ROUTER, SUB, and XSUB.
//
// Both depend only on iface.Pipe so engine-specific logic (identity
// prefixing, subscription filtering, FSM state, sticky peer selection)
// stays in the per-engine files rather than leaking into the primitive.
//
// A third shared primitive, SubTree, is a refcounted byte-prefix trie used
// by PUB/XPUB (one per peer) and SUB/XSUB (one per socket) for subscription
// matching.
//
// # Thread-safety model
//
// Engines must be fully goroutine-safe: there is no external
// serialization. The public Socket invokes Send/Recv/AttachPipe/
// DetachPipe/Close without holding any lock, application goroutines may
// call Send and Recv concurrently, and the session layer drives
// AttachPipe/DetachPipe from its own goroutines. Every engine therefore
// guards its own state — with its mutex, atomics, or by delegating to
// the goroutine-safe primitives — including against the internal
// goroutines it spawns (FairQueue forwarders, pub/xpub per-peer readers).
//
// LoadBalancer, FairQueue, and SubTree are themselves goroutine-safe so
// tests and future patterns can use them directly.
//
// # Ownership model
//
// Each engine owns the per-peer bookkeeping for every pipe it has been
// handed: subscription trees, routing identity tables, sticky peer state,
// FSM bits, reply envelopes. It does not own the pipes themselves — the
// session layer creates and closes those, and calls AttachPipe/DetachPipe
// to let the engine react. The Socket owns the engine and is responsible
// for calling Close exactly once.
package socket

import (
	"sync/atomic"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/msg"
)

// This file holds the shared engine plumbing: the metric counters struct,
// the readiness gate, and the Pollable interface. The two large reusable
// primitives live in sibling files in this package: LoadBalancer in
// loadbalancer.go, FairQueue in fairqueue.go. The simple combinator engines
// (PUSH/PULL/DEALER) are in combinators.go.

// --------------------------------------------------------------------------
// Shared engine plumbing
// --------------------------------------------------------------------------

// counters holds the standard per-engine metric counters. Engines embed it so
// the promoted sent/recv/dropped fields work in place (e.counters.sent ==
// e.sent), and snapshot builds the iface.SocketMetrics. dropped and queueDepth
// are passed explicitly because they usually combine an engine-local counter
// with a FairQueue gauge.
type counters struct {
	sent    atomic.Uint64
	recv    atomic.Uint64
	dropped atomic.Uint64
}

func (c *counters) snapshot(dropped, queueDepth uint64) iface.SocketMetrics {
	return iface.SocketMetrics{
		Sent:       c.sent.Load(),
		Recv:       c.recv.Load(),
		Dropped:    dropped,
		QueueDepth: queueDepth,
	}
}

// readyGate is a level-triggered, single-slot readiness signal for the Poller.
// arm makes the gate readable (idempotent, non-blocking); signal re-arms the
// gate iff ready and returns the channel — the canonical Ready{Recv,Write}
// body. The Poller drains the channel and the next signal/arm re-arms it.
type readyGate struct{ ch chan struct{} }

func newReadyGate() readyGate { return readyGate{ch: make(chan struct{}, 1)} }

func (g readyGate) arm() {
	select {
	case g.ch <- struct{}{}:
	default:
	}
}

func (g readyGate) signal(ready bool) <-chan struct{} {
	if ready {
		g.arm()
	}
	return g.ch
}

// Pollable is the optional interface a SocketEngine implements to expose
// non-consuming readiness signals to the public Poller API. The two
// channels are level-armed: a chan with a buffered value (or that is
// closed) means "ready right now"; a chan that blocks means "not ready".
// Either channel may be nil — Poller skips nil channels. Engines that
// have no recv direction (PUSH) return nil from ReadyRecv; engines with
// no send direction (PULL, SUB, XSUB) return nil from ReadyWrite.
type Pollable interface {
	// ReadyRecv returns a channel that is armed when at least one
	// message is buffered and immediately available to Recv. Reads
	// from the channel do not consume the message; the engine
	// re-arms the signal as long as data remains. Returns nil for
	// send-only engines.
	ReadyRecv() <-chan struct{}
	// ReadyWrite returns a channel that is armed when at least one
	// peer is attached and a Send would not block on the peer-set
	// being empty. (HWM-induced blocking is not surfaced; this
	// mirrors libzmq's poll semantics where POLLOUT means "a peer
	// exists," not "the next byte is guaranteed to land instantly.")
	// Returns nil for recv-only engines.
	ReadyWrite() <-chan struct{}
	// CanRecvNow is a non-consuming level check: true iff a Recv
	// would not block on inbound queue emptiness right now. Always
	// false for send-only engines.
	CanRecvNow() bool
	// CanSendNow is a non-consuming level check: true iff a Send
	// would not block on peer-set emptiness right now (HWM blocking
	// is not surfaced; see ReadyWrite). Always false for recv-only.
	CanSendNow() bool
}

// chanPipe is the channel-level surface of a pipe end: the iface.Pipe
// contract plus the direct channel accessors and non-blocking helpers
// engines use to inline pipe I/O into their own selects (no per-call
// goroutine or context). Every iface.Pipe in the tree is *pipe.end
// (constructed via pipe.NewPair — see internal/session, internal/stream,
// and the root direct-pipe path), which implements all of these.
type chanPipe interface {
	iface.Pipe
	// RecvCh exposes the inbound channel for use in selects. Callers
	// handle pipe-closed semantics themselves (via Closed()).
	RecvCh() <-chan *msg.Msg
	// SendCh exposes the outbound channel for use in selects.
	SendCh() chan<- *msg.Msg
	// TrySend is a non-blocking Send; false when full or closed.
	TrySend(*msg.Msg) bool
	// TryRecv is a non-blocking Recv; false when empty or closed.
	TryRecv() (*msg.Msg, bool)
	// SetRecvReady registers a level-trigger gate armed whenever a
	// message becomes available to Recv (and on close). Engines hand
	// their Poller-facing readyGate channel to the pipe so blocking
	// Poll wakes on data arrival.
	SetRecvReady(gate chan<- struct{})
}

// asChanPipe converts p to its channel-level surface. The conversion is
// a structural invariant (*pipe.end is the sole iface.Pipe
// implementation); failure means a new pipe type was introduced without
// the channel accessors and is a programmer error worth the panic.
func asChanPipe(p iface.Pipe) chanPipe { return p.(chanPipe) } //nolint:errcheck // deliberate panic, see above
