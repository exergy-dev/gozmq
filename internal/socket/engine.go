// Package socket holds the per-pattern Engine implementations that
// back the public gozmq Socket types. Each ZMTP 3.1 socket pattern — PAIR,
// PUSH, PULL, PUB, SUB, XPUB, XSUB, REQ, REP, DEALER, ROUTER, STREAM — is
// realised by its own engine in this package.
//
// # Engine contract
//
// The per-pattern engines implement Engine. That interface
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
//   - FairQueue — fair-queued inbound multiplexer that receivers read
//     round-robin across the attached pipes, with no goroutines. Used by
//     PULL, DEALER, REP, ROUTER, STREAM, SUB, and XSUB.
//
// Both depend only on *pipe.End so engine-specific logic (identity
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
// the goroutine-safe primitives. Engines spawn no goroutines of their
// own.
//
// LoadBalancer, FairQueue, and SubTree are themselves goroutine-safe so
// tests and future patterns can use them directly.
//
// # Message references
//
// Every *msg.Msg queued on a pipe carries one reference (a no-op for plain
// memory; see msg's "Lent frames"), owned by whoever holds the message.
// Send takes over m's reference when it returns nil — handing m to one
// pipe, or releasing it once a derived message, a fan-out copy or a drop
// has replaced it — and leaves it with the caller on error. Recv hands
// the caller one reference. An engine that rewrites an envelope derives
// the new message from the old one (msg.Msg.Derive) and releases the old
// one; it never modifies a message it was given. Messages an engine
// discards (filter misses, stale replies, drops) are released.
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
	"context"
	"sync/atomic"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// This file holds the Engine contract and the shared engine plumbing: the
// metric counters struct, the readiness gates and the broadcast helper. The
// two large reusable primitives live in sibling files in this package:
// LoadBalancer in loadbalancer.go, FairQueue in fairqueue.go. The simple
// combinator engines (PUSH/PULL/DEALER) are in combinators.go.

// counters holds the standard per-engine metric counters. Engines embed it so
// the promoted sent/recv/dropped fields work in place (e.counters.sent ==
// e.sent), and snapshot builds the Metrics. dropped and queueDepth
// are passed explicitly because they usually combine an engine-local counter
// with a FairQueue gauge.
type counters struct {
	sent    atomic.Uint64
	recv    atomic.Uint64
	dropped atomic.Uint64
}

func (c *counters) snapshot(dropped, queueDepth uint64) Metrics {
	return Metrics{
		Sent:       c.sent.Load(),
		Recv:       c.recv.Load(),
		Dropped:    dropped,
		QueueDepth: queueDepth,
	}
}

// prependFrames returns a new Msg holding head followed by body's frames
// (body may be nil), derived from body so it keeps body's lent frames
// alive on its own reference; body is untouched and still needs its own
// Release. The frame list is assembled in a stack buffer, so for envelopes
// of typical depth the message's copy is the only allocation of a frame
// slice.
func prependFrames(head [][]byte, body *msg.Msg) *msg.Msg {
	var buf [8][]byte
	frames := append(buf[:0], head...)
	if body == nil {
		return msg.FromFrames(frames...)
	}
	return body.Derive(append(frames, body.Frames()...)...)
}

// gates is an engine's readiness: two cap-1 wake-up tokens, in for PollIn
// and out for PollOut. The Poller parks on them, and so do the engine's own
// blocked receivers where it has a FairQueue (which takes the in-gate). An
// engine arms a gate whenever that direction may have become ready; a token
// is only a hint, so a stale one costs a waiter one spurious re-check. A
// waiter that takes the token and finds the direction ready puts it back,
// since another waiter on the same socket may be parked on the same gate.
type gates struct{ in, out chan struct{} }

func newGates() gates {
	return gates{in: make(chan struct{}, 1), out: make(chan struct{}, 1)}
}

// Gates implements Engine.
func (g gates) Gates() (in, out chan struct{}) { return g.in, g.out }

func (g gates) armIn()  { Arm(g.in) }
func (g gates) armOut() { Arm(g.out) }

// Arm puts the token in gate unless it is already there. The Poller uses
// it to re-arm the gates it reports.
func Arm(gate chan struct{}) {
	select {
	case gate <- struct{}{}:
	default:
	}
}

// awaitPeer parks until notify fires (nil: re-check the peer set), the
// engine closes (zmqerr.ErrSocketClosed) or ctx ends (ctx.Err()). It is the
// shared wait of every engine whose Send or Recv has no peer yet; closed
// may be nil for an engine whose close is signalled through notify.
func awaitPeer(ctx context.Context, notify, closed <-chan struct{}) error {
	select {
	case <-notify:
		return nil
	case <-closed:
		return zmqerr.ErrSocketClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// broadcast wakes every waiter at once: wait returns the current
// generation's channel and wake closes it, so waiters that hold it re-check
// while later ones start a new generation. (A cap-1 token loses wakeups
// here: one waiter's drain can swallow the token another needed.) The zero
// value is ready; the owner's mutex guards it.
type broadcast struct{ ch chan struct{} }

func (b *broadcast) wait() <-chan struct{} {
	if b.ch == nil {
		b.ch = make(chan struct{})
	}
	return b.ch
}

func (b *broadcast) wake() {
	if b.ch != nil {
		close(b.ch)
		b.ch = nil
	}
}

// Engine encapsulates the per-type policy (REQ lock-step, PUB fanout,
// DEALER round-robin, etc.) on top of a set of pipes. One engine per
// Socket; the public gozmq.Socket is a thin front for an Engine.
type Engine interface {
	// AttachPipe registers a new peer pipe (post-handshake). Engines with
	// subscription state (SUB, XPUB) may push pending SUBSCRIBE commands
	// into the pipe at attach time.
	AttachPipe(p *pipe.End) error

	// DetachPipe is called by the socket layer when a peer disconnects.
	// Engines must stop dispatching to p; any in-flight state (e.g. a
	// ROUTER's outstanding reply envelope) is resolved per pattern policy.
	DetachPipe(p *pipe.End) error

	// Send is the engine-level SendMsg entry. Pattern-specific:
	//   PUSH:    load-balance
	//   PUB:     fanout filtered by subscription
	//   ROUTER:  route by leading identity frame
	//   REQ:     fail with ErrFSMViolation unless Recv was the last call
	Send(ctx context.Context, m *msg.Msg) error

	// Recv is the engine-level RecvMsg entry. Pattern-specific:
	//   PULL:    fair-queue across pipes
	//   SUB:     fair-queue with subscription filter
	//   ROUTER:  prepend peer identity frame
	//   REP:     fail with ErrFSMViolation unless Send was the last call
	Recv(ctx context.Context) (*msg.Msg, error)

	// Close releases engine state, unblocks pending Send/Recv with
	// ErrSocketClosed, and detaches all pipes.
	Close() error

	// Metrics returns a point-in-time snapshot of the engine's counters
	// and the current inbound queue-depth gauge. Safe to call from any
	// goroutine, including concurrently with Send/Recv.
	Metrics() Metrics

	// Gates returns the engine's two readiness gates (see gates), stable
	// for its lifetime: in is armed when a Recv may have stopped blocking,
	// out when a Send may have. The Poller parks on them and re-checks
	// with CanRecvNow/CanSendNow, which answer false for a direction the
	// engine lacks.
	Gates() (in, out chan struct{})
	// CanRecvNow is a non-consuming level check: true iff a Recv would
	// not block on inbound queue emptiness right now. A false answer also
	// registers a wake-up: the pipes it found empty are marked idle, so
	// the next message arms the in-gate. Always false for send-only
	// engines.
	CanRecvNow() bool
	// CanSendNow is a non-consuming level check: true iff a Send would
	// not block on peer-set emptiness right now. HWM blocking is not
	// surfaced, as in libzmq where POLLOUT means "a peer exists", not
	// "the next message is guaranteed to land instantly". Always false
	// for recv-only engines.
	CanSendNow() bool
}

// Metrics is a point-in-time snapshot of per-engine counters and gauges;
// the root package copies it into the public SocketMetrics. Counters
// (Sent, Recv, Dropped) are monotonically increasing; QueueDepth is the
// number of messages queued in the engine's inbound pipes when sampled
// (zero for send-only patterns).
//
// Dropped covers messages the engine refuses to deliver after the
// caller's Send returned successfully (PUB fanout at HWM, a ROUTER route
// miss); send-side errors returned to the caller are not counted.
type Metrics struct {
	Sent       uint64
	Recv       uint64
	Dropped    uint64
	QueueDepth uint64
}
