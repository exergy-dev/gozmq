// Package iface holds the inter-layer contracts of gozmq.
package iface

import (
	"context"

	"github.com/exergy-dev/gozmq/internal/pipeiface"
	"github.com/exergy-dev/gozmq/internal/zmtp"
	"github.com/exergy-dev/gozmq/msg"
	"github.com/exergy-dev/gozmq/security"
	"github.com/exergy-dev/gozmq/transport"
)

// --------------------------------------------------------------------------
// Transport layer
// --------------------------------------------------------------------------

// The transport-layer contracts are defined in the public transport package
// so that pkg.go.dev documents them and out-of-tree transports can implement
// them (see (*gozmq.Context).RegisterTransport). They are aliased here so the
// internal layers keep referring to them as iface.Transport / iface.Conn /
// iface.Listener.
type (
	// Transport is an alias for [transport.Transport].
	Transport = transport.Transport
	// Conn is an alias for [transport.Conn].
	Conn = transport.Conn
	// Listener is an alias for [transport.Listener].
	Listener = transport.Listener
)

// --------------------------------------------------------------------------
// Direct-pipe (inproc bypass) layer
// --------------------------------------------------------------------------

// The direct-pipe contracts live in internal/pipeiface, a leaf package the
// transport package can import without a cycle. Aliased here so the internal
// layers keep referring to them as iface.DirectPipe*.
type (
	// DirectPipeOffer is an alias for [pipeiface.DirectPipeOffer].
	DirectPipeOffer = pipeiface.DirectPipeOffer
	// DirectPipeAcceptor is an alias for [pipeiface.DirectPipeAcceptor].
	DirectPipeAcceptor = pipeiface.DirectPipeAcceptor
	// DirectPipeDialer is an alias for [pipeiface.DirectPipeDialer].
	DirectPipeDialer = pipeiface.DirectPipeDialer
)

// --------------------------------------------------------------------------
// Framing layer
// --------------------------------------------------------------------------

// The framing contracts are defined in internal/zmtp, the package that
// implements them, so that zmtp does not have to depend on this one.
// Aliased here so the session layer keeps referring to them as
// iface.FrameCodec / iface.Flag*.
type (
	// FrameCodec is an alias for [zmtp.FrameCodec].
	FrameCodec = zmtp.FrameCodec
	// FrameHeaderAppender is an alias for [zmtp.FrameHeaderAppender].
	FrameHeaderAppender = zmtp.FrameHeaderAppender
)

// ZMTP frame flag bits (ZMTP 3.1 §2.1), aliased from internal/zmtp.
const (
	FlagMore    = zmtp.FlagMore
	FlagLong    = zmtp.FlagLong
	FlagCommand = zmtp.FlagCommand
)

// --------------------------------------------------------------------------
// Security layer
// --------------------------------------------------------------------------

// The security contracts are defined in the public security package, which
// implements them, so that pkg.go.dev documents them and out-of-tree
// mechanisms can be written. Aliased here so the session layer keeps
// referring to them as iface.SecurityMech / iface.Role.
type (
	// SecurityMech is an alias for [security.Mech].
	SecurityMech = security.Mech
	// ZAPAuthorizer is an alias for [security.ZAPAuthorizer].
	ZAPAuthorizer = security.ZAPAuthorizer
	// ZapConfigurable is an alias for [security.ZapConfigurable].
	ZapConfigurable = security.ZapConfigurable
	// Role is an alias for [security.Role].
	Role = security.Role
)

// Role constants, aliased from the security package.
const (
	RoleClient = security.RoleClient
	RoleServer = security.RoleServer
)

// --------------------------------------------------------------------------
// Pipe layer
// --------------------------------------------------------------------------

// Pipe is an alias for [pipeiface.Pipe] — one end of a bidirectional
// bounded message queue. A Session and a SocketEngine sit on opposite ends;
// a Send on one end surfaces as a Recv on the other.
type Pipe = pipeiface.Pipe

// --------------------------------------------------------------------------
// Session layer
// --------------------------------------------------------------------------

// Session owns the ZMTP state machine for a single peer connection: it
// drives greeting, handshake, heartbeats, and the traffic phase. It sits
// between a Conn (bytes) and a Pipe (messages).
type Session interface {
	// Start launches the session's reader/writer goroutines. Returns
	// immediately; handshake progress is observable via Handshaked.
	Start()

	// Stop terminates the session, closes the underlying Conn, and blocks
	// until internal goroutines exit.
	Stop() error

	// Handshaked returns a channel closed when the handshake succeeds.
	Handshaked() <-chan struct{}

	// HandshakeErr returns a non-nil error if the handshake failed. Read
	// after Handshaked closes.
	HandshakeErr() error

	// Pipe returns the engine-facing end of the session's pipe. Valid only
	// after Handshaked closes without error.
	Pipe() Pipe

	// Done returns a channel that closes when every session goroutine
	// (reader, writer, optional heartbeat) has exited.
	Done() <-chan struct{}

	// WriterFlushed returns a channel that closes when the writer goroutine
	// has exited — all outbound messages have been flushed to conn.
	WriterFlushed() <-chan struct{}

	// CancelWriter cancels the writer's blocking Recv, causing it to drain
	// remaining queued messages non-blocking then exit. Does NOT close the
	// conn, so in-flight conn.Write calls complete. Call engine.Close before
	// CancelWriter to prevent new messages entering the queue, then wait
	// WriterFlushed, then call Stop.
	CancelWriter()
}

// --------------------------------------------------------------------------
// Socket engine layer
// --------------------------------------------------------------------------

// SocketMetrics is a point-in-time snapshot of per-socket counters and
// gauges. Counters (Sent, Recv, Dropped) are monotonically increasing
// uint64s read atomically; QueueDepth is sampled at the moment Metrics
// is called and reflects the number of messages buffered in the engine's
// inbound handoff at that instant (zero for send-only patterns).
//
// "Dropped" covers messages the engine refuses to deliver after the
// caller's Send returned successfully:
//   - PUB fanout drops when a peer's pipe is at HWM (per-message,
//     not per-peer).
//   - FairQueue forwarder drops when its context cancels (Detach) or
//     the engine closes while a message is mid-handoff.
//
// Send-side errors returned to the caller do NOT increment Dropped —
// the caller already observed the failure.
type SocketMetrics struct {
	Sent       uint64 // messages successfully sent
	Recv       uint64 // messages successfully received
	Dropped    uint64 // messages dropped (PUB HWM, fairqueue ctx-cancel/close)
	QueueDepth uint64 // current inbound queue depth (gauge)
}

// SocketEngine encapsulates the per-type policy (REQ lock-step, PUB fanout,
// DEALER round-robin, etc.) on top of a set of Pipes. One engine per
// Socket. The public gozmq.Socket is a thin front for a SocketEngine.
type SocketEngine interface {
	// AttachPipe registers a new peer pipe (post-handshake). Engines with
	// subscription state (SUB, XPUB) may push pending SUBSCRIBE commands
	// into the pipe at attach time.
	AttachPipe(p Pipe) error

	// DetachPipe is called by the session layer when a peer disconnects.
	// Engines must stop dispatching to p; any in-flight state (e.g. a
	// ROUTER's outstanding reply envelope) is resolved per pattern policy.
	DetachPipe(p Pipe) error

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
	Metrics() SocketMetrics
}
