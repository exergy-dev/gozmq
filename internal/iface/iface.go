// Package iface holds the inter-layer contracts of gozmq.
package iface

import (
	"context"
	"io"

	"github.com/exergy-dev/gozmq/internal/pipeiface"
	"github.com/exergy-dev/gozmq/msg"
	"github.com/exergy-dev/gozmq/transport"
)

// Role distinguishes the initiator (client, from Connect) from the acceptor
// (server, from Bind) within a single session. Some security mechanisms and
// socket patterns branch on role.
type Role uint8

const (
	RoleClient Role = iota
	RoleServer
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

// ZMTP frame flag bits (ZMTP 3.1 §2.1).
const (
	FlagMore    uint8 = 0x01
	FlagLong    uint8 = 0x02
	FlagCommand uint8 = 0x04
)

// FrameCodec encodes and decodes ZMTP frames. Implementations must be
// goroutine-safe only to the extent that concurrent Read/Write operate on
// disjoint io.Reader/io.Writer pairs; a single codec value is typically
// shared across sessions without synchronization.
type FrameCodec interface {
	// ReadFrame reads one frame from r. Returns the flags byte and a body
	// slice. If dst has sufficient capacity the body aliases dst[:n];
	// otherwise the body is drawn from the size-tiered frame pool and may
	// be returned to the internal buffer pool once no longer referenced.
	ReadFrame(r io.Reader, dst []byte) (flags uint8, body []byte, err error)

	// WriteFrame writes flags + length + body as one ZMTP frame. Multi-byte
	// lengths are big-endian per the spec. Implementations may internally
	// batch via net.Buffers; callers should not assume one write = one
	// socket operation.
	WriteFrame(w io.Writer, flags uint8, body []byte) error
}

// FrameHeaderAppender is the allocation-free fast path used by the writer
// batching loop. It appends the wire-format frame header (flags + length)
// for a body of len(body) bytes onto scratch and returns the grown scratch.
// Callers materialize net.Buffers entries from scratch + body slices after
// the entire batch has been encoded — by that point scratch's underlying
// array is stable, so subslices referencing scratch[off:off+hdrLen] do not
// race with further appends.
//
// This method allocates no closures and uses no internal sync.Pool: header
// bytes live in caller-owned scratch storage that the caller recycles
// across batches.
type FrameHeaderAppender interface {
	AppendFrameHeader(scratch []byte, flags uint8, body []byte) (newScratch []byte, headerLen int, err error)
}

// --------------------------------------------------------------------------
// Security layer
// --------------------------------------------------------------------------

// ZAPAuthorizer is the server-side authorization hook for mechanisms that
// delegate policy to RFC 27 ZAP. On success it returns a user-id plus
// optional metadata to merge into the peer properties. On rejection it
// should return an error wrapping zmqerr.ErrAuthFailed.
type ZAPAuthorizer func(domain, address, mech string, credentials [][]byte) (userID string, metadata map[string]string, err error)

// ZapConfigurable is an optional extension implemented by security
// mechanisms that support server-side ZAP authorization.
type ZapConfigurable interface {
	ConfigureZAP(authorizer ZAPAuthorizer, domain, address string)
}

// SecurityMech drives a single handshake exchange and, for mechanisms that
// require it (CURVE), wraps/unwraps every subsequent frame body.
//
// The handshake is modeled as a lock-step NextHandshake loop. The session
// layer drives it:
//
//	out, done, err := mech.NextHandshake(nil)              // seed
//	for !done {
//	    write(out); in := read()
//	    out, done, err = mech.NextHandshake(in)
//	}
//
// For NULL the handshake is a single READY exchange; for CURVE it is the
// five-step HELLO/WELCOME/INITIATE/READY/MESSAGE sequence.
type SecurityMech interface {
	// Name is the greeting mechanism token (e.g. "NULL", "PLAIN", "CURVE"),
	// unpadded; the greeting encoder pads to 20 bytes.
	Name() string

	// AsServer reports the value of the greeting as-server byte.
	AsServer() bool

	// NextHandshake advances the handshake. On the first call, in is nil.
	// Returns the next outbound command (may be empty on final step) and
	// done=true when the handshake is complete.
	NextHandshake(in []byte) (out []byte, done bool, err error)

	// EncodeFrame rewrites a plaintext frame body and its ZMTP flags into
	// their wire form. NULL/PLAIN preserve both body and flags. CURVE moves
	// multipart state into the encrypted MESSAGE inner flags byte while
	// preserving outer flags such as COMMAND.
	EncodeFrame(flags uint8, plain []byte) (wireFlags uint8, wire []byte, err error)

	// DecodeFrame reverses EncodeFrame. The returned flags are the logical
	// post-decode frame flags seen by the session layer. Returns
	// ErrMalformedFrame on MAC failure.
	DecodeFrame(flags uint8, wire []byte) (plainFlags uint8, plain []byte, err error)

	// Metadata returns local properties sent in READY. Must return after
	// the handshake is seeded.
	Metadata() map[string]string

	// SetPeerMetadata is called by the session layer once the peer's READY
	// has been parsed. Mechanisms may reject (return error) on mismatch
	// (e.g. Socket-Type incompatibility).
	SetPeerMetadata(props map[string]string) error
}

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
