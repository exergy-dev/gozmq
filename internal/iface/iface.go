// Package iface holds the inter-layer contracts of gozmq.
package iface

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/rkm/gozmq/internal/msg"
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

// Transport turns an endpoint string into dial/listen operations for a
// single scheme (tcp, ipc, inproc, tls+tcp).
type Transport interface {
	// Scheme reports the URL scheme this transport handles, without the "://"
	// suffix (e.g. "tcp", "inproc").
	Scheme() string

	// Dial connects to endpoint and returns a Conn ready for ZMTP framing.
	// Respects ctx cancellation and deadline.
	Dial(ctx context.Context, endpoint string) (Conn, error)

	// Listen binds to endpoint and returns a Listener. For wildcard
	// endpoints (e.g. "tcp://*:0") the returned Listener's Endpoint()
	// reports the resolved address.
	Listen(ctx context.Context, endpoint string) (Listener, error)
}

// Conn is a single ZMTP connection underneath the session layer. Read and
// Write deliver raw bytes; the session layer applies framing and security
// codecs on top.
type Conn interface {
	io.Reader
	io.Writer
	io.Closer

	// LocalEndpoint returns the local side in ZMQ string form (e.g.
	// "tcp://127.0.0.1:5555"). May be empty for inproc.
	LocalEndpoint() string
	// RemoteEndpoint returns the peer in ZMQ string form.
	RemoteEndpoint() string

	// SetDeadline matches net.Conn's semantic.
	SetDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

// Listener accepts incoming ZMTP connections on a bound endpoint.
type Listener interface {
	// Accept blocks until an incoming connection arrives or ctx cancels.
	Accept(ctx context.Context) (Conn, error)
	// Endpoint returns the actual bound endpoint (wildcards resolved).
	Endpoint() string
	// Close stops accepting and unblocks in-flight Accept calls.
	Close() error
}

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
	// be returned via msg.PoolPut once no longer referenced.
	ReadFrame(r io.Reader, dst []byte) (flags uint8, body []byte, err error)

	// WriteFrame writes flags + length + body as one ZMTP frame. Multi-byte
	// lengths are big-endian per the spec. Implementations may internally
	// batch via net.Buffers; callers should not assume one write = one
	// socket operation.
	WriteFrame(w io.Writer, flags uint8, body []byte) error
}

// FrameBufferAppender is an optional fast path for codecs that can append
// one fully-encoded frame directly into a net.Buffers batch without first
// materializing a contiguous header+body blob. Callers must invoke the
// returned release hook after the batch has been written (or abandoned).
type FrameBufferAppender interface {
	AppendFrameBuffers(dst *net.Buffers, flags uint8, body []byte) (release func(), frameBytes int, err error)
}

// MetadataCodec encodes and decodes the ZMTP READY property list (ZMTP 3.1
// §2.3.1). Used by NULL, PLAIN, and CURVE — hence a dedicated interface.
type MetadataCodec interface {
	Encode(props map[string]string) []byte
	Decode(raw []byte) (map[string]string, error)
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

// Pipe is one end of a bidirectional bounded message queue. A Session and
// a SocketEngine sit on opposite ends; a Send on one end surfaces as a
// Recv on the other. Each direction has its own high-water mark.
type Pipe interface {
	// Send enqueues m for the other end. Blocks until space is available or
	// ctx cancels. Returns ErrHWM is NOT used by Pipe directly — HWM
	// behavior is expressed as a blocking Send; non-blocking behavior is
	// achieved by passing a ctx with zero deadline.
	Send(ctx context.Context, m *msg.Msg) error

	// Recv dequeues a message posted by the other end. Blocks until one is
	// available or ctx cancels.
	Recv(ctx context.Context) (*msg.Msg, error)

	// Close terminates this end of the pipe. Messages already in flight may
	// still be delivered; the other end sees io.EOF on subsequent Recv.
	Close() error

	// Closed returns a channel that closes when either end has been Closed.
	Closed() <-chan struct{}

	// SendHWM reports the configured outbound high-water mark (messages).
	SendHWM() int
	// RecvHWM reports the configured inbound high-water mark.
	RecvHWM() int

	// PeerEndpoint reports the peer's ZMQ endpoint string, if any. Empty
	// for inproc and during pre-handshake windows.
	PeerEndpoint() string
	// PeerIdentity returns the ZMTP Identity advertised by the peer in its
	// READY metadata. Empty when the peer did not set one.
	PeerIdentity() []byte
}

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

	// Endpoint returns the peer's ZMQ endpoint string.
	Endpoint() string

	// Role reports whether this session was initiated by Connect (Client)
	// or Accept (Server).
	Role() Role

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
}

// --------------------------------------------------------------------------
// Transport registry
// --------------------------------------------------------------------------

// Registry resolves an endpoint string to its Transport.
type Registry interface {
	// Lookup returns the Transport handling endpoint's scheme.
	Lookup(endpoint string) (Transport, error)
	// Register installs t for its declared Scheme. Overwrites any existing
	// entry for the same scheme.
	Register(t Transport)
}
