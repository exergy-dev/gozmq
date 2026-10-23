package gozmq

import "github.com/exergy-dev/gozmq/internal/zmqerr"

// Sentinel errors returned across the public API. An error that reports a
// gozmq condition (closed socket, lock-step violation, unknown route,
// handshake or wire failure, bad endpoint) wraps one of these, so callers
// can dispatch with errors.Is instead of string-matching. Errors that
// originate elsewhere are not rewrapped:
//
//   - A blocking call whose context ends returns ctx.Err() as is
//     (context.Canceled or context.DeadlineExceeded).
//   - Operating-system and network errors from a transport — Bind on an
//     address in use, a failed STREAM dial — are returned as the underlying
//     error (typically a *net.OpError); use errors.As.
//   - Context.NewSocket and Proxy report invalid configuration (option
//     values and combinations, socket pairings) with plain descriptive
//     errors.
//   - A ZAP handler's rejection always wraps ErrAuthFailed, with the
//     handler's own error kept in the chain.
//
// Failures on an established session do not reach SendMsg or RecvMsg: the
// session is torn down and the error surfaces on the monitor channel.
//
// These are variable aliases for the authoritative values in
// internal/zmqerr (to break the import cycle where internal packages
// wrap sentinels and the public gozmq package imports those internal
// packages for wiring). errors.Is sees them as the same value.
var (
	// ErrContextClosed is returned by Context.NewSocket once the Context
	// has been closed. Context.Close closes every socket, so calls already
	// in flight on them see ErrSocketClosed (or, if they were passed
	// Context.Ctx, context.Canceled). Context.Close itself is idempotent
	// and returns nil on repeated calls.
	ErrContextClosed = zmqerr.ErrContextClosed

	// ErrSocketClosed is returned by the Socket methods that can fail
	// (SendMsg, RecvMsg, Bind, Connect, Unbind, Disconnect, Subscribe,
	// Monitor, ...) once the socket has been closed, including calls
	// blocked when Close runs. Close itself is idempotent and returns nil
	// on repeated calls, matching Context.Close.
	ErrSocketClosed = zmqerr.ErrSocketClosed

	// ErrFSMViolation is returned by socket methods called in the wrong
	// state for the socket pattern — e.g. REQ.Send when the socket is
	// awaiting a reply, REP.Recv when the socket is mid-response, a
	// concurrent call that would break REQ/REP lock-step,
	// Subscribe/Unsubscribe on a non-SUB socket, RecvBytes on a multipart
	// message, or SendMsg with a nil message.
	ErrFSMViolation = zmqerr.ErrFSMViolation

	// ErrHandshakeTimeout is returned (or surfaced on a monitor channel
	// as EventHandshakeFailedProtocol) when the ZMTP greeting plus
	// security handshake does not complete within the duration configured
	// by [WithHandshakeTimeout]. On client sockets the reconnector then
	// backs off and retries.
	ErrHandshakeTimeout = zmqerr.ErrHandshakeTimeout

	// ErrAuthFailed is reported when the security mechanism rejects a
	// peer — PLAIN credentials refused by the server's validator, CURVE
	// handshake boxes that fail to open, a server's ERROR command seen by
	// a client, or any ZAP handler rejection. Surfaces on
	// monitor channels as EventHandshakeFailedAuth.
	ErrAuthFailed = zmqerr.ErrAuthFailed

	// ErrMalformedFrame reports a frame that violates ZMTP framing rules
	// or exceeds [WithMaxMessageSize]. From the wire, the session is torn
	// down and the error surfaces via the monitor channel rather than
	// through a Send/Recv call. On the send side, SendMsg returns it
	// synchronously for a frame over WithMaxMessageSize, without touching
	// the session.
	ErrMalformedFrame = zmqerr.ErrMalformedFrame

	// ErrProtocolVersion is returned when the peer's greeting advertises
	// a ZMTP major version gozmq does not speak (anything other than 3).
	// Surfaces as EventHandshakeFailedProtocol on the monitor channel.
	ErrProtocolVersion = zmqerr.ErrProtocolVersion

	// ErrRouteUnknown is returned by a ROUTER send whose leading identity
	// frame matches no connected peer (including one that has just
	// disconnected) when [WithRouterMandatory] is enabled; without it the
	// message is dropped silently and counted in Metrics. A message with
	// no identity frame at all always returns it. STREAM sends always
	// behave as mandatory.
	ErrRouteUnknown = zmqerr.ErrRouteUnknown

	// ErrEndpointInvalid is returned by Bind or Connect when the
	// endpoint string is syntactically invalid for the chosen transport
	// (bad port, empty inproc name, malformed ipc path, etc.).
	ErrEndpointInvalid = zmqerr.ErrEndpointInvalid

	// ErrEndpointUnknown is returned by Disconnect when the endpoint
	// does not match any active Connect target, and by Unbind when it
	// does not match any active listener. Distinct from
	// ErrEndpointInvalid, which signals a syntactically bad endpoint.
	ErrEndpointUnknown = zmqerr.ErrEndpointUnknown

	// ErrTransportUnknown is returned by Bind or Connect when the
	// endpoint's scheme has no transport registered on the Context — for
	// example tls+tcp:// without a prior Context.RegisterTransport call
	// that installs a TLS transport.
	ErrTransportUnknown = zmqerr.ErrTransportUnknown

	// ErrNonceExhausted is returned by a CURVE-secured session when its
	// per-direction short-nonce counter would wrap. RFC 26 defines
	// uint64 counters starting at 1 and expects rekeying long before
	// wrap; gozmq does not implement rekey, so the session is torn down
	// cleanly with this sentinel, which is logged through [WithLogger] (the
	// EventDisconnected it causes carries no reason). Operators seeing it
	// should rotate keys and/or reconnect.
	ErrNonceExhausted = zmqerr.ErrNonceExhausted
)
