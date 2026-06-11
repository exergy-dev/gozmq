package gozmq

import "github.com/rkm/gozmq/internal/zmqerr"

// Sentinel errors returned across the public API. All errors returned from
// gozmq wrap one of these via errors.Is so callers can dispatch without
// string-matching.
//
// These are variable aliases for the authoritative values in
// internal/zmqerr (to break the import cycle where internal packages
// wrap sentinels and the public gozmq package imports those internal
// packages for wiring). errors.Is sees them as the same value.
var (
	// ErrContextClosed is returned by Context methods after the Context has
	// already been closed, and by any in-flight operation whose root Context
	// was cancelled by Context.Close. Context.Close itself is idempotent and
	// returns nil on repeated calls.
	ErrContextClosed = zmqerr.ErrContextClosed

	// ErrSocketClosed is returned by every Socket method (Send, Recv,
	// Bind, Connect, Disconnect, Subscribe, Monitor) once the socket has
	// been closed. Close itself is idempotent and returns nil on repeated
	// calls, matching Context.Close.
	ErrSocketClosed = zmqerr.ErrSocketClosed

	// ErrHWM is returned by Send on patterns that surface high-water-mark
	// pressure as an error rather than by blocking — notably drop-or-
	// return patterns and non-blocking send paths once the outbound pipe
	// for the target peer is full.
	ErrHWM = zmqerr.ErrHWM

	// ErrFSMViolation is returned by socket methods called in the wrong
	// state for the socket pattern — e.g. REQ.Send when the socket is
	// awaiting a reply, REP.Recv when the socket is mid-response,
	// Subscribe/Unsubscribe on a non-SUB socket.
	ErrFSMViolation = zmqerr.ErrFSMViolation

	// ErrHandshakeTimeout is returned (or surfaced on a monitor channel
	// as EventHandshakeFailedProtocol) when the ZMTP greeting plus
	// security handshake does not complete within the duration configured
	// by [WithHandshakeTimeout]. On client sockets the reconnector then
	// backs off and retries.
	ErrHandshakeTimeout = zmqerr.ErrHandshakeTimeout

	// ErrAuthFailed is returned when the security mechanism rejects a
	// peer — PLAIN credentials refused by the server's validator, CURVE
	// handshake boxes that fail to open, or an unauthorized public key
	// via ZAP. Surfaces on monitor channels as EventHandshakeFailedAuth.
	ErrAuthFailed = zmqerr.ErrAuthFailed

	// ErrMalformedFrame is returned when a frame arriving from the wire
	// violates ZMTP framing rules or exceeds [WithMaxMessageSize]. The
	// session is torn down; the error surfaces via the monitor channel
	// rather than through a synchronous Send/Recv call.
	ErrMalformedFrame = zmqerr.ErrMalformedFrame

	// ErrProtocolVersion is returned when the peer's greeting advertises
	// a ZMTP major version gozmq does not speak (anything other than 3).
	// Surfaces as EventHandshakeFailedProtocol on the monitor channel.
	ErrProtocolVersion = zmqerr.ErrProtocolVersion

	// ErrRouteUnknown is returned by ROUTER.Send when the leading
	// identity frame does not match any connected peer AND
	// [WithRouterMandatory] is enabled. Without the mandatory flag the
	// message is silently dropped and no error is returned.
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
	// cleanly with this sentinel. Operators detecting this in logs or
	// monitor events should rotate keys and/or reconnect.
	ErrNonceExhausted = zmqerr.ErrNonceExhausted
)
