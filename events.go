package gozmq

import "time"

// EventType classifies lifecycle events emitted on a socket's monitor channel.
// The value set matches the ZMTP monitor spec; new kinds may be appended, so
// consumers should handle unknown values gracefully. EventType is a bitmask
// so consumers can subscribe to a subset of event kinds via [WithMonitor]
// or [Socket.Monitor].
type EventType uint32

const (
	// EventConnected fires on a client-side socket after a dial plus
	// ZMTP handshake completes and the session's pipe is attached to
	// the engine. Event.Endpoint is the configured connect target.
	EventConnected EventType = 1 << iota

	// EventConnectDelayed fires on the first reconnect attempt following
	// a dial or handshake failure. Event.Endpoint is the target;
	// Event.Reason carries the pending backoff duration in string form.
	EventConnectDelayed

	// EventConnectRetried fires on the second and subsequent reconnect
	// attempts, distinguishing ongoing trouble from first trouble.
	// Event.Endpoint is the target; Event.Reason carries the pending
	// backoff duration.
	EventConnectRetried

	// EventConnectFailed fires when Connect fails terminally with no
	// reconnect loop to fall back on: an unknown transport scheme, or a
	// STREAM dial failure (STREAM dials exactly once). Event.Endpoint is
	// the requested target; Event.Reason is the underlying error string.
	EventConnectFailed

	// EventListening fires after a successful Bind with wildcards and
	// ephemeral ports resolved. Event.Endpoint is the resolved endpoint
	// (same value as Socket.LastEndpoint).
	EventListening

	// EventBindFailed fires when Bind cannot complete — unknown
	// transport scheme, invalid endpoint, or a net.Listen error.
	// Event.Endpoint is the requested (unresolved) endpoint;
	// Event.Reason is the underlying error string.
	EventBindFailed

	// EventAccepted fires on a server-side socket when a new peer
	// connection is accepted off the listener, before the ZMTP
	// handshake begins. Event.Endpoint is the peer's remote endpoint.
	EventAccepted

	// EventAcceptFailed fires when the accept loop exits due to a
	// listener error. Event.Endpoint is the bound endpoint;
	// Event.Reason carries the underlying error (e.g. a context
	// cancellation during shutdown, or a real network error).
	EventAcceptFailed

	// EventClosed fires once Socket.Close has completed its teardown
	// sequence. Event.Endpoint is the most recent bind endpoint (may
	// be empty for connect-only sockets).
	EventClosed

	// EventDisconnected fires when a previously-connected session is
	// torn down — peer closed, heartbeat timeout, or local
	// Socket.Close. Event.Endpoint is the remote peer endpoint.
	EventDisconnected

	// EventHandshakeSucceeded fires after the ZMTP greeting and
	// security handshake complete successfully on either side of a
	// session. Event.Endpoint is the connect target (client) or peer
	// remote (server). Event.Reason is empty.
	EventHandshakeSucceeded

	// EventHandshakeFailedProtocol fires when the handshake aborts due
	// to a protocol-level problem: unsupported ZMTP version, malformed
	// greeting or command, handshake timeout, or an unexpected peer
	// drop. Event.Reason carries the underlying error string.
	EventHandshakeFailedProtocol

	// EventHandshakeFailedAuth fires when the handshake aborts because
	// the security mechanism rejected the peer — PLAIN credentials
	// refused, CURVE box authentication failure, or ZAP denial.
	// Event.Reason carries the underlying error string.
	EventHandshakeFailedAuth

	// EventAll is the bitmask of every event type defined above. Pass
	// it as the mask argument to [WithMonitor] or [Socket.Monitor] to
	// receive every lifecycle event.
	EventAll EventType = 0xFFFFFFFF
)

// Event is a single lifecycle notification delivered on a monitor channel
// registered via [WithMonitor] or [Socket.Monitor]. All fields are
// populated for every event.
type Event struct {
	// Type is the event kind. Consumers should treat unknown values
	// defensively because new kinds may be added in minor versions.
	Type EventType

	// Endpoint is the endpoint string the event relates to. For
	// server-side events (Accepted, Disconnected on an accepted session)
	// this is the peer's remote endpoint; for client-side events it is
	// the configured connect target; for bind events it is the
	// resolved bind endpoint.
	Endpoint string

	// Time is the wall-clock timestamp at which the event was emitted.
	Time time.Time

	// Reason is a machine-readable code or error string explaining
	// failure events (EventBindFailed, EventAcceptFailed,
	// EventHandshakeFailed*, EventDisconnected when caused by an error,
	// EventConnectDelayed/Retried where it carries the pending backoff).
	// Empty for unambiguous success events such as EventConnected,
	// EventListening, and EventHandshakeSucceeded.
	Reason string
}
