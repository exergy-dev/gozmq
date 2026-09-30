// Package gozmq is a pure-Go implementation of the ZeroMQ messaging patterns
// and the ZMTP 3.1 wire protocol. It has no CGO dependency and builds cleanly
// with CGO_ENABLED=0 on every platform the Go toolchain supports.
//
// # Overview
//
// gozmq provides the standard ZeroMQ socket patterns (REQ/REP, DEALER/ROUTER,
// PUB/SUB, XPUB/XSUB, PUSH/PULL, PAIR, STREAM) wired to the usual transports
// (tcp://, tcp6://, ipc://, inproc://, and the library-specific tls+tcp://
// for TLS-wrapped TCP). Wire compatibility with libzmq >= 4.2 is a first-class
// goal: a gozmq socket interoperates bit-for-bit with libzmq over ZMTP 3.1
// and negotiates down to 3.0 when the peer predates heartbeats.
// Peers whose advertised Socket-Type is incompatible with the local socket
// are rejected during the handshake before traffic phase begins.
//
// See design.md at the repository root for architecture, protocol, security,
// and testing specifications. The public API is stable only after the v1.0.0
// tag; until then, breaking changes are noted in CHANGELOG.md.
//
// # Hello world
//
// A minimal PAIR round-trip over inproc:
//
//	ctx := gozmq.NewContext()
//	defer ctx.Close()
//
//	server, _ := ctx.NewSocket(gozmq.Pair)
//	defer server.Close()
//	_ = server.Bind("inproc://hello")
//
//	client, _ := ctx.NewSocket(gozmq.Pair)
//	defer client.Close()
//	_ = client.Connect("inproc://hello")
//
//	callCtx, cancel := context.WithTimeout(context.Background(), time.Second)
//	defer cancel()
//
//	_ = client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("ping")))
//	msg, _ := server.RecvMsg(callCtx)
//	fmt.Printf("%s\n", msg.Bytes()) // "ping"
//
// Received frames are ordinary Go memory owned by the caller; there is
// nothing to release ((*Msg).Release does nothing for them).
//
// # Receive buffer pool
//
// A socket created with [WithRecvBufferPool](true) receives frames larger
// than 4 KiB into pooled buffers, which avoids a large allocation and its
// garbage-collection cost per frame. Those messages are lent ([Msg.Lent]),
// with an explicit contract: the receiver owns one reference and calls
// [Msg.Release] when done; the frames are valid until the last reference
// is released and are then reused; [Msg.Retain] adds a reference; releasing
// more than retained panics. [Socket.SendMsg] takes its own reference, so a
// forwarder can RecvMsg, SendMsg and Release in that order. To re-envelope a
// lent message, use [Msg.Derive] rather than wrapping its frames in a new
// message. There is no finalizer: a message dropped without Release is
// simply garbage collected, never recycled under a live frame.
//
// # Security
//
// The default security mechanism is NULL: no authentication, no encryption.
// This matches libzmq and is appropriate for trusted networks (inproc, ipc,
// or a private tcp segment) but NOT for untrusted or public networks. To
// authenticate and/or encrypt, choose one of:
//
//   - [WithPlainAuth] / [WithPlainClient] — username/password authentication.
//     Credentials travel in cleartext unless the underlying transport is
//     wrapped in tls+tcp://. Server-side PLAIN and CURVE authorization can
//     also be delegated to an RFC 27 handler via [WithZAPHandler] plus the
//     per-socket [WithZAPDomain].
//   - [WithCurveServer] / [WithCurveClient] — CurveZMQ mutual authentication
//     with forward-secret encryption using long-term Curve25519 keys.
//
// For untrusted networks prefer CURVE, or layer PLAIN under tls+tcp://.
//
// Note that inproc connections between NULL-mechanism sockets in the same
// Context use a direct-pipe bypass that skips the session layer entirely —
// including the ZAP authorization hook. This matches libzmq, which also
// performs no ZAP check on inproc, but it means a Context-level ZAP
// handler does not see inproc peers.
//
// # Defaults
//
// Sockets constructed via [Context.NewSocket] start with these defaults. Each
// is overridable via the corresponding With* option ([WithSendHWM],
// [WithRecvHWM], [WithLinger], [WithHeartbeat], [WithHandshakeTimeout],
// [WithMaxMessageSize], [WithReconnectInterval], [WithReconnectIntervalMax]);
// see each option's documentation for the full semantics.
//
//	sendHWM              1000         (outbound high-water mark, messages)
//	recvHWM              1000         (inbound  high-water mark, messages)
//	linger               1s           (Close drain grace period)
//	heartbeatInterval    30s          (ZMTP 3.1 PING cadence)
//	heartbeatTimeout     90s          (peer-unresponsive deadline)
//	handshakeTimeout     30s          (greeting + security handshake cap)
//	maxMessageSize       64 MiB       (per-message ceiling; oversize tears session)
//	reconnectInterval    100ms        (base backoff; exponential with 20% jitter)
//	reconnectIntervalMax 30s          (backoff cap; 0 = no cap, diverges from libzmq)
//
// These differ from libzmq where libzmq's defaults are hostile: notably,
// linger defaults to 1s (not infinite) so a forgotten Close cannot hang the
// process indefinitely.
//
// # STREAM sockets
//
// The STREAM socket type is a deliberate departure from the rest of the ZMTP
// patterns and is best thought of as a raw-TCP bridge dressed up as a ZeroMQ
// socket. Specifically, STREAM:
//
//   - carries raw bytes on the wire — there is no ZMTP frame envelope, so
//     every byte sent or received corresponds 1:1 to TCP payload bytes;
//   - does NOT perform a ZMTP greeting or security handshake with the peer,
//     so it interoperates with any TCP server or client (HTTP, line-based
//     protocols, opaque binary streams);
//   - synthesises a per-connection routing identity. RecvMsg returns a two-
//     frame message whose leading frame is the connection identity and whose
//     second frame is the raw payload; SendMsg requires the same connection
//     identity as the leading frame to address a specific peer (an empty
//     payload closes that connection);
//   - has no subscription, topic, or pub/sub semantics — WithSubscribe and
//     XPUB verbose-mode options do not apply;
//   - dials exactly once on Connect and never reconnects: a dial failure
//     is returned synchronously (plus [EventConnectFailed] on the monitor
//     channel) and a later peer disconnect is final. Reconnect supervision
//     for raw STREAM peers is the application's responsibility;
//   - diverges from libzmq in NOT delivering synthetic [identity, empty]
//     connect/disconnect notification messages (libzmq's ZMQ_STREAM_NOTIFY
//     behavior). Use a monitor channel to observe STREAM peer lifecycle.
//
// Options that configure ZMTP-layer behavior (security, heartbeat,
// identity, handshake timeout, ZAP domain) are rejected by NewSocket on
// STREAM sockets rather than silently ignored.
//
// # Concurrency model
//
// Socket.Send and Socket.Recv are safe for concurrent use across goroutines;
// the engine serializes them internally. However, the idiomatic usage — and
// the pattern the library is optimised for — is one goroutine per socket.
// Sharing a socket across many concurrent senders works but buys you nothing
// over a dedicated fan-in goroutine and makes flow-control reasoning harder.
//
// # Polling
//
// [Poller] multiplexes blocking readiness across many sockets, in the
// spirit of zmq_poll: register sockets with [PollIn] / [PollOut] interest
// masks, then call [Poller.Poll] to block until at least one is ready.
// Readiness is non-consuming — the caller still invokes RecvMsg/SendMsg
// on the returned sockets. One divergence from libzmq: [PollOut] reports
// only that at least one peer is attached to dispatch to; it does NOT
// account for high-water-mark pressure, so a SendMsg after a PollOut
// wakeup may still block if every peer pipe is full. Event loops ported
// from zmq_poll should pair PollOut sends with a deadline or context.
//
// # Close semantics
//
// Socket.Close blocks for up to the configured linger duration while the
// session writers flush any queued outbound messages to the wire. linger == 0
// discards queued messages immediately; a negative linger waits indefinitely
// (matching libzmq) — this can deadlock Close if a peer never drains, so
// pair infinite linger with [Socket.CloseCtx], which honors its context's
// cancellation as an escape hatch: when the context fires mid-drain, the
// drain is abandoned but teardown completes and the socket is fully
// closed on return. Close and CloseCtx are idempotent — repeated calls
// return nil.
//
// # Monitor events
//
// A channel registered via [WithMonitor] or [Socket.Monitor] receives [Event]
// values for lifecycle transitions (connect, listen, handshake, disconnect,
// close). Delivery is best-effort: if the channel is full the event is
// dropped rather than blocking the socket's hot path. Monitor channels
// must be buffered; 64-256 is a reasonable default for most applications.
//
// # Metrics and logging
//
// [Socket.Metrics] returns a point-in-time [SocketMetrics] snapshot
// (Sent/Recv/Dropped counters plus a QueueDepth gauge); poll it and publish
// to whatever sink you use. [WithLogger] injects a [*slog.Logger] for internal
// diagnostics (handshake failures, reconnect attempts, heartbeat loss). gozmq
// does not register expvar vars or emit OpenTelemetry spans itself.
//
// # Live Reconfiguration
//
// Most With* options are construction-time only because they shape the
// engine, session, or transport wiring created by [Context.NewSocket].
// Use the dedicated live APIs for mutable behavior: [Socket.Monitor],
// [Socket.Subscribe], [Socket.Unsubscribe], [Socket.Disconnect], and
// [Socket.Unbind]. Disconnect tears down the connections to one Connect
// target and stops its reconnect loop; Unbind stops one listener (matched
// against the resolved endpoint Bind produced) while leaving already-
// accepted connections alive, matching libzmq's zmq_unbind.
//
// # ROUTER identity handover
//
// When a peer attaches to a ROUTER claiming an identity that is already in
// use, gozmq hands the identity over: the new pipe takes it and the stale
// one is displaced. This is libzmq's ZMQ_ROUTER_HANDOVER=1 behaviour rather
// than its default, which rejects the new peer and keeps the old pipe
// bound. Handover is the better default in practice — a client reconnecting
// before the server's TCP stack has noticed the old connection died would
// otherwise be stranded until the stale pipe times out — but it does mean
// two clients that pick the same [WithIdentity] value will silently evict
// each other rather than one failing loudly. Identities must be unique.
//
// # Subscription refcount
//
// SUB and XSUB maintain per-prefix refcounts. Calling Subscribe with the same
// prefix twice requires two matching Unsubscribe calls before the prefix is
// removed from the filter and a CANCEL is sent upstream. This matches libzmq
// and lets independent components on the same socket manage their own
// subscriptions without interfering.
//
// # Further reading
//
// See design.md at the repository root for the full architecture, protocol
// implementation notes, security mechanisms, transport details, and test
// strategy.
package gozmq
