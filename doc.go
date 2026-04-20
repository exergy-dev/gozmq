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
// # Security
//
// The default security mechanism is NULL: no authentication, no encryption.
// This matches libzmq and is appropriate for trusted networks (inproc, ipc,
// or a private tcp segment) but NOT for untrusted or public networks. To
// authenticate and/or encrypt, choose one of:
//
//   - [WithPlainAuth] / [WithPlainClient] — username/password authentication.
//     Credentials travel in cleartext unless the underlying transport is
//     wrapped in tls+tcp://.
//   - [WithCurveServer] / [WithCurveClient] — CurveZMQ mutual authentication
//     with forward-secret encryption using long-term Curve25519 keys.
//
// For untrusted networks prefer CURVE, or layer PLAIN under tls+tcp://.
//
// # Defaults
//
// Sockets constructed via [Context.NewSocket] start with these defaults. Each
// is overridable via a With* option; see options.go for full documentation.
//
//	sendHWM              1000         (outbound high-water mark, messages)
//	recvHWM              1000         (inbound  high-water mark, messages)
//	linger               1s           (Close drain grace period)
//	heartbeatInterval    30s          (ZMTP 3.1 PING cadence)
//	heartbeatTimeout     90s          (peer-unresponsive deadline)
//	handshakeTimeout     30s          (greeting + security handshake cap)
//	maxMessageSize       64 MiB       (per-message ceiling; oversize tears session)
//	reconnectInterval    100ms        (base backoff; exponential with 20% jitter)
//	tcpKeepalive         true         (SO_KEEPALIVE on TCP)
//	tcpNoDelay           true         (TCP_NODELAY on TCP)
//
// These differ from libzmq where libzmq's defaults are hostile: notably,
// linger defaults to 1s (not infinite) so a forgotten Close cannot hang the
// process indefinitely.
//
// # Concurrency model
//
// Socket.Send and Socket.Recv are safe for concurrent use across goroutines;
// the engine serializes them internally. However, the idiomatic usage — and
// the pattern the library is optimised for — is one goroutine per socket.
// Sharing a socket across many concurrent senders works but buys you nothing
// over a dedicated fan-in goroutine and makes flow-control reasoning harder.
//
// # Close semantics
//
// Socket.Close blocks for up to the configured linger duration while the
// session writers flush any queued outbound messages to the wire. linger == 0
// discards queued messages immediately; a negative linger waits indefinitely
// (matching libzmq) — this can deadlock Close if a peer never drains and
// should only be used with a parallel shutdown watchdog.
//
// # Monitor events
//
// A channel registered via [WithMonitor] or [Socket.Monitor] receives [Event]
// values for lifecycle transitions (connect, listen, handshake, disconnect,
// close). Delivery is best-effort: if the channel is full the event is
// dropped rather than blocking the socket's hot path. Dropped events are
// counted in the MonitorChanDropped expvar counter. Monitor channels must
// be buffered; 64-256 is a reasonable default for most applications.
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
