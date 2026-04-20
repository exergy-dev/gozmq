# Changelog

All notable changes to `gozmq` are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) after v1.0.0.

## [Unreleased]

### Draft socket types removed

- Removed `Server`, `Client`, `Radio`, `Dish`, `Scatter`, `Gather`, `Peer`, `Channel` from the `SocketType` enum.
- Removed `WithDraftTypes`, `Socket.Join`, `Socket.Leave`, and the `socket/draft` package.
- `SocketType` now enumerates only stable ZMTP patterns: Req, Rep, Dealer, Router, Pub, Sub, XPub, XSub, Push, Pull, Pair, Stream.

### LINGER drain fix

- Per-writer cancel context so `Socket.Close` no longer tears down the TCP conn before the writer has flushed queued outbound frames (`internal/session/writer.go`, `internal/session/session.go`).
- Non-blocking drain observation via pipe `Closed()` poll in `internal/session/linger.go`; `DrainOutbound` bounded by `WithLinger` duration, `-1` waits forever, `0` discards.
- `SO_LINGER` set on `*net.TCPConn` during teardown so `close(2)` blocks until kernel send buffer is flushed (`transport/tcp.go` `SetLinger`, wired from `internal/session/session.go`).
- `Reconnector.CurrentSession` and `Reconnector.Quiesce` (`internal/session/reconnect.go`) let `Socket.Close` track the client-side session across reconnects and stop the dial loop without racing the drain.
- Analysis and fix rationale captured in `docs/linger-drain-bug.md`.

### Wave 4 — Operational polish

- Lifecycle monitor events in `events.go`: Connected, ConnectDelayed, ConnectRetried, Listening, BindFailed, Accepted, AcceptFailed, Closed, CloseFailed, Disconnected, HandshakeSucceeded, HandshakeFailedProtocol, HandshakeFailedAuth; delivered via `Socket.Monitor` and `WithMonitor(ch, mask)`.
- Per-socket expvar metrics in `internal/metrics/`: `MessagesSent`, `MessagesReceived`, `BytesSent`, `BytesReceived`, `HandshakesSucceeded`, `HandshakesFailed`, `Reconnects`, `HWMHits`, `MonitorChanDropped`; published as `gozmq.<label>` with pointer-hash fallback on label collision.
- `WithLogger(*slog.Logger)` injects a structured logger into the socket; session/reconnect paths fan diagnostics through it (`options.go`, `gozmq.go`).
- TLS transport in `transport/tls.go`: `tls+tcp://` scheme, registered through `transport.RegisterTransport`.
- IPC transport in `transport/ipc.go`: Unix domain sockets, Linux abstract-namespace support, `WithIPCFilePerm` for filesystem-path permissions.
- STREAM socket in `socket/stream.go` + `internal/stream/adapter.go`: raw non-ZMTP bytes, per-peer identity synthesis (ROUTER-shaped anonymous identity), zero-body-frame half-close sentinel.

### Wave 3 — Security

- PLAIN mechanism in `security/plain.go`: `WithPlainAuth(cb)` server hook, `WithPlainClient(user, pass)` client credentials.
- CURVE mechanism in `security/curve/`: Curve25519 via `golang.org/x/crypto/nacl/box`, Z85 key codec (`keys.go`), nonce discipline (`nonce.go`), handshake golden vectors in `testdata/handshake_golden.hex`.
- `WithCurveServer(secretKey)` and `WithCurveClient(serverKey, publicKey, secretKey)` wire CURVE into the handshake.
- ZAP (RFC 27) request/response codec in `security/zap.go`: `ServeRequest` hook; authorizer addressed at `inproc://zeromq.zap.01`.
- `WithHandshakeTimeout(d)` bounds greeting + security handshake (`options.go`, consumed in `internal/session/handshake.go`).

### Wave 2 — Core patterns

- REQ/REP lock-step FSM in `socket/req.go` and `socket/rep.go`; out-of-order Send/Recv return `ErrFSMViolation`.
- DEALER engine in `socket/dealer.go` round-robins outbound across peers via `LoadBalancer` and fair-queues inbound via `FairQueue`.
- ROUTER engine in `socket/router.go` routes by leading identity frame; `WithRouterMandatory(true)` returns `ErrRouteUnknown` instead of silently dropping unknown-peer sends.
- PUB/SUB fanout in `socket/pub.go` / `socket/sub.go`; subscriptions propagate upstream as SUBSCRIBE/CANCEL commands, per-peer `SubTree` in `socket/subtree.go` filters before delivery.
- XPUB/XSUB in `socket/xpub.go` / `socket/xsub.go` surface subscriptions as ordinary messages; `WithXPubVerbose(true)` forwards duplicates instead of coalescing.
- LINGER on `Socket.Close` (initial implementation) via `WithLinger` and `internal/session/linger.go` — the drain-correctness fix landed post-wave (see "LINGER drain fix").
- Reconnect with exponential backoff + jitter in `internal/session/reconnect.go`: `WithReconnectInterval`, `WithReconnectIntervalMax`, symmetric `JitterFraction`, `OnRetry` hook feeding `EventConnectDelayed` / `EventConnectRetried`.

### Wave 1 — M1 deliverable: PAIR round-trip over TCP and inproc

- ZMTP 3.1 framing, greeting (incl. 3.0 fallback), commands (READY, PING/PONG, SUBSCRIBE/CANCEL, ERROR, DISCONNECT) in `zmtp/`.
- Metadata property-list codec in `zmtp/metadata.go`.
- NULL security mechanism in `security/null.go`.
- TCP + TCP6 + inproc transports in `transport/` with connect-before-bind queueing for inproc.
- Session layer in `internal/session/` (greeting + handshake FSM, reader/writer goroutines, heartbeat).
- PUSH / PULL / PAIR engines in `socket/`, plus reusable `LoadBalancer` / `FairQueue` primitives for Wave 2.
- Public `Socket` API wires transport registry + session + engine; `Bind`, `Connect`, `SendMsg`, `RecvMsg`, `Close` all functional for Pair/Push/Pull.
- Import cycle broken by moving error sentinels to `internal/zmqerr` (re-exported via variable aliases at root).
- Fuzz targets `FuzzFrameDecode`, `FuzzGreeting`, `FuzzCommand`, `FuzzMetadata` — each clean for 2.9M+ execs over 3s.
- `examples/pair_ping/` runnable demo round-trips multipart messages over both transports.

### Wave 0 — Foundation

- Repo skeleton, `go.mod` (Go 1.22+), package layout per design §6.2.
- Public type stubs: `Context`, `Socket`, `Msg`, `Option`, `Event`.
- Error sentinels: `ErrContextClosed`, `ErrSocketClosed`, `ErrHWM`, `ErrFSMViolation`, `ErrHandshakeTimeout`, `ErrAuthFailed`, `ErrMalformedFrame`, `ErrProtocolVersion`, `ErrRouteUnknown`.
- Internal interface contracts in `internal/iface/`.
- `internal/pipe/` channel-backed bounded `Pipe` reference implementation with HWM property tests.
- Test scaffolding: `goleak`, fuzz corpus skeleton, CI with `-race`.
