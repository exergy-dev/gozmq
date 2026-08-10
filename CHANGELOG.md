# Changelog

All notable changes to `gozmq` are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) after v1.0.0.

## [Unreleased]

### Security

- A socket configured for CURVE could silently fall back to NULL (unauthenticated, plaintext) when the mechanism failed to construct. Mechanism construction failure is now an error and the connection is torn down — never a downgrade to a weaker mechanism (`wiring.go` `newMechanism`).
- DoS: inbound multipart messages were accumulated without bound regardless of `WithMaxMessageSize`. The session reader now caps the *accumulated* multipart size (`session.Config.MaxMessageSize`), tearing the session down with `ErrMalformedFrame` (surfaced via a monitor `EventDisconnected`) on overflow. The send side keeps its synchronous per-frame error.
- CURVE: best-effort zeroization of long-term and ephemeral secret keys and cookie keys after use; handshake command names are now compared exactly (case-sensitive); an `ERROR` command is emitted to the peer on ZAP rejection.
- PLAIN: removed undocumented lax "compat" fallbacks in the `ERROR`-command parser; the server no longer retains the client password after the auth decision; `WithPlainAuth` docs now recommend constant-time credential comparison.
- TLS transport: `MinVersion` is floored at TLS 1.2 (a zero value is upgraded; explicitly configured lower values are rejected), and server-side handshakes run under a hard deadline (new `HandshakeTimeout` field, default 10s) so a stalled client cannot pin resources.

### Breaking changes

- `Socket.Close` / `Socket.CloseCtx` are now idempotent: repeated calls return nil (previously `ErrSocketClosed`), matching `Context.Close` and the `io.Closer` convention.
- `Socket.Disconnect` of an endpoint that was never connected now returns an error wrapping the new `ErrEndpointUnknown` sentinel (previously `ErrEndpointInvalid`, which is reserved for syntactically bad endpoints).
- Options that configure ZMTP-layer behavior (PLAIN/CURVE security, `WithHeartbeat`, `WithIdentity`, `WithHandshakeTimeout`, `WithZapDomain`) now fail `Context.NewSocket` on STREAM sockets instead of being silently ignored — silently accepting e.g. `WithPlainAuth` on a raw-TCP listener that authenticates nobody was a security hazard.
- `WithReconnectIntervalMax` default changed from 0 (unbounded backoff growth) to 30s. 0 now explicitly means "no cap"; note the libzmq divergence — `ZMQ_RECONNECT_IVL_MAX=0` (libzmq's default) means "constant interval, no exponential backoff".
- `typed`: `Router` renamed to `TopicMux` (`NewTopicMux`, `ErrTopicMuxRunning`, `ErrNoTopicMatch`). Registration now unlocks again after `Run` returns, so Register → Run → Unregister/Register → Run sequences work.

### Fixed

- Poller: a blocking `Poll` never woke on the single-peer fast paths (PAIR, REQ, direct-mode FairQueue) because those paths bypassed the readiness channels. Data-path readiness gates were added in `internal/pipe` so every engine wakes the Poller.
- Use-after-free: ROUTER `Send` and the REP reply path aliased pooled frame buffers after ownership transfer; fixed via `TransferPool` ownership moves and deep-copied envelopes.
- PUB/XPUB fan-out shared one `*Msg` across N peer pipes with a non-concurrency-safe recycle. `Recycle` is now refcount-aware: the engine takes a per-target `Retain`, the N session writers release concurrently post-flush, and exactly one (the last) reclaims.
- `LoadBalancer` (DEALER/PUSH): fixed a lost wakeup under concurrent `Send`s (generation-channel pattern). It also now rotates past peers whose pipe is at HWM like libzmq's `lb_t` instead of head-of-line blocking on the cursor peer; only when every peer is full does Send block.
- PUB now drops immediately for a subscriber whose pipe is full instead of stalling fan-out ~10ms per message on the slow peer.
- REQ: a ctx-canceled `Recv` no longer resets the FSM — the outstanding reply is still awaited, matching libzmq's EAGAIN semantics; a stale buffered reply from an abandoned request is discarded at the next `Send`; and a `Send` blocked at HWM now honors `Close`.
- XPUB synthesizes `CANCEL` messages when a departing peer's subscriptions drop to refcount zero (libzmq parity), so applications tracking subscriptions see symmetric subscribe/cancel pairs.
- Session: the reader no longer writes PONG replies inline under the writer mutex (deadlock under full-duplex saturation — PONGs now route through the writer); `writerExited` closes on every exit path (fixes a `Close` hang with `linger < 0`); heartbeat timeout no longer tears down sessions parked on local engine backpressure; the greeting decode accepts any ZMTP 3.x minor version (minor ≥ 1 is treated as 3.1, so future 3.x peers are not rejected); PING commands now carry the TTL; and handshake-failure `ERROR` commands (including the new NULL/CURVE ZAP-rejection ERRORs) are actually written to the wire before teardown.
- TLS transport: a ctx-canceled `Accept` no longer closes the shared listener; handshakes no longer serialize the accept loop (each runs concurrently under its deadline).
- inproc: `SetReadDeadline` / `SetWriteDeadline` now interrupt in-flight blocked `Read` / `Write` calls, per the `net.Conn` contract.
- inproc: a PLAIN/CURVE-configured binder now actively rejects (closes) direct-pipe bypass offers from no-security dialers. Previously the offer sat undrained: the dialer emitted `EventConnected` and hung silently on a pipe wired to nothing; it now observes a prompt `EventDisconnected`, matching the visibility of a failed handshake.

### Added

- `Socket.Unbind(endpoint)`: stops the listener matching the resolved endpoint (the value `LastEndpoint` reports). Established connections accepted from that listener survive — only new accepts stop, matching libzmq's `zmq_unbind`. Unmatched endpoints return an error wrapping `ErrEndpointUnknown`.
- `Socket.RecvBytes(ctx)`: receives a single-frame message and returns an independent copy of the payload, releasing internal pool buffers automatically — the misuse-proof counterpart to `SendBytes`. Multipart messages return an error.
- `ErrEndpointUnknown` sentinel: returned by `Disconnect` and `Unbind` when the endpoint matches nothing active.
- `EventConnectFailed` monitor event: terminal connect failures with no reconnect loop behind them — an unknown transport scheme on `Connect`, or a STREAM dial failure (STREAM dials exactly once). With this split, `EventConnectDelayed`'s `Reason` is now always the pending backoff duration.
- `transport`: TLS `HandshakeTimeout` field (default 10s); TCP `ReadBufferSize` / `WriteBufferSize` fields making the previously hard-coded 8 MiB socket buffers configurable (0 = OS default).

### Changed

- CURVE traffic-phase crypto precomputes the shared key once per session (`box.Precompute`) instead of doing per-frame ECDH — a large encrypted-throughput win.
- TCP accept-poll interval unified at 5ms.
- Docs reconciled with shipped behavior: `SocketType` constants are documented and `String()` renders unknown values as `SocketType(n)`; the `Msg` alias doc enumerates its supported method set; `PollOut` documents its divergence from `ZMQ_POLLOUT` (no HWM awareness); `Connect` documents STREAM's dial-once/no-reconnect semantics (and STREAM's lack of libzmq's `ZMQ_STREAM_NOTIFY` connect/disconnect notifications); `Socket.Monitor` documents replace semantics, nil-deregistration, and the buffered-channel requirement.

### Performance

- Session writer: fixed `net.Buffers` capacity erosion — `WriteTo` consumes the slice header it is handed, so every batch re-grew its buffer slice from zero capacity (~33% of all bytes allocated on the TCP throughput path; 32B PUSH/PULL: 291 → 194 B/op).
- Session reader: a persistent per-reader scratch buffer (sized exactly at the 4 KiB pool threshold) now backs small inbound frames, removing a pool round-trip per frame. Combined with the writer fix: 32B PUSH/PULL TCP ~300 → ~160 ns/op; native two-process throughput vs libzmq 4.3.4 is now 1.06–1.10x at 256B–8KB (gozmq ahead) and 0.57x/0.89x at 32B/64KB.
- PUB/XPUB: `Send` reads a copy-on-write peer snapshot — no engine mutex and no per-message slice allocation on the fan-out path (3-subscriber fanout: −26% ns/op, 2 → 1 allocs; XPub/XSub latency: −24%).
- CURVE: `EncodeFrame` seals directly into the pre-sized wire buffer using a per-direction scratch (3 → 1 allocs, one full-payload copy removed; +12–22% encode throughput). Wire bytes are unchanged (golden vectors pass).
- Poller: the registration snapshot and `reflect.SelectCase` slice are cached across `Poll` calls (invalidated on registration change or socket close); steady-state `Poll` allocates only the result slice (~113 ns at 8 registered sockets, was ~377 ns / 4 allocs).
- `perf/REPORT.md` refreshed with current in-repo and native-libzmq numbers (see the 2026-06-10 update section).

### Internal

- Deleted ~300 SLOC of dead fallback paths; unified readiness signaling on a single gate idiom; corrected the engine thread-safety contract documentation.
- Added an engine conformance test suite; heartbeat behavior is now tested via the injectable clock (`session.Config.Now`); un-skipped the writer batching/backpressure tests.
- Removed the single-caller `iface.FrameBufferAppender` fast lane (and its header `sync.Pool`s) — command writes use the plain codec path; the batch hot path is untouched. Deleted the session writer's unreachable foreign-pipe fallbacks behind a `sessionChanPipe` assertion mirroring the socket package's `asChanPipe` contract.
- Interface trims: `iface.Session` dropped the unused `Endpoint`/`Role` methods; `iface.Pipe` dropped `SendHWM`/`RecvHWM`/`PeerEndpoint` (the concrete pipe keeps them).
- `internal/socket`: ROUTER's identity-cache behavior is now plain methods on `identityRouter` (gated by a constructor flag) instead of four function-pointer hooks; synthetic subscription frames are built by one shared helper; deleted dead `pipeRing.contains`.
- `typed`: `TopicMux.Run` is now built on the shared `handleLoop` dispatch skeleton instead of duplicating it (error classification and messages unchanged).
- `security/curve`: deleted the unused `newWithRand` constructor; added Encode/Decode/round-trip benchmarks for the traffic path.
- Dead-code sweep (~650 SLOC): deleted `internal/zmtp`'s unused greeting and PING/PONG codecs (the session layer deliberately implements both inline) plus their tests and fuzz corpus; removed write-only fields (`session.Config.Role`, `pipe.PeerInfo.Endpoint` and the `stream.Config.Endpoint` chain that fed it, pipe HWM accessors); narrowed `iface.Conn` (dropped per-direction deadline setters) and `iface.DirectPipeAcceptor` (dropped `CloseCh`); removed structurally impossible nil-guards in session construction and the inproc transport; `ErrHWM` documented as reserved (no built-in pattern currently returns it) and `WithSendHWM` docs corrected to describe actual blocking/drop behavior.

### Pooled-message safety & teardown (correctness)

- Fixed a use-after-free on the writer hot path: the session writer recycled a message's pooled frame buffers before the batched `writev` consumed them, corrupting large (>4 KiB) re-sent inbound frames in proxy patterns. `internal/session/writer.go` now defers `msg.Recycle` until after the flush, tracking only pooled messages via the new `(*Msg).IsPooled` so the common non-pooled path stays bookkeeping-free. Covered by a `-race` proxy regression test (`gozmq_pool_proxy_test.go`).
- Removed the 10 ms `writerDrainSettleTime` sleep in the writer drain path. `Socket.CloseCtx` already blocks on `WriterFlushed` before closing the conn, which closes only after the final `writev` returns — so the sleep guarded nothing.
- `(*Msg).TransferPool` now carries the `subscription` flag across envelope rewrites; `internal/msg/msg.go` documents the full per-`Msg` state model.

### Internal refactors (no behavior change)

- Split the monolithic `gozmq.go` into `context.go`, `socket.go`, `wiring.go`, `directpipe.go`, and `close.go`.
- Split `internal/socket/engine.go` into `loadbalancer.go`, `fairqueue.go`, and `combinators.go`; the shared metric/readiness plumbing stays in `engine.go`.
- Extracted `internal/socket/pipering.go` (`pipeRing`) to unify the round-robin pipe-set + cursor logic shared by `LoadBalancer` and the REQ engine.
- Consolidated SUB/XSUB subscription mutation behind `SubEngine.applySubLocked`.
- Modernized the PAIR engine to inline the send/recv channel select, dropping the per-Send/per-Recv goroutine (kept only as a fallback for pipes without channel access).
- Added an injectable monotonic clock (`session.Config.Now`) so heartbeat-timeout behavior can be tested without real elapsed time (the heartbeat test suite now exercises it; see Internal above).
- Removed the empty `internal/security/` directory (NULL/PLAIN live in the public `security/` package).

### Documentation

- Documented that transports and security mechanisms are an intentional first-party closed set (`transport`/`security` package docs, `Context.RegisterTransport`, design §7.5).
- Reconciled the observability docs with reality: `Socket.Metrics()`, monitor events, and `WithLogger` are implemented; `expvar`/OpenTelemetry are explicitly deferred (design §12, `doc.go`).

### Msg reclamation

- BREAKING: `*Msg` now exposes `Retain`/`Release`; pooled receive buffers are returned to the size-tiered pool when the refcount reaches zero. Callers should `Release` *Msg values returned by `Socket.RecvMsg` (a no-op for *Msg without pooled frames). A safety-net finalizer remains for unmodified callers but is not the primary reclamation path.

### Draft socket types removed

- Removed `Server`, `Client`, `Radio`, `Dish`, `Scatter`, `Gather`, `Peer`, `Channel` from the `SocketType` enum.
- Removed `WithDraftTypes`, `Socket.Join`, `Socket.Leave`, and the `socket/draft` package.
- `SocketType` now enumerates only stable ZMTP patterns: Req, Rep, Dealer, Router, Pub, Sub, XPub, XSub, Push, Pull, Pair, Stream.

### API simplification

- Removed `Socket.SetOption`; socket configuration now happens only through options passed to `Context.NewSocket`.
- Removed the internal expvar metrics package and hooks.
- Removed `EventCloseFailed`, `WithTCPKeepalive`, and unused socket-tuning plumbing.
- Moved implementation-only socket engines, ZMTP codecs, and NULL/PLAIN mechanisms under `internal/`.

### Handshake compatibility

- Session handshakes now propagate peer READY metadata through `SetPeerMetadata` and reject incompatible advertised `Socket-Type` pairings before traffic phase begins.

### LINGER drain fix

- Per-writer cancel context so `Socket.Close` no longer tears down the TCP conn before the writer has flushed queued outbound frames (`internal/session/writer.go`, `internal/session/session.go`).
- Outbound drain bounded by `WithLinger` duration (`-1` waits forever, `0` discards), implemented in `Socket.CloseCtx` (`close.go`) over the unified `peerLink` teardown contract (`peerlink.go`).
- `SO_LINGER` set on `*net.TCPConn` during teardown so `close(2)` blocks until kernel send buffer is flushed (`transport/tcp.go` `SetLinger`, wired from `internal/session/session.go`).
- `Reconnector.CurrentSession` and `Reconnector.Quiesce` (`internal/session/reconnect.go`) let `Socket.Close` track the client-side session across reconnects and stop the dial loop without racing the drain.

### Monitor, transports, and stream

- Lifecycle monitor events in `events.go`: Connected, ConnectDelayed, ConnectRetried, Listening, BindFailed, Accepted, AcceptFailed, Closed, Disconnected, HandshakeSucceeded, HandshakeFailedProtocol, HandshakeFailedAuth; delivered via `Socket.Monitor` and `WithMonitor(ch, mask)`.
- `WithLogger(*slog.Logger)` injects a structured logger into the socket; session/reconnect paths fan diagnostics through it (`options.go`, `gozmq.go`).
- TLS transport in `transport/tls.go`: `tls+tcp://` scheme, registered through `transport.RegisterTransport`.
- IPC transport in `transport/ipc.go`: Unix domain sockets, Linux abstract-namespace support, `WithIPCFilePerm` for filesystem-path permissions.
- STREAM socket in `internal/socket/stream.go` + `internal/stream/adapter.go`: raw non-ZMTP bytes, per-peer identity synthesis (ROUTER-shaped anonymous identity), zero-body-frame half-close sentinel.

### Security mechanisms (PLAIN, CURVE, ZAP)

- PLAIN mechanism in `security/plain.go`: `WithPlainAuth(cb)` server hook, `WithPlainClient(user, pass)` client credentials.
- CURVE mechanism in `security/curve/`: Curve25519 via `golang.org/x/crypto/nacl/box`, Z85 key codec (`keys.go`), nonce discipline (`nonce.go`), handshake golden vectors in `testdata/handshake_golden.hex`.
- `WithCurveServer(secretKey)` and `WithCurveClient(serverKey, publicKey, secretKey)` wire CURVE into the handshake.
- ZAP (RFC 27) request/response codec in `security/zap.go`: `ServeRequest` hook; authorizer addressed at `inproc://zeromq.zap.01`.
- `WithHandshakeTimeout(d)` bounds greeting + security handshake (`options.go`, consumed in `internal/session/handshake.go`).

### Socket patterns

- REQ/REP lock-step FSM in `internal/socket/req.go` and `internal/socket/rep.go`; out-of-order Send/Recv return `ErrFSMViolation`.
- DEALER engine in `internal/socket/dealer.go` round-robins outbound across peers via `LoadBalancer` and fair-queues inbound via `FairQueue`.
- ROUTER engine in `internal/socket/router.go` routes by leading identity frame; `WithRouterMandatory(true)` returns `ErrRouteUnknown` instead of silently dropping unknown-peer sends.
- PUB/SUB fanout in `internal/socket/pub.go` / `internal/socket/sub.go`; subscriptions propagate upstream as SUBSCRIBE/CANCEL commands, per-peer `SubTree` in `internal/socket/subtree.go` filters before delivery.
- XPUB/XSUB in `internal/socket/xpub.go` / `internal/socket/xsub.go` surface subscriptions as ordinary messages; `WithXPubVerbose(true)` forwards duplicates instead of coalescing.
- LINGER on `Socket.Close` via `WithLinger`, drained in `Socket.CloseCtx` (`close.go`).
- Reconnect with exponential backoff + jitter in `internal/session/reconnect.go`: `WithReconnectInterval`, `WithReconnectIntervalMax`, symmetric `JitterFraction`, `OnRetry` hook feeding `EventConnectDelayed` / `EventConnectRetried`.

### Core implementation

- ZMTP 3.1 framing, greeting (incl. 3.0 fallback), commands (READY, PING/PONG, SUBSCRIBE/CANCEL, ERROR, DISCONNECT) in `internal/zmtp/`.
- Metadata property-list codec in `internal/zmtp/metadata.go`.
- NULL security mechanism in `security/null.go`.
- TCP + TCP6 + inproc transports in `transport/` with connect-before-bind queueing for inproc.
- Session layer in `internal/session/` (greeting + handshake FSM, reader/writer goroutines, heartbeat).
- PUSH / PULL / PAIR engines in `internal/socket/`, plus reusable `LoadBalancer` / `FairQueue` primitives.
- Public `Socket` API wires transport registry + session + engine; `Bind`, `Connect`, `SendMsg`, `RecvMsg`, `Close` all functional for Pair/Push/Pull.
- Import cycle broken by moving error sentinels to `internal/zmqerr` (re-exported via variable aliases at root).
- Fuzz targets `FuzzFrameDecode`, `FuzzGreeting`, `FuzzCommand`, `FuzzMetadata` — each clean for 2.9M+ execs over 3s.
- `examples/pair_ping/` runnable demo round-trips multipart messages over both transports.

### Foundation

- Repo skeleton, `go.mod` (Go 1.22+), package layout per design §6.2.
- Public type stubs: `Context`, `Socket`, `Msg`, `Option`, `Event`.
- Error sentinels: `ErrContextClosed`, `ErrSocketClosed`, `ErrHWM`, `ErrFSMViolation`, `ErrHandshakeTimeout`, `ErrAuthFailed`, `ErrMalformedFrame`, `ErrProtocolVersion`, `ErrRouteUnknown`.
- Internal interface contracts in `internal/iface/`.
- `internal/pipe/` channel-backed bounded `Pipe` reference implementation with HWM property tests.
- Test scaffolding: `goleak`, fuzz corpus skeleton, CI with `-race`.
