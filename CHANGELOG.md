# Changelog

All notable changes to `gozmq` are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) after v1.0.0.

## [Unreleased]

**Breaking changes**, detailed in the sections below. No `v1.0.0` tag has
been published yet; if 1.0.0 is tagged first, the API removals need a major
version under the compatibility promise.

API:

- `WithZapHandler(*security.ZapHandler)` → `WithZAPHandler(security.ZAPHandler)`,
  `WithZapDomain` → `WithZAPDomain`. Turn a `ZapPolicy`/`ZAPAuthorizer` into a
  plain function of type `security.ZAPHandler`.
- `security.ZAPAuthorizer`, `ZapPolicy`, the `ZapHandler` struct,
  `NewZapHandler` and `ZapConfigurable` are removed; use
  `security.ZAPHandler` (its `ServeRequest` method serves RFC 27 wire
  requests).
- ZAP codec renames: `ZapRequest`/`ZapResponse` → `ZAPRequest`/`ZAPResponse`,
  `Encode`/`DecodeZap*` → `Encode`/`DecodeZAP*`, `ZapStatus*` → `ZAPStatus*`,
  `ErrZapTemporary` → `ErrZAPTemporary`. Search and replace.
- `security.Mech` requires `PeerMetadata() map[string]string`, and
  `SetPeerMetadata` is gone from the interface and `MechBase`. Custom
  mechanisms that embed `MechBase` should call `MergePeerMetadata` with the
  peer's properties; `MechBase.ConfigureZAP` takes a `security.ZAPHandler`.
- `transport.TLSTransport.HandshakeTimeout` is removed; `WithHandshakeTimeout`
  on the socket now bounds the TLS handshake.
- `transport.Registry`, `NewRegistry` and `New` are removed; use
  `(*gozmq.Context).RegisterTransport`.
- `msg.Recycle`, `(*Msg).IsPooled`, `AppendPooledFrame`, `TransferPool`,
  `Reset`, `msg.FromFramesOwned`, `MarkSubscription` and `IsSubscription` are
  removed (all but `IsSubscription` were excluded from the 1.0.0
  compatibility promise). Drop the calls; use `msg.FromFrames`, and
  `(*Msg).Derive` to re-envelope a lent message.

Behaviour:

- `NewSocket` rejects `WithPlainAuth` + `WithPlainClient`, and PLAIN options
  combined with CURVE options (CURVE used to win silently). Configure one
  mechanism per socket.
- The PLAIN role now follows the option rather than Bind/Connect, so a
  connecting `WithPlainAuth` socket (or a binding `WithPlainClient` one) runs
  PLAIN instead of silently falling back to NULL. Check that both ends agree
  on who is the PLAIN server.
- A NULL socket consults the ZAP handler only when it sets `WithZAPDomain`
  (libzmq behaviour). Set a domain on NULL sockets that ZAP must vet.
- `transport.TCPTransport` buffer sizes default to 0 (OS autotuning) instead of
  8 MiB. Set `ReadBufferSize`/`WriteBufferSize` to `8 << 20` and re-register the
  transport to get the old sizes back.
- PAIR no longer returns `io.EOF`/`io.ErrClosedPipe` when the peer goes away;
  `Send`/`Recv` wait for the next peer. Use a context deadline or monitor
  events to notice a lost peer.
- CURVE traffic-phase commands use the libzmq encoding, which older gozmq
  releases reject. Upgrade both ends of a gozmq↔gozmq CURVE link together.

### Security

- PLAIN: a `WithPlainAuth` socket that connected, or a `WithPlainClient`
  socket that bound, silently used NULL and authenticated nobody. The PLAIN
  role now follows the option; setting both is an error.
- inproc: `WithSecurityMech` sockets took the direct-pipe bypass and ran no
  handshake at all.
- CURVE: low-order public keys with bit 255 set were accepted, letting a
  client forge the INITIATE vouch without a secret key. All low-order keys
  are now rejected.
- ZAP now also vets PLAIN/CURVE servers that connect rather than bind.
- Handshake frames are capped at 64 KiB; an unauthenticated peer could make
  the server allocate up to `MaxMessageSize` (64 MiB) per connection with a
  9-byte header.
- The greeting rejects a peer announcing a different security mechanism, as
  ZMTP and libzmq require.
- ZAP metadata can no longer override the peer's ZMTP `Identity` or
  `Socket-Type`; it only adds properties the peer did not send. The ZAP
  user-id is surfaced as the `User-Id` peer property, as in libzmq.
- A ZAP policy returning an over-long metadata name no longer panics inside
  the handshake; it yields a 500 response.
- **Breaking:** `NewSocket` rejects `WithPlainAuth` combined with
  `WithPlainClient`, and PLAIN options combined with CURVE options
  (previously CURVE silently won).
- An in-process ZAP handler's error is now preserved in the handshake error
  chain.

### Added

- `WithRecvBufferPool(enabled)`: frames over 4 KiB on NULL/PLAIN sessions are
  received into a size-tiered buffer pool, as lent messages. The receiver
  must `Release`; frames are valid until the last reference is released;
  `SendMsg` holds its own reference, so forward-then-`Release` is safe;
  over-release panics; there is no finalizer, and a forgotten `Release` is
  safe (the GC collects the buffer). Rejected on STREAM sockets.
- `msg.NewWithRelease`, `(*Msg).Lent` and `(*Msg).Derive`, the building
  blocks of lent messages.
- `security.DecodeError` for recognising a peer's ERROR command in custom
  mechanisms.
- A custom mechanism may implement `IdentityFraming() bool` to opt into the
  zero-copy framing path (see Fixed).
- `tcp://host:*` binds an ephemeral port.

### Changed

- Received frames are ordinary Go memory owned by the caller, with no pool
  and no finalizer behind them. `(*Msg).Release` and `Retain` are no-ops on
  such plain messages, so existing calls are harmless and new code need not
  make them; they are reference operations only on lent messages (see
  `WithRecvBufferPool`). `Msg` grew from 48 to 64 bytes.
- **Behaviour change — PAIR:** a peer disconnect is no longer surfaced to
  the application. `RecvMsg` delivers any messages the old peer left
  buffered, then blocks until the next peer attaches (libzmq semantics)
  instead of returning `io.EOF`; a `SendMsg` racing the disconnect waits
  for the next peer instead of returning `io.ErrClosedPipe`.
- ROUTER/STREAM `Send` to a peer that just disconnected: ROUTER without
  `WithRouterMandatory` silently drops (counted in `Metrics.Dropped`); ROUTER
  with it and STREAM return an error wrapping `ErrRouteUnknown` instead of a
  raw `io.ErrClosedPipe`.
- NULL consults the ZAP handler only when a ZAP domain is set, matching
  libzmq (previously every accepting NULL socket was vetted once a handler
  was installed).
- **Breaking:** `transport.TCPTransport` read/write buffer sizes default to 0
  (OS autotuning, as libzmq does) instead of 8 MiB. TCP socket options are
  applied best-effort.
- tcp/tls/ipc `Accept` blocks instead of polling every 5ms, and `Listen`
  honours its context. `transport.Listener.Accept` documents its terminal
  errors (`net.ErrClosed`, ctx errors); closed-listener errors match
  `net.ErrClosed`.
- XPUB subscription processing never blocks on the application; pending
  events are queued without a fixed bound (previously a 16-slot buffer).
- `typed.Receiver.Recv` and `typed.Subscriber.Recv` decode errors now wrap
  `typed.ErrDecode` (still `ErrFrameCount` where applicable); message text
  changed from "typed: unmarshal: …" to "typed: decode failed: …".
- `EventConnectDelayed`/`EventConnectRetried` are no longer emitted while a
  socket is closing.
- **Breaking:** `security.Mech` now requires `PeerMetadata() map[string]string`;
  `SetPeerMetadata` is removed from the interface and from `MechBase`.
- **Breaking:** `WithZapHandler(*security.ZapHandler)` →
  `WithZAPHandler(security.ZAPHandler)`, called directly instead of via an
  in-process ZAP encode/decode; `WithZapDomain` → `WithZAPDomain`.
- **Breaking:** ZAP codec renamed to consistent spelling:
  `ZapRequest`/`ZapResponse` → `ZAPRequest`/`ZAPResponse`;
  `Encode/DecodeZapRequest/Response` → `Encode/DecodeZAPRequest/Response`;
  `ZapStatus*` → `ZAPStatus*`; `ErrZapTemporary` → `ErrZAPTemporary`;
  `MechBase.ConfigureZAP` takes a `security.ZAPHandler`.
- `WithSendHWM(0)`/`WithRecvHWM(0)` (unlimited) no longer run a relay
  goroutine per pipe direction; an unbounded direction is a channel plus an
  overflow slice. inproc linger completes on a signal instead of polling.
- PUB no longer runs a watcher goroutine per subscriber, and inproc Conn
  deadlines no longer start a goroutine per Read/Write.
- CURVE no longer wipes its per-connection copy of the long-term secret key
  after the handshake: the socket keeps the key for later connections, so
  the wipe protected nothing. Ephemeral and cookie keys are still wiped.
- ROUTER/STREAM: the routing identity now travels with the peer's pipe, so
  messages drained after a peer disconnects keep its identity without the
  1024-entry post-detach cache, which pinned up to ~16 MB of pipe buffers
  under connection churn. ROUTER `Recv` no longer takes the engine lock.
- PUSH/DEALER send: removed the lock-free single-peer fast path (measured at
  under 9% on inproc PUSH/PULL, nothing elsewhere). FairQueue's single-peer
  receive path is kept (1.3×–12× on the perf suite).
- **XSUB no longer surfaces upstream SUBSCRIBE/CANCEL commands** (libzmq
  never does; conforming peers never send them to a subscriber). The
  session now turns ZMTP SUBSCRIBE/CANCEL commands into 0x01/0x00-prefixed
  messages only for PUB/XPUB and drops them for every other socket type.
  SUB/XSUB treat inbound data beginning with 0x00/0x01 as ordinary data.
- **Inbound fair-queueing runs no goroutines.** PULL, DEALER, REP, ROUTER,
  STREAM, SUB and XSUB read their peers' pipes directly, round-robin over
  the peers that have data (libzmq `fq_t`), instead of one forwarder
  goroutine per peer. An idle TCP connection costs one goroutine less on
  the receiving side, an inproc link one less; receive cost no longer
  grows with idle peers.
- `SocketMetrics.QueueDepth` now counts the messages queued in the
  socket's inbound pipes (PAIR and REQ included); it used to count only a
  one-slot internal handoff and was 0 with a single peer.
- `SocketMetrics.Dropped` no longer includes FairQueue handoff drops,
  which cannot happen any more: a message is only taken from a pipe by
  the receiver that returns it.
- **PUB/XPUB run no goroutine per subscriber.** Subscriptions are applied on
  the socket's next `Send` (and, for XPUB, `Recv` or poll), as in libzmq,
  instead of by a reader goroutine per peer. A subscriber link costs one
  goroutine less; XPUB subscribe churn is ~43% faster. A PUB that never
  sends leaves subscriptions queued, so a subscriber may back off at the
  receive HWM until the next `Send`.

### Removed

- **Breaking:** `transport.TLSTransport.HandshakeTimeout`. TLS handshakes now
  run lazily on the connection's first I/O, inside the ZMTP handshake,
  bounded by `WithHandshakeTimeout` (default 30s); `WithHandshakeTimeout(0)`
  therefore also leaves the TLS handshake unbounded.
- **Breaking:** `security.ZAPAuthorizer`, `security.ZapPolicy`,
  `security.ZapHandler` (struct), `security.NewZapHandler` and
  `security.ZapConfigurable`, replaced by the single func type
  `security.ZAPHandler` (whose `ServeRequest` method serves RFC 27 wire
  requests).
- **Breaking:** `msg.Recycle`, `(*Msg).IsPooled`, `(*Msg).AppendPooledFrame`,
  `(*Msg).TransferPool` and `(*Msg).Reset` (marked "internal plumbing" and
  excluded from the compatibility promise in 1.0.0), along with the default
  frame pool and the `*Msg` finalizer. The opt-in pool behind
  `WithRecvBufferPool` is internal.
- **Breaking:** `msg.FromFramesOwned`, `Msg.MarkSubscription` (both excluded
  from the compatibility promise in 1.0.0) and `Msg.IsSubscription`. Use
  `msg.FromFrames`; subscription commands are
  flagged on the internal pipe instead of the message.
- **Breaking:** `transport.Registry`, `transport.NewRegistry`, `transport.New`.
  Use `(*gozmq.Context).RegisterTransport`; every Context still preloads
  tcp, tcp6, ipc and inproc.

### Fixed

- A ZAP handler rejection that does not wrap `ErrAuthFailed` is still
  reported as an authorization failure (`ErrAuthFailed`,
  `EventHandshakeFailedAuth`), with the handler's error kept in the chain.
- Forwarding a received message (`RecvMsg` → `SendMsg` → `Release`) no longer
  corrupts large frames in flight.
- Frame slices kept after dropping their `*Msg` are no longer recycled by a
  finalizer.
- ROUTER `Send` no longer mutates the caller's message (data race with
  `Proxy` capture).
- CURVE: traffic-phase commands (heartbeat PING/PONG) are now carried in the
  encrypted MESSAGE flags byte (command bit 0x02) with no outer COMMAND flag,
  matching libzmq. Previously, with heartbeats enabled (the default), libzmq
  CURVE peers' PINGs were rejected as malformed and tore the session down, and
  gozmq's PINGs reached libzmq applications as user messages. Commands from
  older gozmq peers (outer COMMAND flag) are still accepted, but older gozmq
  releases reject the new encoding: upgrade both ends of gozmq↔gozmq CURVE
  links together.
- A per-connection accept error (EMFILE, an out-of-tree transport's
  per-connection error) no longer stops a bound socket from accepting; it is
  reported as `EventAcceptFailed` and retried with capped backoff.
- Silent or non-TLS clients no longer stall a tls+tcp listener; a non-TLS
  server no longer parks the TLS reconnector.
- A custom security mechanism named "NULL" or "PLAIN" no longer has its
  `EncodeFrame`/`DecodeFrame` skipped; the zero-copy framing path is opt-in
  via an `IdentityFraming() bool` method.
- REQ/REP: concurrent `SendMsg`/`RecvMsg` calls could both pass the
  state-machine check (two requests in flight, or an overwritten REP
  envelope); the loser now gets `ErrFSMViolation`.
- REQ with HWM 0 could pair a stale reply with the next request.
- ROUTER/STREAM: auto-generated peer identities can no longer collide (and
  misroute replies); they are now a per-socket counter with a random start.
- PULL/DEALER/ROUTER/REP (fair-queued sockets): messages already received from
  a peer that disconnects are no longer discarded when it was the only peer.
- XPUB: a subscriber that disconnects before the application reads its
  SUBSCRIBE now yields the matching CANCEL and no longer suppresses later
  SUBSCRIBEs for that prefix; a peer can no longer CANCEL a prefix it never
  subscribed to.
- PAIR: a reconnecting peer is no longer rejected when it attaches before
  the previous peer's pipe has been detached.
- A client connection whose pipe the engine refuses (e.g. a PAIR that already
  has a peer) is now torn down and retried with backoff instead of staying
  open unattached; `EventConnected` is emitted only after a successful attach.
- REP: a reply to a requester that has just disconnected is dropped
  silently instead of returning a raw `io.ErrClosedPipe`.
- A REP `Send` blocked at HWM now returns `ErrSocketClosed` when the socket
  closes.
- `Socket.Close` with a positive linger now flushes the entire outbound queue
  when `WithSendHWM(0)` is set; previously only a few hundred messages of a
  large backlog were sent.
- `WithRecvHWM(0)` sockets no longer leak a goroutine (and its queued
  messages) per disconnected peer after `Close`.
- Session teardown no longer sets `SO_LINGER(2)` on TCP connections, which
  made `Close` block up to 2s per connection and reset it on expiry.
- `Socket.Close` on an inproc (direct-pipe) connection no longer waits the
  full linger while the peer socket is alive; it returns as soon as the peer
  has received everything sent.
- `WithHeartbeat` timeout 0 now means 3× the interval, as documented
  (previously 2×).
- NULL and CURVE clients report a server ERROR (e.g. ZAP denial) as
  `ErrAuthFailed` with the reason, not `ErrMalformedFrame`.
- `Poller`: two Pollers, or concurrent `Poll` calls, waiting on the same
  socket no longer lose a wakeup; the second waiter used to sleep until its
  context expired while the message stayed queued.
- A `Poller` never woke for messages sent by an inproc PAIR peer.
- A `Poller` never reported REP, SUB or XSUB sockets as ready.
- `Proxy` now returns promptly after ctx cancellation even when the capture
  socket blocks (e.g. a PUSH/DEALER with no peer); taps queued or in flight
  at shutdown may be dropped.
- `Bind` racing `Close` no longer leaks the listener.
- inproc: a connect racing listener close can no longer be stranded; `Dial`
  honours ctx when the accept queue is full.
- ipc: a stale socket file left by a crashed process no longer blocks `Bind`;
  `Close` no longer unlinks a path another listener may have re-bound.
- `typed.TopicMux`: a concurrent `Run` now returns `ErrTopicMuxRunning`
  instead of running alongside the first; fixed a data race between
  `Register`/`Unregister` and a starting `Run`.
- STREAM sockets no longer pin a 32 KiB buffer per received chunk (small
  segments at the HWM could hold tens of MiB per peer).
- `RecvBytes`' multipart-message error now wraps `ErrFSMViolation`.
- `Poller`: a Poll waiting for PollIn on a REP socket that owed a reply now
  wakes when `SendMsg` makes an already-queued request receivable (it
  previously slept until its deadline).

## [1.0.0] - 2026-08-10

First stable release. From this tag, everything exported outside `internal/` is covered by the compatibility promise — with one documented exception: the methods in the `msg` package marked *"Internal plumbing — NOT covered by the compatibility promise"*, which are exported only so gozmq's own packages can reach them across a package boundary.

### Module path

- The module is now `github.com/exergy-dev/gozmq` (was `github.com/rkm/gozmq`). No tags had been published under the old path.

### Breaking changes — public API

The pre-1.0 window was the only chance to get internal types off the public surface. Four exported symbols were aliases to (or took) types under `internal/`, so pkg.go.dev rendered no method set, no struct fields, and parameter types callers could not name.

- **`Msg`**: `internal/msg` is now the public `msg` package. `gozmq.Msg` remains an alias, so **no application code changes** — but the method set is now documented in one place instead of hand-listed in prose. Methods exported only for cross-package internal use (`FromFramesOwned`, `Recycle`, `IsPooled`, `MarkSubscription`, `AppendPooledFrame`, `TransferPool`, `Reset`) each carry an explicit exclusion banner. The size-tiered frame pool moved to `internal/bufpool`, off the public surface entirely.
- **`SocketMetrics`** is a concrete struct in the root package instead of an alias to `iface.SocketMetrics`; its fields now render.
- **`Context.RegisterTransport`** takes `transport.Transport` instead of `internal/iface.Transport`. `Transport`, `Conn`, and `Listener` are defined in the public `transport` package and name only standard-library types, so out-of-tree transports (QUIC, WebSocket, fault-injecting harnesses) are now genuinely implementable — the package doc previously claimed the opposite.
- **`security`**: `SecurityMech` is now `security.Mech` (renamed to avoid `security.SecurityMech` stutter), and `Role`, `ZAPAuthorizer`, and `ZapConfigurable` likewise move to the public package. Constructors (`NewNull`, `NewPlainClient`, `NewPlainServer`, `NewCurveClient`, `NewCurveServer`, `NewMechBase`) now have nameable signatures.
- **`transport.InprocTransport`** is unexported; `NewInprocTransport` returns `Transport`. Its only extra method was the internal direct-pipe probe.

### Added

- **`Proxy(ctx, frontend, backend, capture)`** — the `zmq_proxy` equivalent, previously missing entirely. Moves whole multipart messages, so ROUTER envelopes and XSUB/XPUB subscription control messages survive the hop with no special casing. Half-duplex pairings only run the direction that can work; capture runs behind a bounded queue on its own goroutine so a slow tap cannot backpressure the data path. See `examples/proxy_xsub_xpub`.
- **`WithSecurityMech`** — installs a caller-supplied `security.Mech`. Rejected alongside the PLAIN/CURVE options and on STREAM sockets; a constructor error fails the session rather than downgrading to NULL.
- **`security.EncodeReady` / `DecodeReady` / `EncodeMetadata` / `DecodeMetadata`** — the ZMTP READY and property-list wire format, without which an out-of-tree mechanism could not produce a valid handshake. NULL now uses the same helpers.
- **`docs/COMPATIBILITY.md`** — the full libzmq inventory: socket types, transports, security mechanisms, a three-way split of `zmq_setsockopt` options (supported / not-applicable-in-Go / not implemented, each with a workaround), and every intentional behavioural divergence in one place.
- **`SECURITY.md`**, **`CONTRIBUTING.md`**, **`docs/RELEASING.md`**, and GitHub issue templates.
- **`soak/`** — a build-tagged (`soak`) stability suite that churns connections, subscribers, and aborted handshakes, then asserts goroutine count and heap have returned to baseline. Configurable via `GOZMQ_SOAK_DURATION`; nightly CI runs it for ten minutes.

### Fixed

- **Socket leak on a long-lived Context.** `NewSocket` appended each socket to `Context.sockets` and nothing ever removed one on `Close`, so a Context — the one-per-process pattern the docs recommend — retained every socket it had ever created, along with each socket's config, engine, and engine-owned maps and channels. Roughly 2 KiB per socket: a connection-churning server leaked ~150 MiB per 75k cycles, unbounded. Found by the new soak suite; guarded in the fast suite by `TestContextReleasesClosedSockets`.
- **Unguarded command-name length.** The session writer open-codes the ZMTP command encoding to control buffer ownership but omitted the length guard `zmtp.EncodeCommand` enforces; a name over 255 bytes would have been silently truncated into a malformed frame. Now panics, matching the codec's contract.
- **PLAIN authentication failures against a real libzmq server surfaced as malformed frames.** libzmq's `plain_server_t::produce_error` writes the C literal `"\x5ERROR"` instead of `"\5ERROR"`; the hex escape swallows the `E`, so the ERROR command declares a 94-byte name with four bytes behind it. gozmq now accepts that exact sequence — matched in full, only after the strict parse fails — and reports `ErrAuthFailed`. Confirmed against libzmq 4.3.4 and covered by the interop suite. No other malformed command name is accepted.
- **ROUTER identity handover was misdocumented.** `internal/socket/identity.go` described last-writer-wins handover as "matching libzmq"; it is the opposite — libzmq defaults to `ZMQ_ROUTER_HANDOVER=0`, which rejects the new peer. gozmq's behaviour is the better default, but two clients sharing a `WithIdentity` value silently evict each other, and nothing said so. Now documented in `doc.go` and the compatibility matrix.

### Changed

- **CI**: the test matrix now covers the `go.mod` minimum (1.22.x) plus `oldstable` and `stable` — previously only 1.22.x ran, so a break on current Go would have shipped unnoticed. `-race` runs `./...` instead of a hand-listed package set that had already fallen out of date. New lint job and an 80% coverage gate over library packages. Cross-build adds linux/arm64, darwin/arm64, and freebsd/amd64. A new nightly workflow runs 10m-per-target fuzzing, five race passes, the soak suite, and repeated interop runs.
- **golangci-lint** is now enforced. The first pass reported 82 issues; all are resolved. Two were real (above); the rest were mechanical or suppressed with the rationale recorded in `.golangci.yml`.
- `README.md` rewritten for release: measured libzmq comparison, compatibility link, stability statement, and a `Proxy` example. The README's snippets are compiler-verified as testable examples.

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
