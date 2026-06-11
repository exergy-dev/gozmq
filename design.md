# gozmq: A Pure Go ZeroMQ Library — Design Document

**Status:** Current
**Author:** TBD
**Last updated:** 2026-06-10

## 1. Overview

`gozmq` is a pure-Go implementation of the ZeroMQ messaging patterns, wire protocol (ZMTP 3.1), and transports. Unlike `pebbe/zmq4` (CGO wrapper over libzmq) and `goczmq` (CGO wrapper over CZMQ), `gozmq` has no C dependencies. Unlike the existing pure-Go `go-zeromq/zmq4`, it targets full ZMTP 3.1 compliance, all standard socket types (draft socket types are out of scope; see §2.1), all practical transports, all three practical security mechanisms (including a correct CURVE implementation), and production-grade operational characteristics (backpressure, reconnection, monitoring, observability).

The target consumer is a Go application author who wants ZeroMQ semantics without linking libzmq, without CGO-related build/deployment complexity, and without the feature gaps that currently make pure-Go options unsuitable for production interop with heterogeneous ZMQ deployments.

## 2. Goals and Non-Goals

### 2.1 Goals

- **Wire compatibility** with libzmq ≥ 4.2 over TCP, IPC, and inproc. A `gozmq` REQ socket must interoperate with a libzmq REP socket bit-for-bit and vice versa.
- **Full standard socket type coverage**: `REQ`, `REP`, `DEALER`, `ROUTER`, `PUB`, `SUB`, `XPUB`, `XSUB`, `PUSH`, `PULL`, `PAIR`, `STREAM`. Draft socket types (`SERVER`/`CLIENT`, `RADIO`/`DISH`, `SCATTER`/`GATHER`, `PEER`, `CHANNEL`) are out of scope.
- **Full transport coverage**: `tcp://`, `tcp6://`, `ipc://` (Unix domain sockets), `inproc://` (in-process), `tls+tcp://` (our extension, see §7.5). Multicast (`pgm`, `epgm`) is explicitly deferred; see §2.2.
- **All three ZMTP security mechanisms**: `NULL`, `PLAIN`, and `CURVE` (CurveZMQ, using `golang.org/x/crypto/nacl/box` primitives).
- **Correct socket semantics**: fair-queued inbound, load-balanced or routed outbound per socket type; subscription forwarding for XPUB/XSUB; routing-identity round-tripping for ROUTER; request/reply state machine for REQ/REP.
- **Production operational behavior**: high-water-mark backpressure, reconnection with exponential backoff and jitter, connect/disconnect/accept monitoring events, context-based cancellation, graceful `LINGER` semantics on close.
- **Idiomatic Go API**: `io.Reader`/`io.Writer`-adjacent where it makes sense, `context.Context` for cancellation, typed errors, zero-value-useful structs, no globals that aren't explicitly opt-in.
- **Zero CGO**, builds with `CGO_ENABLED=0`, cross-compiles cleanly to all Go-supported platforms.
- **Performance** within 2× of libzmq for the common TCP/inproc hot paths (PUB/SUB fanout, PUSH/PULL pipeline, REQ/REP round-trip). See §11.

### 2.2 Non-Goals

- **Transport and mechanism gaps (deliberate, for now).** Relative to the full libzmq surface, the following are intentional non-goals at present:
  - `ws://` / `wss://` (WebSocket transport, ZMTP-over-WS);
  - `udp://` (and the draft `RADIO`/`DISH` pattern it serves);
  - `pgm://` / `epgm://` reliable multicast (requires an OpenPGM-equivalent stack with no mature pure-Go counterpart);
  - `vmci://` (VMware sockets) and `tipc://`;
  - the `GSSAPI` security mechanism (Kerberos).

  None of these are architecturally precluded — the internal `Transport` and `SecurityMech` interfaces can absorb them later (§7.5) — but they are deferred until demand justifies the work.
- **libzmq API parity at the C level.** We mirror semantics, not C identifiers or option numeric codes.
- **Being a drop-in replacement for `pebbe/zmq4`.** Migration will require code changes; see §6.1 for the API philosophy.
- **Supporting ZMTP 2.0 and older.** We negotiate down to ZMTP 3.0 if the peer advertises it, but ZMTP 1.0/2.0 framing is out of scope.

## 3. Background

### 3.1 ZeroMQ in one paragraph

ZeroMQ is a messaging library providing message-oriented sockets that encapsulate a set of high-level communication patterns (request/reply, publish/subscribe, push/pull, etc.). A single ZMQ socket can be `bind()`-ed to one or more endpoints and `connect()`-ed to many others; the library multiplexes underlying connections and applies a pattern-specific policy (fair queueing, load balancing, routing) when messages flow in either direction. Messages are multi-part sequences of binary frames.

### 3.2 ZMTP 3.1

ZMTP (ZeroMQ Message Transport Protocol) is the wire protocol. Key elements:

- A **greeting** (signature, version, security mechanism name, server/client flag, filler).
- A **handshake** whose contents depend on the negotiated security mechanism; at minimum this exchanges a `READY` command carrying metadata like `Socket-Type`, `Identity`, and `Resource`.
- A **traffic phase** of length-prefixed frames with a `MORE` flag for multipart, a `LONG` flag for frames ≥ 256 bytes, and a `COMMAND` flag distinguishing control frames (`PING`, `PONG`, `SUBSCRIBE`, `CANCEL`, `ERROR`, `DISCONNECT`) from user messages.

ZMTP 3.1 adds heartbeats (`PING`/`PONG` with configurable TTL and timeout) and the `DISCONNECT` command.

### 3.3 Survey of existing Go options

| Library | Approach | Gaps that motivate `gozmq` |
|---|---|---|
| `pebbe/zmq4` | CGO over libzmq | Requires libzmq at build and runtime; cross-compilation pain; CGO deployment complexity. |
| `zeromq/goczmq` | CGO over CZMQ | Same as above, plus extra CZMQ dependency. |
| `go-zeromq/zmq4` | Pure Go | Partial socket coverage (draft sockets absent), incomplete/untested CURVE, limited transport work, limited heartbeat support, limited monitoring. |

The rest of the ecosystem is abandoned or experimental.

## 4. Architecture

### 4.1 Layering

```
┌──────────────────────────────────────────────────────────────┐
│  Public API        (Socket, Context, Msg, options, events)   │
├──────────────────────────────────────────────────────────────┤
│  Socket Engines    (per-type policy: REQ, ROUTER, PUB, …)    │
├──────────────────────────────────────────────────────────────┤
│  Pipe Layer        (bounded in-memory queues, HWM, flow ctl) │
├──────────────────────────────────────────────────────────────┤
│  Session Layer     (1 session per peer: ZMTP state machine)  │
├──────────────────────────────────────────────────────────────┤
│  Security Layer    (NULL / PLAIN / CURVE handshake + codec)  │
├──────────────────────────────────────────────────────────────┤
│  Framing Layer     (ZMTP frame encode/decode, heartbeats)    │
├──────────────────────────────────────────────────────────────┤
│  Transport Layer   (tcp, ipc, inproc, tls+tcp)               │
└──────────────────────────────────────────────────────────────┘
```

Each layer has a narrow interface and a dedicated test suite. Layers never reach across more than one boundary.

### 4.2 Core concepts

- **Socket**: the user-facing object. Holds configuration, a socket engine, and a set of pipes (one per connected peer).
- **Pipe**: a bidirectional bounded queue between a socket engine and a session. Each direction has an independent HWM. Pipes are where backpressure is expressed.
- **Session**: owns one peer connection end-to-end. Runs the ZMTP state machine, owns the security codec, translates between framed bytes on the wire and `Msg` structs on the pipe.
- **Transport**: turns an endpoint string (`tcp://host:port`, `ipc:///run/foo.sock`, `inproc://name`) into something connection-like. Hides listener vs. dialer.
- **Context**: a top-level object owning shared resources (the transport registry, including the inproc name table) and the defaults sockets inherit. Multiple Contexts can coexist; they share nothing. (The multi-socket `Poller` is a separate, caller-constructed type — not owned by the Context.)

### 4.3 Concurrency model

Go's runtime makes the classic libzmq "one I/O thread, one reactor" architecture unnecessary. Instead:

- Each **session** owns two goroutines: a reader (wire → pipe in-bound) and a writer (pipe out-bound → wire), plus an optional heartbeat scheduler when both peers are ZMTP 3.1. Shared state both touch (last-inbound timestamp, handshake error) uses `sync/atomic` rather than a coarse mutex; a small `writeMu` serialises the writer, PONG, and PING writes to the conn.
- **Socket engines** are goroutine-frugal. Inbound fair-queueing (`FairQueue`, used by PULL/DEALER/REP/ROUTER/SUB/XSUB) spawns one lightweight forwarder goroutine per attached peer, and **zero** when exactly one peer is attached (a lock-free direct path). Outbound load-balancing (`LoadBalancer`, used by PUSH/DEALER) runs **no** steady-state goroutines — it inlines the send onto the peer's channel. REQ reads its sticky reply pipe inline (no per-request goroutine); PUB runs one reader goroutine per subscriber to absorb SUBSCRIBE/CANCEL.
- **Public API calls** (`Send`, `Recv`) are thread-safe but serialize per socket. Concurrent senders on the same socket are supported but not encouraged — prefer one goroutine per socket, as with libzmq.

This gives us the Go idiom (goroutines + channels + `context.Context`) while preserving the ZeroMQ guarantee that a socket is conceptually single-threaded from the user's point of view.

## 5. Protocol Implementation

### 5.1 Greeting

```
  0                   1                   2                   3
  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 +---------------+               +---------------+---------------+
 |    0xFF       |  8 bytes pad  |    0x7F       |   version     |
 +---------------+---------------+---------------+---------------+
 |       mechanism (20 bytes, ASCII, null-padded)                |
 +---------------------------------------------------------------+
 |  as-server   |              filler (31 bytes)                 |
 +--------------+------------------------------------------------+
```

Version is `0x03 0x01`. Mechanism is `"NULL"`, `"PLAIN"`, or `"CURVE"`. `as-server` is 1 if this peer acts as the server in the chosen mechanism, else 0.

We negotiate down to 3.0 by inspecting the peer's minor version; if the peer is 3.0, we disable heartbeats.

### 5.2 Framing

Every frame starts with a flags byte:

- `bit 0` — `MORE`: another frame follows in the current logical message.
- `bit 1` — `LONG`: length is 8 bytes (big-endian) instead of 1.
- `bit 2` — `COMMAND`: this is a ZMTP command, not a user message frame.

Then the length, then the body. Commands have a name-length-prefixed name followed by a body. We implement `READY`, `ERROR`, `SUBSCRIBE`, `CANCEL`, `PING`, `PONG`, and `DISCONNECT`.

### 5.3 Handshake

After the greeting:

1. Security mechanism exchange (§8).
2. Metadata exchange via `READY` command. Metadata properties include `Socket-Type`, `Identity` (optional), `Resource` (optional). We validate that the peer's `Socket-Type` is compatible with ours (e.g., a REQ peer must match a REP or ROUTER, never another REQ).
3. Transition to traffic phase.

### 5.4 Heartbeats

When both peers are ZMTP 3.1, each session can send `PING` every `HeartbeatInterval` (default 30s) with a TTL. If no frame (data or `PONG`) is received within `HeartbeatTimeout` (default 90s — 3× the interval), the session is torn down and reconnection (if applicable) begins. A session parked on local backpressure (its inbound pipe at HWM) is not treated as peer-dead.

Heartbeat frames and user frames share the same read loop; the decoder distinguishes them via the `COMMAND` flag.

### 5.5 Traffic phase

Inbound frames are accumulated into a `Msg` (a slice of `[]byte` frames) until a frame without `MORE` arrives, then the complete `Msg` is pushed onto the inbound pipe. Accumulation is bounded by `MaxMessageSize` across the whole multipart message (not just per frame); a peer exceeding the cap has its session torn down, preventing unbounded-allocation DoS. Outbound pipes accept a full `Msg` and the writer emits frames with `MORE` set on all but the last.

## 6. Public API

### 6.1 Philosophy

We do not mimic the libzmq C API. We design for Go. That means:

- Constructors that return `(*Socket, error)`.
- `context.Context` on every blocking call that accepts one.
- Typed option structs, not integer option codes.
- Methods over getters/setters where it reads naturally.
- Errors wrap sentinel values (`errors.Is` works).

### 6.2 Package layout

```
github.com/.../gozmq
├── gozmq.go               // SocketType, Msg aliases, emit/logger helpers
├── context.go             // Context lifecycle: NewContext, Close, RegisterTransport, ZAP
├── socket.go              // Socket: Bind/Connect/SendMsg/RecvMsg/Subscribe/Metrics
├── wiring.go              // per-socket session + security mechanism construction
├── directpipe.go          // inproc direct-pipe bypass + STREAM adapter wiring
├── close.go               // LINGER teardown (Close / CloseCtx)
├── peerlink.go            // unified peer-teardown contract (session/pipe/stream)
├── options.go             // typed option structs, defaults
├── errors.go              // sentinel errors (aliases of internal/zmqerr)
├── events.go              // monitoring events
├── poller.go              // multi-socket readiness Poller
├── transport/             // tcp, tcp6, ipc, inproc, tls (per-Context registry)
├── security/              // NULL, PLAIN mechanisms + RFC 27 ZAP codec
│   └── curve/             // CurveZMQ mechanism + Z85 public-key helpers
├── typed/                 // generics convenience wrappers (Sender/Receiver/…)
└── internal/
    ├── iface/             // cross-layer contracts (the dependency sink)
    ├── socket/            // socket engines + shared LoadBalancer / FairQueue / SubTree
    ├── zmtp/              // greeting, framing, commands, metadata
    ├── session/           // handshake, reader/writer, heartbeat, reconnect
    ├── pipe/              // bounded bidirectional queues (HWM)
    ├── msg/               // Msg type + size-tiered frame-buffer pool
    ├── stream/            // raw-bytes adapter for STREAM sockets
    └── zmqerr/            // sentinel error definitions
```

NULL and PLAIN live in the public `security/` package (alongside ZAP), not
under `internal/` — the security mechanisms and transports are an intentional
first-party closed set (see §7.5), but their entry points are public so callers
can reference the option helpers and ZAP types.

### 6.3 Sketch

```go
ctx := gozmq.NewContext()
defer ctx.Close()

sock, err := ctx.NewSocket(gozmq.Router,
    gozmq.WithIdentity([]byte("frontend-1")),
    gozmq.WithSendHWM(10_000),
    gozmq.WithRecvHWM(10_000),
    gozmq.WithHeartbeat(30*time.Second, 90*time.Second),
)
if err != nil { /* ... */ }
defer sock.Close()

if err := sock.Bind("tcp://*:5555"); err != nil { /* ... */ }

for {
    msg, err := sock.RecvMsg(ctx.Ctx())
    if err != nil { /* ... */ }
    // msg.Frames() is [][]byte; for ROUTER, msg.Frames()[0] is the peer identity
    if err := sock.SendMsg(ctx.Ctx(), reply); err != nil { /* ... */ }
}
```

### 6.4 Options

Every option is a typed `Option` passed to a socket constructor. Representative set (non-exhaustive):

`WithIdentity`, `WithSendHWM`, `WithRecvHWM`, `WithLinger`, `WithReconnectInterval`, `WithReconnectIntervalMax`, `WithHeartbeat`, `WithMaxMessageSize`, `WithRouterMandatory`, `WithXPubVerbose`, `WithSubscribe`, `WithPlainAuth`, `WithCurveServer`, `WithCurveClient`, `WithHandshakeTimeout`, `WithMonitor`.

Defaults match libzmq defaults unless libzmq's default is hostile (e.g., infinite LINGER on close — we default to 1 second and require explicit opt-in for infinite).

### 6.5 Message type

```go
type Msg struct {
    frames [][]byte
    // internally: pooled-buffer bookkeeping + an atomic refcount
    // (see internal/msg).
}

func (m *Msg) Frames() [][]byte
func (m *Msg) NumFrames() int
func (m *Msg) Bytes() []byte              // single-frame shortcut (panics on multipart)
func (m *Msg) SingleFrame() ([]byte, bool)
func (m *Msg) TotalSize() int
func (m *Msg) AppendFrame(b []byte)
func (m *Msg) Retain()
func (m *Msg) Release()
func NewMsg() *Msg
func NewMsgFromBytes(b []byte) *Msg
func NewMsgFromFrames(fs ...[]byte) *Msg
```

**Ownership model (as shipped).** Frames delivered by `RecvMsg` are caller-owned: they remain valid indefinitely and may be retained, stashed, or passed between goroutines — there is no "valid until the next call" window and no `Clone()` requirement. Reclamation is refcount-driven:

- Most `*Msg` values carry ordinary heap-allocated frames and need no explicit reclamation; `Release` on them is a no-op.
- The session reader backs *large* inbound frames with buffers from a size-tiered pool (`internal/msg.PoolGet`/`PoolPut`). The first pooled frame installs a refcount of 1; the consumer calls `Release` when done, and when the count reaches zero the pooled buffers return to the pool. `Retain` adds a reference for cross-goroutine shares.
- Calling `Release` unconditionally on every received `*Msg` is always safe and is the documented best practice. A runtime-finalizer safety net reclaims pooled buffers for callers that forget, but it is not the primary path.
- Fan-out is refcount-aware: when an engine (PUB/XPUB) dispatches one `*Msg` to N pipes it takes N references, and the N session writers recycle concurrently post-flush — exactly one (the last) reclaims. `TransferPool` moves pooled-buffer ownership when engines rewrite envelopes (REQ/REP/ROUTER strip/prepend), preventing use-after-free across the rewrite.
- Single-frame convenience: `Socket.RecvBytes` copies the payload and releases internally, so callers who want zero bookkeeping have a misuse-proof path.

## 7. Transports

### 7.1 TCP

Standard `net.Dial` / `net.Listen` on `tcp4` or `tcp6`. Wildcard binding (`tcp://*:port`) listens on all interfaces. Port 0 selects an ephemeral port; the actual bound endpoint is recoverable via `sock.LastEndpoint()`.

Per-connection tuning lives in the concrete transport constructors.

### 7.2 IPC

Unix domain sockets. Abstract namespace supported on Linux via `@name` syntax (matching libzmq). Socket file is removed on listener close if we created it. Permissions are configurable (`WithIPCFilePerm`).

### 7.3 inproc

In-process transport. A Context holds a map from name → bound socket's pipe factory. `connect("inproc://foo")` looks up the bound socket in the same Context and creates a paired pipe directly — no goroutine-per-byte copying, no ZMTP framing overhead, no security layer. Subtlety: libzmq requires `bind` before `connect` for inproc; we relax this by queueing connect-before-bind attempts for a configurable window, then erroring.

### 7.4 TLS (tcp+tls)

Not in ZMTP proper, but essential for modern deployments and currently requires ad-hoc wrapping elsewhere. We expose `tls+tcp://host:port` which wraps the TCP connection in `crypto/tls` with a user-supplied `*tls.Config`. This runs **underneath** the ZMTP security mechanism, so `tls+tcp` + `CURVE` is legal and reasonable (defense in depth; enterprise crypto review for TLS, ZMQ-level mutual auth via CURVE).

### 7.5 Deferred

PGM/EPGM, VMCI, NORM. The internal `Transport` interface can accept these later without API churn — but transports are an intentionally **closed, first-party set**: `iface.Transport` lives under `internal/`, so adding a transport is an in-module change, not a third-party plugin point. `Context.RegisterTransport` exists to inject configuration into the built-in transports (chiefly TLS's `*tls.Config`), not to register arbitrary external transports. The same applies to security mechanisms (`iface.SecurityMech`): ZMTP defines exactly NULL/PLAIN/CURVE and all three are first-party; the supported extension point for authorization is the ZAP handler, not a custom mechanism.

## 8. Security

### 8.1 NULL

Trivial: after the greeting, each side sends a `READY` command with metadata. No cryptographic material. Default mechanism.

### 8.2 PLAIN

Client sends `HELLO` with `username` and `password`. Server responds with `WELCOME` (on success) or `ERROR` (on failure), then the client sends `INITIATE` with metadata and the server finishes with `READY` metadata. Credentials are validated via a user-supplied callback:

```go
gozmq.WithPlainAuth(func(user, pass string) bool { ... })
```

### 8.3 CURVE

CurveZMQ ([rfc.zeromq.org/spec/26](https://rfc.zeromq.org/spec/26)) is an authenticated encryption protocol built on the NaCl `crypto_box` primitive (Curve25519 + XSalsa20 + Poly1305). It has five handshake commands: `HELLO`, `WELCOME`, `INITIATE`, `READY`, `MESSAGE`. It provides forward secrecy via ephemeral keys and mutual authentication via long-term public keys.

Implementation uses `golang.org/x/crypto/nacl/box` for the `crypto_box` primitive directly — **not** a bespoke crypto reimplementation. The CurveZMQ codec logic (nonce format, cookie box, envelope) is ours; the primitive operations are not.

Key management:

- Long-term keys generated with `curve.GenerateKeyPair() (pub, priv Key, err error)`.
- Z85 encoding/decoding for human-readable key transport, matching libzmq's format.
- Optional ZAP (ZeroMQ Authentication Protocol) integration for authorization by public key (§8.4).

Test strategy for CURVE is unusually important; see §10.3.

### 8.4 ZAP

The ZeroMQ Authentication Protocol ([rfc.zeromq.org/spec/27](https://rfc.zeromq.org/spec/27)) is an inproc authorization hook. A ZAP handler is a socket bound to `inproc://zeromq.zap.01` that answers authentication queries. We implement both the authorizer side (user can register a ZAP handler with a Context) and the authenticatee side (our sessions consult the ZAP handler when the security mechanism requests it).

## 9. Socket Type Semantics

Each socket type gets a dedicated engine. Representative behaviors:

- **REQ / REP**: strict lock-step. REQ auto-prepends an empty delimiter frame. REP records the peer identity stack on receive and replays it on send. State machine enforces alternation; out-of-order `Send`/`Recv` returns `ErrFSMViolation`.
- **DEALER / ROUTER**: DEALER load-balances outbound across connected peers round-robin, fair-queues inbound. The load balancer rotates past peers whose pipe is at HWM rather than head-of-line blocking on the cursor peer (matching libzmq's `lb_t`); only when *every* peer is full does Send block. ROUTER prepends the peer identity to inbound messages and routes outbound by the leading identity frame. `WithRouterMandatory` makes unroutable sends return an error instead of silently dropping.
- **PUB / SUB**: PUB fans out to all subscribed peers, filtering by subscription prefix maintained per-peer. SUB maintains its subscription list and forwards `SUBSCRIBE`/`CANCEL` commands to every connected PUB.
- **XPUB / XSUB**: identical wire behavior but with subscriptions surfaced as normal messages to the application. `WithXPubVerbose` disables the default dedup. XPUB synthesizes `CANCEL` messages when a departing peer's subscriptions drop to refcount zero (libzmq parity).
- **PUSH / PULL**: PUSH load-balances, PULL fair-queues. No identity.
- **PAIR**: exclusive 1:1.
- **STREAM**: raw TCP bytes, no ZMTP on the wire. Each inbound frame is prefixed with a peer identity so a single STREAM socket can multiplex many clients. Useful for speaking non-ZMQ protocols (HTTP, SMTP) from a ZMQ pipeline.

## 10. Testing Strategy

### 10.1 Unit tests

Each layer has unit tests. ZMTP framing has table-driven encode/decode tests with fixtures. The pipe layer has property-based tests (via `testing/quick` or `pgregory.net/rapid`) for HWM invariants: total messages in flight never exceeds HWM, blocked senders unblock within bounded time after a receive.

### 10.2 Interop tests

The most important test suite. For every standard socket type, every transport, and every security mechanism, we run cross-implementation tests:

- `gozmq` client ↔ `gozmq` server (baseline).
- `gozmq` client ↔ libzmq server (via a small CGO test harness, isolated behind a build tag so the main module stays CGO-free).
- libzmq client ↔ `gozmq` server.

The CGO harness lives under `interop/` and is only built when `-tags interop` is set. CI (`.github/workflows/ci.yml`) runs five jobs:

- **test** — `go mod tidy` verification, `go vet ./...`, a `CGO_ENABLED=0` build (the whole point of the project), and `go test ./...`;
- **race** — `go test -race` over the full core package list (root, `internal/session`, `internal/socket`, `internal/pipe`, `internal/msg`, `internal/stream`, `transport`, `internal/zmtp`, `security`, `security/curve`, `typed`, `typed/codecjson`);
- **cross-build** — pure-Go (`CGO_ENABLED=0`) builds for linux/386, darwin/amd64, and windows/amd64;
- **fuzz-smoke** — discovers every `Fuzz*` target and runs each for 10s;
- **interop** — installs `libzmq3-dev` and runs `go test -tags interop -race ./interop/...`.

### 10.3 CURVE test vectors

CurveZMQ is easy to get subtly wrong in ways that only show up against other implementations. We lift the test vectors from libzmq's `tests/test_security_curve.cpp` where available, author our own for the handshake message-box contents, and assert byte-exact agreement.

### 10.4 Fuzzing

Go's native fuzzing (`go test -fuzz`) targets: the greeting parser, the frame parser, the command parser, the CurveZMQ envelope parser. Corpus seeded with traces captured from libzmq.

### 10.5 Race and leak detection

CI runs the core package list under `-race` in a dedicated job (see §10.2); the plain test job covers every package without the race detector. Goroutine leak detection via `go.uber.org/goleak` in test `TestMain`s ensures sockets shut down cleanly.

### 10.6 Benchmarks

Benchmark suite mirroring libzmq's `perf/` directory: `local_lat`, `remote_lat`, `local_thr`, `remote_thr` for inproc, TCP, and IPC. Results are tracked per-commit via benchmark CI to catch regressions.

## 11. Performance Considerations

- **Frame pooling**: a per-Context `sync.Pool` of `[]byte` buffers for frame bodies to amortize allocation. The pool is sized-tiered (common sizes cached) to avoid wasting memory on oversized buffers.
- **Batched writes**: the session writer coalesces up to *N* frames from the outbound pipe into a single `writev`-style call (`net.Buffers` does this natively on TCP and IPC).
- **Zero-copy inproc**: inproc pipes pass `*Msg` directly, no framing, no copy. The only cost is channel synchronization.
- **HWM as a channel capacity**: bounded Go channels are the native expression of HWM, not something we bolt on.
- **Goroutine count**: 2 per session (reader + writer, plus an optional heartbeat) + roughly one inbound forwarder per attached peer for fair-queued patterns + 1 per listener. Outbound load-balancing and the single-peer inbound fast path add none. For a ROUTER serving 10 000 peers this is ~30 000 goroutines — acceptable in Go but documented as the scaling cost. A future optimization pools sessions onto shared I/O goroutines for the 100k+ peer case.

We target ≥ 1M msg/s on inproc, ≥ 500k msg/s on TCP loopback with small frames, and < 10 µs median round-trip REQ/REP on inproc. These are aspirational numbers to be validated against prototype benchmarks.

## 12. Monitoring and Observability

Every socket supports a monitor channel:

```go
events := make(chan gozmq.Event, 64)
sock.Monitor(events, gozmq.EventAll)
```

Events cover: `Connected`, `ConnectDelayed`, `ConnectRetried`, `ConnectFailed`, `Listening`, `BindFailed`, `Accepted`, `AcceptFailed`, `Closed`, `Disconnected`, `HandshakeSucceeded`, `HandshakeFailedProtocol`, `HandshakeFailedAuth`. Each event carries the endpoint, a timestamp, and a free-text `Reason` string (the underlying error string for failures; the pending backoff duration for `ConnectDelayed`/`ConnectRetried`; empty for unambiguous successes). `ConnectFailed` marks terminal connect failures with no reconnect loop behind them — an unknown transport scheme, or a STREAM dial failure (STREAM dials exactly once). There is no `CloseFailed` event.

Implemented observability surface:

- **Monitor events** (above) via `Socket.Monitor` / `WithMonitor` — the primary lifecycle signal.
- **Counter/gauge snapshots** via `Socket.Metrics()`, returning `SocketMetrics{Sent, Recv, Dropped, QueueDepth}` (atomic counters + a sampled queue-depth gauge). Drop semantics are documented on `SocketMetrics`.
- **Structured logging** via `WithLogger(*slog.Logger)` — internal diagnostics (handshake failures, reconnect attempts, heartbeat loss, teardown anomalies) are emitted with structured context; defaults to `slog.Default`.

Not implemented (possible future work, deliberately out of scope for now):

- `expvar` registration of the `Metrics()` counters. Today callers poll `Metrics()` and publish to whatever sink they use; gozmq does not register an `expvar` var itself.
- OpenTelemetry spans around socket lifecycle. Deferred to avoid forcing the dependency; callers can derive spans from monitor events.

## 13. Error Model

All errors are typed. Sentinels at the package root (`ErrContextClosed`, `ErrSocketClosed`, `ErrHWM`, `ErrFSMViolation`, `ErrHandshakeTimeout`, `ErrAuthFailed`, `ErrMalformedFrame`, `ErrProtocolVersion`, `ErrRouteUnknown`, …). Wrapped errors preserve the sentinel via `errors.Is`. Transport errors wrap the underlying `net.Error`; timeouts are detectable via the standard `Timeout() bool` interface.

Blocking calls that accept a `context.Context` return `ctx.Err()` on cancellation — no surprise `ErrInterrupted`.

## 14. Compatibility and Versioning

- Go 1.22+ (for `slog` in the stdlib and the `for range N` numeric range).
- `v0.x` pre-1.0 while socket semantics are being hammered against interop. API may change; changes documented in `CHANGELOG.md`.
- `v1.0` commits to API stability for the standard socket types, standard transports, and NULL/PLAIN/CURVE.
- Semantic versioning thereafter.

## 15. Milestones

1. **M1 — Scaffolding (2 weeks)**: Context, Socket skeleton, transport interface, TCP + inproc, NULL mechanism, PAIR socket, framing encode/decode with fuzz corpus. Deliverable: two PAIR sockets can round-trip a multipart message over TCP and inproc, with `-race` clean.
2. **M2 — Core patterns (4 weeks)**: REQ/REP, PUSH/PULL, PUB/SUB, DEALER/ROUTER, XPUB/XSUB. HWM, LINGER, reconnection with backoff. Interop tests against libzmq for each.
3. **M3 — Security (3 weeks)**: PLAIN, CURVE with test vectors, ZAP, handshake timeouts.
4. **M4 — Operational polish (3 weeks)**: monitoring events, `Metrics()` snapshots, `slog`, TLS transport, IPC transport, STREAM socket. (`expvar`/OTel integration deferred — see §12.)
5. **M5 — Perf (3 weeks)**: benchmark suite; performance tuning against numbers in §11.
6. **M6 — v1.0 release (2 weeks)**: API review, doc pass, migration guide from `pebbe/zmq4` and `go-zeromq/zmq4`, tagged release.

Total: ~17 weeks of focused work for a single engineer, less with parallelism (security can run concurrently with core patterns once M1 lands).

## 16. Open Questions

- **Async DNS**: libzmq resolves names lazily at connect time. Should we expose `WithResolver(*net.Resolver)` or defer?
- **Epoll-level I/O**: Go's netpoller is fine for tens of thousands of peers. For hundreds of thousands, is there value in a shared-goroutine I/O model, or do we declare that out of scope?
- **Auth plugins**: should `WithPlainAuth` accept a full `Authenticator` interface that covers PLAIN, CURVE key lookup, and ZAP uniformly? Probably yes — to be designed in M3.
- ~~**Message ownership**: returning pooled frames from `RecvMsg` is fast but invites use-after-free bugs. Options: (a) always copy, (b) pooled with `Clone()` for retention (current plan), (c) pooled with an explicit `Release()` call.~~ **Resolved — option (c) shipped**: refcounted `Retain`/`Release` with caller-owned frames, a finalizer safety net, and `RecvBytes` as the copy-always convenience. See §6.5.

## 17. References

- ZMTP 3.1: https://rfc.zeromq.org/spec/23
- CurveZMQ: https://rfc.zeromq.org/spec/26
- ZAP: https://rfc.zeromq.org/spec/27
- libzmq: https://github.com/zeromq/libzmq
- pebbe/zmq4: https://github.com/pebbe/zmq4
- go-zeromq/zmq4: https://github.com/go-zeromq/zmq4
