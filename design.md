# gozmq: A Pure Go ZeroMQ Library — Design Document

**Status:** Current
**Author:** TBD
**Last updated:** 2026-09-30

## 1. Overview

`gozmq` is a pure-Go implementation of the ZeroMQ messaging patterns, wire protocol (ZMTP 3.1), and transports. Unlike `pebbe/zmq4` (CGO wrapper over libzmq) and `goczmq` (CGO wrapper over CZMQ), `gozmq` has no C dependencies. Unlike the existing pure-Go `go-zeromq/zmq4`, it targets full ZMTP 3.1 compliance, all standard socket types (draft socket types are out of scope; see §2.1), all practical transports, all three practical security mechanisms (including a correct CURVE implementation), and production-grade operational characteristics (backpressure, reconnection, monitoring, observability).

The target consumer is a Go application author who wants ZeroMQ semantics without linking libzmq, without CGO-related build/deployment complexity, and without the feature gaps that currently make pure-Go options unsuitable for production interop with heterogeneous ZMQ deployments.

## 2. Goals and Non-Goals

### 2.1 Goals

- **Wire compatibility** with libzmq ≥ 4.2 over TCP and IPC, and libzmq's semantics on inproc (which has no wire format). A `gozmq` REQ socket must interoperate with a libzmq REP socket bit-for-bit and vice versa.
- **Full standard socket type coverage**: `REQ`, `REP`, `DEALER`, `ROUTER`, `PUB`, `SUB`, `XPUB`, `XSUB`, `PUSH`, `PULL`, `PAIR`, `STREAM`. Draft socket types (`SERVER`/`CLIENT`, `RADIO`/`DISH`, `SCATTER`/`GATHER`, `PEER`, `CHANNEL`) are out of scope.
- **Full transport coverage**: `tcp://`, `tcp6://`, `ipc://` (Unix domain sockets), `inproc://` (in-process), `tls+tcp://` (our extension, see §7.4). Multicast (`pgm`, `epgm`) is explicitly deferred; see §2.2.
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

  None of these are architecturally precluded — the public `transport.Transport` and `security.Mech` interfaces can carry them (§7.5) — but gozmq does not ship them until demand justifies the work.
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
│  Transport Layer   (tcp, tcp6, ipc, tls+tcp)                 │
└──────────────────────────────────────────────────────────────┘
```

Each layer has a narrow interface and a dedicated test suite. Layers never reach across more than one boundary. Two paths skip layers: inproc joins two engines with one pipe pair and has no session, security, framing or transport (§7.3), and a STREAM session runs in raw mode with no security or framing (§4.8).

### 4.2 Core concepts

- **Socket**: the user-facing object. Holds configuration, a socket engine, and a set of pipes (one per connected peer).
- **Pipe**: a bidirectional bounded queue (`internal/pipe.End`, one per side) between a socket engine and a session, or, on inproc, between two engines. Each direction has an independent HWM. Pipes are where backpressure is expressed.
- **Session**: owns one peer connection end-to-end. Runs the ZMTP state machine, owns the security codec, translates between framed bytes on the wire and `Msg` structs on the pipe. A STREAM session runs in raw mode instead (§4.8).
- **Transport**: turns an endpoint string (`tcp://host:port`, `ipc:///run/foo.sock`) into something connection-like. Hides listener vs. dialer. `inproc://` is not a transport; the Context joins inproc sockets itself (§7.3).
- **Link**: a socket's record of one peer — a session, or its side of an inproc connection — behind the one teardown contract that `Close` and `Disconnect` use (§4.7).
- **Context**: a top-level object owning shared resources (the scheme → transport table, the inproc registry, and the optional ZAP handler). Multiple Contexts can coexist; they share nothing. (The multi-socket `Poller` is a separate, caller-constructed type — not owned by the Context.)

### 4.3 Concurrency model

Go's runtime makes the classic libzmq "one I/O thread, one reactor" architecture unnecessary. Goroutines exist only where something blocks on I/O; everything else runs on the caller's goroutine or the sender's.

| Unit | Long-lived goroutines |
|---|---|
| ZMTP session (one per TCP/IPC/TLS connection, each side) | 2: reader and writer |
| `Connect` (non-STREAM) | 1 reconnector, for the life of the Connect |
| `Bind` on a transport | 1 accept loop |
| inproc link | 0 |
| STREAM connection | 2: the raw session's reader and writer (no reconnector) |
| Socket engines, pipes, FairQueue, LoadBalancer, Poller | 0 |

An idle TCP connection between two gozmq sockets therefore costs 5 goroutines counting both ends: a reader and a writer per side plus the client's reconnector. `gozmq_goroutines_test.go` enforces these budgets (5 per TCP connection, 0 per inproc link, 2 per STREAM connection); they may only be tightened.

- **Session.** The reader drives the greeting and security handshake, then moves wire frames to the pipe. It hands each outbound greeting and handshake frame to the writer over a small channel (`hsOut`), so a peer that writes before it reads cannot deadlock the handshake. The writer writes those frames in order, then moves pipe messages to the wire, so the last handshake frame always precedes traffic. PING/PONG go through the writer too (`cmdCh`); the writer is the only goroutine that writes to the conn. The heartbeat is a self-rearming `time.AfterFunc` timer, not a goroutine. A write error closes the conn so the reader unblocks; the last of the two goroutines to exit closes the session pipe, runs `OnClosed` and closes `Done`.
- **Callbacks, not supervisors.** The socket layer passes `OnReady(*pipe.End) error` and `OnClosed(err, attached)` in `session.Config`. `OnReady` runs on the reader after the handshake: it attaches the pipe to the engine (a refusal ends the session unattached) and emits the handshake and connect events. `OnClosed` detaches the pipe, emits `EventDisconnected` or the handshake-failure event, and removes the link. No goroutine waits on a session.
- **Reconnector.** One loop per `Connect` dials, starts a session through `Start(conn) (Runner, error)`, waits for the session's `Done`, and backs off (exponential with jitter, reset after an attach). `Quiesce` stops redialling while the current session drains on `Close`.
- **Accept loop.** It blocks in `Accept`; there is no polling. Each accepted conn starts a session directly. A per-connection accept error (EMFILE, an out-of-tree transport's per-connection error) is reported as `EventAcceptFailed` and retried with capped backoff (5 ms doubling to 1 s); only a closed listener or socket shutdown ends the loop.
- **Engines run no goroutines.** Receivers read the pipes themselves (§4.5), PUB/XPUB apply subscriptions on the application's own calls, and readiness is signalled by arming channels (§4.6). A pipe's notification runs on the sending goroutine.
- **Pipes hold no goroutines.** A pipe direction with a positive HWM is a buffered channel of that capacity. HWM 0 means unbounded: a 1024-slot channel plus a mutex-guarded overflow slice that the receiving end refills from after each receive, so an unbounded direction costs memory, not a relay goroutine.
- **Public API calls** (`SendMsg`, `RecvMsg`) are safe for concurrent use; each engine serialises what its pattern requires (REQ/REP reject a concurrent call that would break lock-step with `ErrFSMViolation`). Concurrent senders on one socket work but are not encouraged — prefer one goroutine per socket, as with libzmq.
- **Proxy** runs a goroutine per forwarding direction, plus one for the capture tap, for the duration of the call.

This gives us the Go idiom (goroutines + channels + `context.Context`) while preserving the ZeroMQ guarantee that a socket is conceptually single-threaded from the user's point of view.

### 4.4 Pipes and the idle notification

`pipe.NewPair(aToB, bToA, aInfo, bInfo)` returns two concrete `*pipe.End` values; engines, sessions and the socket layer all use the concrete type. Each `End` carries the peer's `PeerInfo` (its ZMTP `Identity`) and, once a ROUTER or STREAM engine has assigned one, its routing identity, so a message received after the pipe was detached is still stamped with the right peer.

A receiver that multiplexes many pipes must neither poll them nor run a goroutine per pipe to learn which has data. Each `End` therefore has an `idle` flag and a `notify` callback (`SetNotify`):

- `RecvOrIdle` takes a message, or marks the end idle and re-checks the queue.
- `MarkIdle` does the same without consuming, for readiness checks.
- After every enqueue, and on `Close`, the sender loads `idle`; only on a true→false CAS does it run `notify`, on its own goroutine. While the receiver is busy, a send costs one extra atomic load.

The receiver sets `idle` with a full barrier and only then re-checks; the sender publishes its message and only then loads `idle`. One of the two always sees the other, so no wakeup is lost. `Len` is a gauge for `Metrics`; `SendDrained` is the linger signal for inproc (§4.7).

### 4.5 Fair queueing

`FairQueue` (PULL, DEALER, REP, ROUTER, STREAM, SUB, XSUB, and the PUB/XPUB subscription path) follows libzmq's `fq_t`:

- The attached pipes sit in a slice whose first `nActive` entries are the active ones, those that may hold a message. Activating or deactivating swaps an entry across that boundary in O(1).
- A receiver walks the active entries round-robin under the queue's mutex with `RecvOrIdle`. A hit advances the cursor. An empty pipe is deactivated (it is now idle); a closed and empty one is removed.
- A pipe's notification reactivates its entry and arms the socket's in-gate. With nothing active, a receiver parks on that gate, the close channel and its context.
- **Single-pipe fast path.** While exactly one pipe is attached, `single` publishes it with a generation channel and `Recv` blocks in that pipe directly, without the mutex. Attaching or detaching a pipe closes the generation, which aborts receivers blocked in the old pipe so they re-evaluate.
- A pipe that closes with messages still queued stays attached until a receiver drains it to EOF, so a departed peer's messages are still delivered.

Receive cost depends on the active pipes, not the attached ones: a ROUTER with 1,000 idle peers receives about as fast as one with none. `QueueDepth` in `Metrics` is the sum of `Len()` over the inbound pipes. There are no FairQueue drops: a message leaves a pipe only for the receiver that returns it.

PAIR and REQ have one pipe at a time and register their in-gate directly as its notification. `LoadBalancer` (PUSH, DEALER) is the outbound counterpart, after libzmq's `lb_t`: it rotates past peers at HWM and blocks only when every peer is full.

### 4.6 Readiness and subscriptions

Each engine embeds one `gates` value: two cap-1 channels, `in` and `out`, returned by `Engine.Gates()`. A token in a gate is a hint that the direction may have become ready. Pipe notifications arm `in`; engines arm `out` on attach and when REQ/REP enter a can-send state, and `in` when REP enters can-recv or an XPUB event is queued. `CanRecvNow`/`CanSendNow` are level checks, and a false `CanRecvNow` also marks the empty pipes idle, so the next message arms the gate. The `Poller` parks on the gates with one `reflect.Select` and re-checks with those methods; an engine's own blocked receivers park on the same in-gate. A waiter that takes the token and finds the direction ready puts it back for the next one. `broadcast`, a close-and-replace channel, wakes every waiter at once where a single token would not do (LoadBalancer, PAIR, REQ, FairQueue generations).

**Subscriptions on PUB/XPUB.** The session turns inbound ZMTP SUBSCRIBE/CANCEL commands into `0x01`/`0x00`-prefixed messages on the pipe, but only for PUB and XPUB; every other socket type drops them. The PUB engine fair-queues its peers' inbound pipes. Queued commands are applied to each peer's `SubTree` on the socket's next `Send` — and for XPUB also on `Recv` and on a readiness check — as libzmq applies them on its next send, recv or poll. When nothing is queued, `Send` pays one atomic load (`FairQueue.Pending`). XPUB settles its refcounts as each command is applied and queues the surfacing events without a bound. `DetachPipe` applies a departing peer's queued commands before XPUB synthesizes its CANCELs. The consequence is that a PUB that never sends leaves subscriptions queued, and a subscriber's commands back up to the receive HWM until the next `Send`.

### 4.7 Links and teardown

A socket records every peer as a `link` in a map: a session (accepted, a reconnector's, or a STREAM Connect's), or its side of an inproc connection, including an inproc Connect still waiting for its Bind. A link holds the Connect endpoint (empty for peers that connected to us) and a peer with two methods:

- `Drain() <-chan struct{}` starts flushing what this side has queued and reports when it is done. A session cancels its writer's context, and the writer writes what is queued and exits, leaving the conn open. An inproc link returns `SendDrained`, which fires once the peer socket has received everything sent.
- `Stop() error` is the hard teardown: close the conn and wait for the session, or close the inproc pipe and detach both sides.

A link that ends on its own removes itself (a session in `OnClosed`, an inproc link when it closes). Whoever takes a link out of the map under the socket's mutex (`CloseCtx`, `Disconnect`) must stop it. Long-lived sockets therefore do not accumulate links under churn.

`CloseCtx` runs:

1. Quiesce the reconnectors: stop redialling, keep the current sessions.
2. Close the listeners and release the socket's inproc names.
3. `engine.Close`, so nothing new is queued.
4. Drain every link, bounded by `WithLinger` and the caller's context.
5. Cancel the run context, then stop the reconnectors and the links.
6. Wait for the accept loops, then emit `EventClosed`.

### 4.8 STREAM

A STREAM connection is an ordinary session started with `Config.Raw`. The mode is fixed when the session is built, so the ZMTP loops carry no branch for it. It has no greeting, handshake or heartbeat, and its pipe pair exists from construction:

- The raw reader calls `OnReady`, then publishes each `conn.Read` (up to 32 KiB) as one single-frame message, copying only the bytes read.
- The raw writer writes each message body verbatim. An empty body closes the conn. `Drain` flushes the queue as in ZMTP mode, so STREAM honours `WithLinger`.

The engine (`internal/socket/stream.go`) stamps and strips the routing identity like ROUTER. A STREAM `Connect` dials once and starts the session directly, with no reconnector, and `Disconnect` stops it like any other link. `WithSendHWM`/`WithRecvHWM` apply as given; 0 means unlimited.

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

We negotiate down to 3.0 by inspecting the peer's minor version; if the peer is 3.0, we disable heartbeats. A peer whose greeting announces a different mechanism from ours is rejected, as ZMTP and libzmq require.

### 5.2 Framing

Every frame starts with a flags byte:

- `bit 0` — `MORE`: another frame follows in the current logical message.
- `bit 1` — `LONG`: length is 8 bytes (big-endian) instead of 1.
- `bit 2` — `COMMAND`: this is a ZMTP command, not a user message frame.

Then the length, then the body. Commands have a name-length-prefixed name followed by a body. We implement `READY`, `ERROR`, `SUBSCRIBE`, `CANCEL`, `PING`, `PONG`, and `DISCONNECT`.

Under CURVE every traffic-phase frame, commands included, travels inside a `MESSAGE` box sent with outer flags `0x00`; `MORE` (`0x01`) and `COMMAND` (`0x02`) are carried in the encrypted inner flags byte, as libzmq does. On decode an outer `COMMAND` flag is still honoured, so commands from older gozmq peers are understood.

### 5.3 Handshake

After the greeting:

1. Security mechanism exchange (§8).
2. Metadata exchange via `READY` command. Metadata properties include `Socket-Type`, `Identity` (optional), `Resource` (optional). We validate that the peer's `Socket-Type` is compatible with ours (e.g., a REQ peer must match a REP or ROUTER, never another REQ).
3. Transition to traffic phase.

The whole handshake (greeting, security exchange, `READY`) is bounded by `WithHandshakeTimeout` (default 30s) through a deadline on the connection; for `tls+tcp` it also bounds the TLS handshake, which runs lazily on the first read or write. Handshake frames are read with a 64 KiB cap regardless of `WithMaxMessageSize`, so an unauthenticated peer cannot make the server allocate more.

### 5.4 Heartbeats

When both peers are ZMTP 3.1, each session can send `PING` every `HeartbeatInterval` (default 30s) with a TTL. If no frame (data or `PONG`) is received within `HeartbeatTimeout` (default 90s — 3× the interval), the session is torn down and reconnection (if applicable) begins. A timeout of 0 means 3× the interval. A session parked on local backpressure (its inbound pipe at HWM) is not treated as peer-dead. Our PINGs advertise our timeout as the TTL; the TTL in a peer's PING is currently ignored in favour of our own timeout.

Heartbeat frames and user frames share the same read loop; the decoder distinguishes them via the `COMMAND` flag.

### 5.5 Traffic phase

Inbound frames are accumulated into a `Msg` (a slice of `[]byte` frames) until a frame without `MORE` arrives, then the complete `Msg` is pushed onto the inbound pipe. Frames up to 4 KiB are read into a per-reader scratch buffer and copied out; larger frames are read straight into a fresh allocation (or, with `WithRecvBufferPool`, a pooled buffer; see §6.5). Accumulation is bounded by `MaxMessageSize` across the whole multipart message (not just per frame); a peer exceeding the cap has its session torn down, preventing unbounded-allocation DoS. Outbound pipes accept a full `Msg` and the writer emits frames with `MORE` set on all but the last.

## 6. Public API

### 6.1 Philosophy

We do not mimic the libzmq C API. We design for Go. That means:

- Constructors that return `(*Socket, error)`.
- `context.Context` on every blocking call that accepts one.
- Typed option structs, not integer option codes.
- Methods over getters/setters where it reads naturally.
- Errors wrap sentinel values (`errors.Is` works); see §13 for the exceptions.

### 6.2 Package layout

```
github.com/exergy-dev/gozmq
├── doc.go                 // package documentation
├── gozmq.go               // SocketType, the Msg alias and NewMsg* constructors, emit/logger helpers
├── context.go             // Context: NewContext, Close, RegisterTransport, WithZAPHandler
├── socket.go              // Socket: Bind/Connect/Unbind/Disconnect, Send/Recv, Subscribe, Metrics, Monitor; accept loop, startSession
├── wiring.go              // session configuration (ZMTP or STREAM raw mode) and security-mechanism selection
├── inproc.go              // the Context's inproc registry, pending Connects, inproc links
├── peerlink.go            // link: one teardown contract for sessions and inproc links
├── close.go               // LINGER teardown (Close / CloseCtx)
├── options.go             // With* socket options, defaults, validation
├── errors.go              // sentinel errors (aliases of internal/zmqerr)
├── events.go              // monitor events
├── poller.go              // multi-socket readiness Poller over the engines' gates
├── proxy.go               // Proxy (zmq_proxy)
├── msg/                   // Msg: frames, lent-frame reference counting
├── transport/             // Transport/Conn/Listener contracts; tcp, tcp6, ipc, tls+tcp
├── security/              // Mech contract, NULL, PLAIN, READY/metadata helpers, RFC 27 ZAP
│   └── curve/             // CurveZMQ mechanism + Z85 key helpers
├── typed/                 // generic codec-backed wrappers (Sender/Receiver/…/TopicMux)
│   └── codecjson/         // JSON codec
└── internal/
    ├── socket/            // per-pattern engines + shared LoadBalancer / FairQueue / SubTree / gates
    ├── session/           // reader/writer (ZMTP and raw mode), handshake, heartbeat timer, reconnector
    ├── pipe/              // pipe.End: bidirectional queue, per-direction HWM, idle notification
    ├── zmtp/              // frame codec, command codec, metadata property lists
    ├── bufpool/           // size-tiered buffers behind the opt-in WithRecvBufferPool
    ├── zmqerr/            // sentinel error definitions
    └── testutil/          // goleak helper for TestMain
```

The contracts that cross the public boundary live in public packages: `msg.Msg`, `transport.Transport`/`Conn`/`Listener`, and `security.Mech`/`Role`/`ZAPHandler` name only standard-library or public types, so no exported signature names an `internal/` type. Internal packages import each other directly; engines, sessions and the socket layer all use the concrete `*pipe.End` rather than a pipe interface.

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
    // msg.Frames() is [][]byte; for ROUTER, msg.Frames()[0] is the peer
    // identity, so sending msg back echoes it to the same peer.
    if err := sock.SendMsg(ctx.Ctx(), msg); err != nil { /* ... */ }
}
```

### 6.4 Options

Every option is a typed `Option` passed to a socket constructor. Representative set (non-exhaustive):

`WithIdentity`, `WithSendHWM`, `WithRecvHWM`, `WithLinger`, `WithReconnectInterval`, `WithReconnectIntervalMax`, `WithHeartbeat`, `WithMaxMessageSize`, `WithRouterMandatory`, `WithXPubVerbose`, `WithSubscribe`, `WithPlainAuth`, `WithCurveServer`, `WithCurveClient`, `WithHandshakeTimeout`, `WithMonitor`.

Defaults match libzmq defaults unless libzmq's default is hostile (e.g., infinite LINGER on close — we default to 1 second and require explicit opt-in for infinite).

### 6.5 Message type

`gozmq.Msg` is an alias for `msg.Msg`:

```go
type Msg struct {
    frames [][]byte
    // plus an inline one-frame array, and for lent messages a pointer to
    // the shared lease and a per-message reference count (see package msg)
}

func (m *Msg) Frames() [][]byte
func (m *Msg) NumFrames() int
func (m *Msg) Bytes() []byte              // single-frame shortcut (panics on multipart)
func (m *Msg) SingleFrame() ([]byte, bool)
func (m *Msg) TotalSize() int
func (m *Msg) AppendFrame(b []byte)
func (m *Msg) Lent() bool
func (m *Msg) Derive(frames ...[]byte) *Msg
func (m *Msg) Retain()
func (m *Msg) Release()

// package msg
func New() *Msg
func FromBytes(b []byte) *Msg
func FromFrames(frames ...[]byte) *Msg
func NewWithRelease(release func(), frames ...[]byte) *Msg

// package gozmq
func NewMsg() *Msg
func NewMsgFromBytes(b []byte) *Msg
func NewMsgFromFrames(frames ...[]byte) *Msg
```

**Ownership, by default.** Frames delivered by `RecvMsg` are ordinary Go memory owned by the caller. They stay valid for as long as the caller holds them, may be kept, sliced, re-sent or handed to another goroutine, and are reclaimed by the garbage collector. There is nothing to release: `Retain` and `Release` do nothing on such a message. Frames passed *into* a message (`FromBytes`, `AppendFrame`, `NewMsgFromFrames`) are held by reference, so the caller must not modify them while the message may still be in flight.

**Lent messages (opt-in).** A socket created with `WithRecvBufferPool(true)` receives frames larger than 4 KiB on NULL/PLAIN sessions into buffers from `internal/bufpool` (power-of-two tiers from 8 KiB to 4 MiB; larger frames fall back to `make`). Such a message is *lent* (`Lent()` reports true), with an explicit contract:

- The receiver holds one reference and calls `Release` when done; `Retain` adds a reference that needs its own `Release`.
- The frames, and any slice taken from them, are valid until the last reference is released: the message's own and one held by every message `Derive`d from it. Then the buffers go back to the pool and are reused by later receives.
- `SendMsg` takes its own reference (via `Derive`) and releases it once the message is written, or, for an inproc peer, when the receiver releases it. `RecvMsg`, `SendMsg`, `Release` is therefore correct whatever the order relative to the actual write.
- Engines that rewrite envelopes (ROUTER/STREAM identity strip and prepend, REQ delimiter, REP split) and PUB fan-out `Derive` rather than copy or move ownership; wrapping lent frames in `FromBytes`/`FromFrames` would drop the link to the pool and is the caller's bug.
- Releasing more references than were held panics; it never returns buffers early or twice.
- There is no finalizer. A lent message dropped without `Release` is collected by the GC: safe, just not recycled.

`msg.NewWithRelease` is the general form: it builds a lent message around any frames whose memory must be returned (the release func runs once, possibly on a socket's writer goroutine). CURVE and custom-mechanism sessions decode into fresh plaintext, so their frames are always plain; STREAM rejects the option.

`Socket.RecvBytes` copies a single-frame payload and releases the message, so callers who want no bookkeeping in either mode have a misuse-proof path.

## 7. Transports

### 7.1 TCP

Standard `net.Dial` / `net.Listen` on `tcp` (v4 or v6 per the resolver) or `tcp6`. Wildcard binding (`tcp://*:port`) listens on all interfaces. Port `0` or `*` selects an ephemeral port; the actual bound endpoint is recoverable via `sock.LastEndpoint()`.

Per-connection tuning lives on `transport.TCPTransport` fields: keepalive (on, 30s idle) and `TCP_NODELAY` (on) by default; `ReadBufferSize`/`WriteBufferSize` default to 0, which leaves the kernel's buffer autotuning in place as libzmq does. Socket options are applied best-effort. To change them, configure a transport and install it with `Context.RegisterTransport`, which replaces the default `tcp://` entry.

### 7.2 IPC

Unix domain sockets. Abstract namespace supported on Linux via `@name` syntax (matching libzmq). Closing the listener unlinks the socket file. A stale socket file left by a crashed process (a probe dial is refused) is reclaimed on `Bind`. Permissions are configurable (`transport.IPCTransport.FilePerm`).

### 7.3 inproc

Built into the Context rather than a `transport.Transport`. Each Context has its own inproc registry (`inproc.go`): the bound names and the Connects waiting for them. `Connect` to a bound name creates one pipe pair and attaches an end to each socket's engine — no session, no ZMTP framing, no copying, no goroutines. As in libzmq:

- A `Connect` before the `Bind` waits, with no time limit, until the `Bind`, a `Disconnect` or a `Close`.
- Each direction queues the sender's send HWM plus the receiver's recv HWM (unlimited if either is 0).
- Both sides see each other's `WithIdentity`.
- No security mechanism runs, so there is no ZAP check; a socket with PLAIN, CURVE or a custom mechanism cannot Bind or Connect on `inproc://` (`ErrEndpointInvalid`).

An incompatible socket-type pairing fails the `Connect` with `ErrFSMViolation`, or is reported as `EventConnectFailed` when the `Connect` was waiting. STREAM sockets cannot use inproc. Each socket records its side of the connection as a link (§4.7). Closing either side (`Close`, `Disconnect`) closes the pipe and detaches both engines synchronously, on the calling goroutine; messages already queued stay receivable. `Unbind` releases the name but keeps established links. `Close` lingers only until the peer socket has received what this side sent.

### 7.4 TLS (tls+tcp)

Not in ZMTP proper, but essential for modern deployments and currently requires ad-hoc wrapping elsewhere. We expose `tls+tcp://host:port`, which wraps the TCP connection in `crypto/tls` with a user-supplied `*tls.Config` (`MinVersion` floored at TLS 1.2). It is not preregistered, since there is no sensible default config. The TLS handshake is lazy: `Dial` and `Accept` return the wrapped connection without handshaking, and the handshake runs on the first read or write, which is inside the ZMTP handshake and so bounded by `WithHandshakeTimeout`. A silent or non-TLS peer therefore cannot stall the accept loop or park a reconnector. TLS runs **underneath** the ZMTP security mechanism, so `tls+tcp` + `CURVE` is legal and reasonable (defense in depth; enterprise crypto review for TLS, ZMQ-level mutual auth via CURVE).

### 7.5 Extensibility and deferred transports

PGM/EPGM, VMCI, NORM and the others in §2.2 are not shipped. Transports are pluggable: `transport.Transport`, `Conn` and `Listener` are public interfaces naming only standard-library types, and `Context.RegisterTransport` installs an implementation for its scheme (replacing any existing entry). Every Context preloads `tcp`, `tcp6` and `ipc`; registering a transport for `inproc` panics. An out-of-tree transport (QUIC, WebSocket, a fault-injecting test harness) plugs in without forking; `gozmq_customtransport_test.go` has a working example. A `Listener.Accept` should return an error matching `net.ErrClosed` once closed; the accept loop treats every other error as per-connection and retries.

Security mechanisms are also a public interface (`security.Mech`, installed with `WithSecurityMech`), but that is a last resort: ZMTP defines NULL, PLAIN and CURVE, a fourth mechanism interoperates with nothing else, and a mistake in one is a security bug. The supported extension point for authorization is the ZAP handler (§8.4).

## 8. Security

### 8.1 NULL

Trivial: after the greeting, each side sends a `READY` command with metadata. No cryptographic material. Default mechanism.

### 8.2 PLAIN

Client sends `HELLO` with `username` and `password`. Server responds with `WELCOME` (on success) or `ERROR` (on failure), then the client sends `INITIATE` with metadata and the server finishes with `READY` metadata. Credentials are validated via a user-supplied callback:

```go
gozmq.WithPlainAuth(func(user, pass string) bool { ... })
```

As with libzmq's `ZMQ_PLAIN_SERVER`, the PLAIN role comes from the option, not from Bind/Connect: a `WithPlainAuth` socket is the server on every connection, a `WithPlainClient` socket the client. Setting both, or combining PLAIN with CURVE options, fails `NewSocket`.

### 8.3 CURVE

CurveZMQ ([rfc.zeromq.org/spec/26](https://rfc.zeromq.org/spec/26)) is an authenticated encryption protocol built on the NaCl `crypto_box` primitive (Curve25519 + XSalsa20 + Poly1305). It has five handshake commands: `HELLO`, `WELCOME`, `INITIATE`, `READY`, `MESSAGE`. It provides forward secrecy via ephemeral keys and mutual authentication via long-term public keys.

Implementation uses `golang.org/x/crypto/nacl/box` for the `crypto_box` primitive directly — **not** a bespoke crypto reimplementation. The CurveZMQ codec logic (nonce format, cookie box, envelope) is ours; the primitive operations are not.

Key management:

- Long-term keys generated with `curve.GenerateKeyPair() (pub, priv Key, err error)`.
- Z85 encoding/decoding for human-readable key transport, matching libzmq's format.
- Optional ZAP (ZeroMQ Authentication Protocol) integration for authorization by public key (§8.4).
- Every low-order peer public key is rejected, in any encoding.
- No rekeying: a session whose 64-bit nonce counter would wrap is torn down with `ErrNonceExhausted`.

Test strategy for CURVE is unusually important; see §10.3.

### 8.4 ZAP

The ZeroMQ Authentication Protocol ([rfc.zeromq.org/spec/27](https://rfc.zeromq.org/spec/27)) defines authorization as a request/response exchange with a handler bound to `inproc://zeromq.zap.01`. gozmq keeps the policy in-process instead: `security.ZAPHandler` is a function `(domain, address, mechanism, credentials) → (userID, metadata, error)`, installed per Context with `WithZAPHandler`, and each socket names its domain with `WithZAPDomain`. The handshake calls it directly:

- on the server side of every PLAIN and CURVE handshake, whether that socket binds or connects;
- on the accepting side of NULL handshakes, only when the socket has a ZAP domain (as in libzmq).

A rejection fails the handshake with an error wrapping the handler's own, so a handler should return an error wrapping `ErrAuthFailed` for the failure to be classified as an authentication failure (`EventHandshakeFailedAuth`); the peer is sent an ERROR command. The user-id becomes the peer's `User-Id` property; handler metadata only adds properties the peer did not send, so it cannot override `Identity` or `Socket-Type`. `ZAPHandler.ServeRequest` and the `Encode`/`DecodeZAPRequest`/`Response` codec serve the RFC 27 wire format for applications that also want a real `zeromq.zap.01` socket, for example for libzmq peers in the same process.

## 9. Socket Type Semantics

Each socket type gets a dedicated engine. Representative behaviors:

- **REQ / REP**: strict lock-step. REQ auto-prepends an empty delimiter frame. REP records the peer identity stack on receive and replays it on send. State machine enforces alternation; out-of-order `Send`/`Recv` returns `ErrFSMViolation`.
- **DEALER / ROUTER**: DEALER load-balances outbound across connected peers round-robin, fair-queues inbound. The load balancer rotates past peers whose pipe is at HWM rather than head-of-line blocking on the cursor peer (matching libzmq's `lb_t`); only when *every* peer is full does Send block. ROUTER prepends the peer identity to inbound messages and routes outbound by the leading identity frame. A peer that advertises no identity gets a synthesized 5-byte one: a leading 0 byte, then a per-socket 32-bit counter with a random start (skipping identities in use), as libzmq does. A peer claiming an identity already in use takes over outbound routing for it (libzmq's `ZMQ_ROUTER_HANDOVER=1`; libzmq's default rejects the newcomer); see `docs/COMPATIBILITY.md`. `WithRouterMandatory` makes unroutable sends, including sends to a peer that has just disconnected, return `ErrRouteUnknown` instead of silently dropping (drops are counted in `Metrics`).
- **PUB / SUB**: PUB fans out to all subscribed peers, filtering by subscription prefix maintained per-peer. A peer's SUBSCRIBE/CANCEL commands take effect on the PUB's next `Send` (§4.6), as in libzmq. SUB maintains its subscription list and forwards `SUBSCRIBE`/`CANCEL` commands to every connected PUB.
- **XPUB / XSUB**: identical wire behavior but with subscriptions surfaced as normal messages to the application: an XPUB `Recv` returns them, and an XSUB `Send` of a `0x01`/`0x00`-prefixed message subscribes or cancels upstream. Only that direction exists: XSUB never surfaces SUBSCRIBE/CANCEL commands from its publishers (libzmq never does, and conforming peers never send them), and SUB/XSUB treat inbound data starting with `0x00`/`0x01` as ordinary data. `WithXPubVerbose` disables the default dedup. XPUB applies commands on its next `Send`, `Recv` or readiness check and settles the refcounts then, not when the application reads the event; surfaced events wait in an unbounded queue (like libzmq's XPUB pending queue). XPUB synthesizes `CANCEL` messages when a departing peer's subscriptions drop to refcount zero (libzmq parity), and a peer cannot CANCEL a prefix it never subscribed to.
- **PUSH / PULL**: PUSH load-balances, PULL fair-queues. No identity.
- **PAIR**: exclusive 1:1. A peer disconnect is not surfaced: `Recv` first delivers what the old peer left buffered, then both `Send` and `Recv` wait for the next peer to attach (libzmq semantics). A second peer is refused while one is attached.
- **STREAM**: raw TCP bytes, no ZMTP on the wire. Each inbound frame is prefixed with a peer identity so a single STREAM socket can multiplex many clients. Useful for speaking non-ZMQ protocols (HTTP, SMTP) from a ZMQ pipeline. STREAM dials once and never reconnects, sends no connect/disconnect notification messages, and rejects every ZMTP-layer option. It honours `WithSendHWM`/`WithRecvHWM` (0 means unlimited) and `WithLinger`, and `Disconnect` closes the connection a `Connect` made (§4.8).

## 10. Testing Strategy

### 10.1 Unit tests

Each layer has unit tests. ZMTP framing has table-driven encode/decode tests with fixtures. The pipe layer has property-based tests (via `testing/quick` or `pgregory.net/rapid`) for HWM invariants: total messages in flight never exceeds HWM, blocked senders unblock within bounded time after a receive.

### 10.2 Interop tests

The most important test suite. For every standard socket type, every transport, and every security mechanism, we run cross-implementation tests:

- `gozmq` client ↔ `gozmq` server (baseline).
- `gozmq` client ↔ libzmq server (via a small CGO test harness, isolated behind a build tag so the main module stays CGO-free).
- libzmq client ↔ `gozmq` server.

The CGO harness lives under `interop/` and is only built when `-tags interop` is set. CI (`.github/workflows/ci.yml`) runs on every push and pull request:

- **test** — on Go 1.22.x (the `go.mod` minimum), `oldstable` and `stable`: `go mod tidy` verification, `go vet ./...`, a `CGO_ENABLED=0` build (the whole point of the project), and `go test ./...`;
- **lint** — `golangci-lint` with `.golangci.yml`, which also builds the `interop` and `soak` tags;
- **race** — `go test -race -count=1 ./...`;
- **coverage** — an 80% floor over the library packages;
- **cross-build** — pure-Go builds for linux/386, linux/arm64, darwin/amd64, darwin/arm64, windows/amd64 and freebsd/amd64;
- **fuzz-smoke** — discovers every `Fuzz*` target and runs each for 10s;
- **interop** — installs `libzmq3-dev` and runs `go test -tags interop -race ./interop/...`.

A nightly workflow (`nightly.yml`) runs every fuzz target for 10 minutes, five `-race` passes, the soak suite for 10 minutes, and the interop suite three times.

### 10.3 CURVE test vectors

CurveZMQ is easy to get subtly wrong in ways that only show up against other implementations. We lift the test vectors from libzmq's `tests/test_security_curve.cpp` where available, author our own for the handshake message-box contents, and assert byte-exact agreement.

### 10.4 Fuzzing

Go's native fuzzing (`go test -fuzz`) covers the frame, command and metadata codecs, endpoint parsing, the NULL/PLAIN/CURVE handshakes on both sides, CURVE `MESSAGE` decoding, Z85 decoding, and the ZAP request/response codecs. Seeds are added in code with `f.Add` or checked in under `testdata/fuzz`.

### 10.5 Race and leak detection

CI runs every package under `-race` in a dedicated job (see §10.2). Every package's `TestMain` runs `go.uber.org/goleak` (via `internal/testutil`), so a goroutine that outlives `Socket.Close` fails the suite. The build-tagged `soak/` suite churns connections, subscribers and aborted handshakes, then checks that goroutine count and heap return to baseline (`GOZMQ_SOAK_DURATION` sets the length).

### 10.6 Benchmarks

`perf/` holds a Go benchmark suite (`go test -bench . ./perf/`) and CLI tools mirroring libzmq's `local_lat`, `remote_lat`, `local_thr` and `remote_thr`, plus a harness for native two-process comparisons against libzmq (`perf/libzmq_thr`). Benchmarks are not run in CI; changes that claim a speedup are expected to include before/after `benchstat` output, and measured results are recorded in `perf/REPORT.md`. `perf/benchgate.sh <base>` compares the working tree against a base commit with interleaved prebuilt binaries; a change to a hot path must not regress any benchmark by more than 5% (p < 0.05), raise allocs/op, or raise B/op by more than 5%.

## 11. Performance Considerations

- **Plain memory by default**: received frames are ordinary allocations. Frames up to 4 KiB are read into a per-reader scratch buffer and copied out; larger frames are read directly into their own allocation. `WithRecvBufferPool` trades that allocation for the explicit `Release` contract of §6.5 when large-frame throughput matters.
- **Buffered reads**: TCP, IPC and TLS sessions read through a 16 KiB `bufio.Reader`.
- **Batched writes**: the session writer coalesces up to 32 queued messages (2 MiB) into one `net.Buffers` write (`writev` on TCP and IPC). On NULL/PLAIN sessions frame headers are appended into a reusable scratch buffer and bodies are written without copying.
- **Zero-copy inproc**: an inproc connection is one pipe pair between two engines, passing `*Msg` values with no framing, no copy and no goroutine.
- **Fair queueing without goroutines**: a multi-peer receive walks only the pipes that have data (§4.5), and a single-peer receive blocks in the one pipe without a lock.
- **HWM as channel capacity**: bounded Go channels express the HWM directly; HWM 0 adds an overflow slice rather than a goroutine.
- **Fan-out**: PUB/XPUB `Send` reads a copy-on-write snapshot of the peers, so fan-out takes no engine lock and allocates no per-message slice.
- **CURVE**: the traffic-phase shared key is precomputed once per session, and `EncodeFrame` seals into the pre-sized wire buffer.
- **Goroutine count**: see §4.3. A bound ROUTER costs two goroutines per TCP peer (the session's reader and writer), with or without heartbeats, and none per inproc peer. That is acceptable in Go for tens of thousands of peers but is the scaling cost; §16 lists shared-goroutine I/O as an open question.

Measured throughput and latency, the methodology, and the native comparisons against libzmq are recorded in [`perf/REPORT.md`](perf/REPORT.md). The native libzmq comparison (2026-06-10) predates the move to plain-memory frames with pooling opt-in, so its large-frame figures reflect the old default; the in-repo suite was last measured for the architecture simplification (2026-10).

## 12. Monitoring and Observability

Every socket supports a monitor channel:

```go
events := make(chan gozmq.Event, 64)
sock.Monitor(events, gozmq.EventAll)
```

Events cover: `Connected`, `ConnectDelayed`, `ConnectRetried`, `ConnectFailed`, `Listening`, `BindFailed`, `Accepted`, `AcceptFailed`, `Closed`, `Disconnected`, `HandshakeSucceeded`, `HandshakeFailedProtocol`, `HandshakeFailedAuth`. Each event carries the endpoint, a timestamp, and a free-text `Reason` string (the underlying error string for failures; the pending backoff duration for `ConnectDelayed`/`ConnectRetried`; empty for unambiguous successes). `ConnectFailed` marks terminal connect failures with no reconnect loop behind them — an unknown transport scheme, or a STREAM dial failure (STREAM dials exactly once). There is no `CloseFailed` event.

Implemented observability surface:

- **Monitor events** (above) via `Socket.Monitor` / `WithMonitor` — the primary lifecycle signal.
- **Counter/gauge snapshots** via `Socket.Metrics()`, returning `SocketMetrics{Sent, Recv, Dropped, QueueDepth}`: atomic counters plus a sampled gauge of the messages queued in the socket's inbound pipes. `Dropped` counts PUB fan-out drops at HWM and ROUTER route misses; see `SocketMetrics`.
- **Structured logging** via `WithLogger(*slog.Logger)` — internal diagnostics (handshake failures, reconnect attempts, heartbeat loss, teardown anomalies) are emitted with structured context; defaults to `slog.Default`.

Not implemented (possible future work, deliberately out of scope for now):

- `expvar` registration of the `Metrics()` counters. Today callers poll `Metrics()` and publish to whatever sink they use; gozmq does not register an `expvar` var itself.
- OpenTelemetry spans around socket lifecycle. Deferred to avoid forcing the dependency; callers can derive spans from monitor events.

## 13. Error Model

Sentinels live at the package root (`ErrContextClosed`, `ErrSocketClosed`, `ErrFSMViolation`, `ErrHandshakeTimeout`, `ErrAuthFailed`, `ErrMalformedFrame`, `ErrProtocolVersion`, `ErrRouteUnknown`, `ErrEndpointInvalid`, `ErrEndpointUnknown`, `ErrTransportUnknown`, `ErrNonceExhausted`, and the reserved `ErrHWM`). Errors that report a gozmq condition wrap one of them, so `errors.Is` works. The exceptions are deliberate:

- Blocking calls return `ctx.Err()` unwrapped on cancellation or deadline — `context.Canceled` / `context.DeadlineExceeded`, no surprise `ErrInterrupted`.
- Operating-system and network errors from the transports (a `Bind` whose address is in use, a refused dial) are returned as the underlying error, typically a `*net.OpError`; use `errors.As` and the standard `Timeout() bool` interface.
- `NewSocket` and `Proxy` reject invalid configuration (option values and combinations, socket pairings) with plain descriptive errors.
- A ZAP handler's rejection is wrapped as returned (§8.4).

Handshake and wire failures on established sessions do not reach `SendMsg`/`RecvMsg`; they surface on the monitor channel (§12) with the error text in `Event.Reason`.

## 14. Compatibility and Versioning

- Go 1.22+ (for `slog` in the stdlib and the `for range N` numeric range).
- Semantic versioning. From v1.0.0 the compatibility promise covers everything exported outside `internal/`, which includes the standard socket types, transports, and NULL/PLAIN/CURVE. `CHANGELOG.md` records every user-visible change; `docs/RELEASING.md` is the release checklist.

## 15. Milestones

The original delivery plan, kept for context.

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
- ~~**Auth plugins**: should `WithPlainAuth` accept a full `Authenticator` interface that covers PLAIN, CURVE key lookup, and ZAP uniformly?~~ **Resolved**: `security.ZAPHandler` is that interface — one function that authorizes PLAIN credentials, CURVE public keys, and (with a ZAP domain) NULL peers. `WithPlainAuth` remains as the simple PLAIN-only callback.
- ~~**Message ownership**: returning pooled frames from `RecvMsg` is fast but invites use-after-free bugs. Options: (a) always copy, (b) pooled with `Clone()` for retention (current plan), (c) pooled with an explicit `Release()` call.~~ **Resolved**: frames are plain caller-owned memory by default; option (c) is available as the opt-in `WithRecvBufferPool`, with explicit reference counting and no finalizer. An earlier default-on pool with a finalizer safety net was removed after it corrupted retained and forwarded frames. See §6.5.

## 17. References

- ZMTP 3.1: https://rfc.zeromq.org/spec/23
- CurveZMQ: https://rfc.zeromq.org/spec/26
- ZAP: https://rfc.zeromq.org/spec/27
- libzmq: https://github.com/zeromq/libzmq
- pebbe/zmq4: https://github.com/pebbe/zmq4
- go-zeromq/zmq4: https://github.com/go-zeromq/zmq4
