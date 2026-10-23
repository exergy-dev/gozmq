# gozmq design

Last updated: 2026-10-23. This is the architecture: what the layers are, how
goroutines, pipes and teardown work, and why. The API is documented in the
packages themselves, the behavioural differences from libzmq in
`docs/COMPATIBILITY.md`, measurements in `perf/REPORT.md`.

## 1. Goals and non-goals

`gozmq` is a pure-Go implementation of the ZeroMQ socket patterns, the ZMTP
3.1 wire protocol and the practical transports, for Go applications that want
ZeroMQ semantics without linking libzmq through cgo.

Goals:

- **Wire compatibility** with libzmq over TCP and IPC, and libzmq's semantics
  on inproc (which has no wire format). Tested against libzmq 4.3.x in both
  directions (`interop/`).
- **Every standard socket type**: `REQ`, `REP`, `DEALER`, `ROUTER`, `PUB`,
  `SUB`, `XPUB`, `XSUB`, `PUSH`, `PULL`, `PAIR`, `STREAM`. Draft types
  (`SERVER`/`CLIENT`, `RADIO`/`DISH`, `SCATTER`/`GATHER`, `PEER`, `CHANNEL`)
  are out of scope.
- **Transports** `tcp://`, `tcp6://`, `ipc://`, `inproc://`, and `tls+tcp://`
  (ours, §6.4); third-party transports plug in (§6.5).
- **NULL, PLAIN and CURVE** (CurveZMQ on `golang.org/x/crypto/nacl/box`), with
  RFC 27 ZAP authorization in-process.
- **Production behaviour**: high-water-mark backpressure, reconnection with
  exponential backoff and jitter, monitor events, context cancellation,
  linger on close.
- **Idiomatic Go**: `context.Context` on every blocking call, typed options,
  sentinel errors that `errors.Is` matches, no globals.
- **No cgo**: `CGO_ENABLED=0 go build` everywhere Go runs.

Non-goals, for now: `ws://`/`wss://`, `udp://`, `pgm://`/`epgm://`,
`vmci://`, `tipc://`, GSSAPI, libzmq's C API or option codes, drop-in
replacement of `pebbe/zmq4`, and ZMTP 2.0 or older (we negotiate down to 3.0
only). None is architecturally precluded.

## 2. Background

ZeroMQ sockets encapsulate messaging patterns (request/reply,
publish/subscribe, pipeline, ...). One socket binds or connects to many
endpoints; the library multiplexes the connections and applies the pattern's
policy (fair queueing, load balancing, routing). Messages are multipart
sequences of binary frames.

ZMTP 3.1 ([RFC 23](https://rfc.zeromq.org/spec/23)) is the wire protocol: a
64-byte greeting (signature, version, mechanism, as-server flag), a
mechanism-specific handshake ending in a `READY` command that carries
metadata (`Socket-Type`, `Identity`), then a traffic phase of
length-prefixed frames with `MORE`, `LONG` and `COMMAND` flags. 3.1 adds
`PING`/`PONG` heartbeats and `DISCONNECT`.

The pure-Go alternative, `go-zeromq/zmq4`, lacks parts of the socket and
transport coverage, a tested CURVE, heartbeats and monitoring; `pebbe/zmq4`
and `goczmq` are cgo bindings.

## 3. Architecture

### 3.1 Layering

```
Public API        (Socket, Context, Msg, options, events)
Socket engines    (per-type policy: REQ, ROUTER, PUB, ...)
Pipe layer        (bounded in-memory queues, HWM, idle notification)
Session layer     (one per peer connection: ZMTP state machine)
Security layer    (NULL / PLAIN / CURVE handshake and frame codec)
Framing layer     (ZMTP frame encode/decode, commands, heartbeats)
Transport layer   (tcp, tcp6, ipc, tls+tcp)
```

Each layer has a narrow interface and its own tests, and no layer reaches
across more than one boundary. Two paths skip layers: inproc joins two
engines with one pipe pair and has no session, security, framing or
transport (§6.3); a STREAM session runs in raw mode with no security or
framing (§3.8).

Concepts: a **Socket** holds configuration, an engine and one pipe per peer.
A **pipe** (`internal/pipe.End`, one per side) is a bidirectional bounded
queue between an engine and a session, or between two engines on inproc;
each direction has its own HWM, and pipes are where backpressure lives. A
**session** owns one peer connection: the ZMTP state machine, the security
codec, and the translation between wire frames and `Msg` values on the pipe.
A **transport** turns an endpoint string into connections. A **link** is a
socket's record of one peer, a session or an inproc end, behind the one
teardown contract `Close` and `Disconnect` use (§3.7). A **Context** owns the
scheme → transport table, the inproc registry and the optional ZAP handler;
Contexts share nothing.

### 3.2 Concurrency model

Goroutines exist only where something blocks on I/O; everything else runs on
the caller's goroutine or the sender's.

| Unit | Long-lived goroutines |
|---|---|
| ZMTP session (one per TCP/IPC/TLS connection, each side) | 2: reader and writer |
| `Connect` (non-STREAM) | 1 reconnector, for the life of the Connect |
| `Bind` on a transport | 1 accept loop |
| inproc link | 0 |
| STREAM connection | 2: the raw session's reader and writer |
| Socket engines, pipes, FairQueue, LoadBalancer, Poller | 0 |

An idle TCP connection between two gozmq sockets costs five goroutines
counting both ends. `gozmq_goroutines_test.go` enforces these budgets; they
may only be tightened.

- **Session.** The reader drives the greeting and handshake, then moves wire
  frames to the pipe. It hands outbound handshake frames to the writer over
  a small channel (`hsOut`), so a peer that writes before it reads cannot
  deadlock the handshake. The writer writes those in order, then moves pipe
  messages to the wire, so the last handshake frame always precedes traffic.
  PING/PONG go through the writer too (`cmdCh`); the writer is the only
  goroutine that writes to the conn. The heartbeat is a self-rearming
  `time.AfterFunc`, not a goroutine. A write error closes the conn so the
  reader unblocks; the last of the two goroutines to exit closes the session
  pipe, runs `OnClosed` and closes `Done`.
- **Callbacks, not supervisors.** The socket layer passes `OnReady` and
  `OnClosed` in `session.Config`. `OnReady` runs on the reader after the
  handshake and attaches the pipe to the engine; `OnClosed` detaches it,
  emits the disconnect or handshake-failure event, and removes the link. No
  goroutine waits on a session.
- **Reconnector.** One loop per `Connect` dials, starts a session, waits for
  its `Done`, and backs off (exponential with jitter, reset after an attach).
  `Quiesce` stops redialling while the current session drains on `Close`.
- **Accept loop.** It blocks in `Accept`. A per-connection accept error is
  reported as `EventAcceptFailed` and retried with capped backoff; only a
  closed listener or socket shutdown ends the loop.
- **Engines and pipes run no goroutines.** Receivers read the pipes
  themselves (§3.4), PUB/XPUB apply subscriptions on the application's own
  calls, and readiness is signalled by arming channels (§3.5). A pipe
  direction with a positive HWM is a buffered channel of that capacity; HWM 0
  is a 1024-slot channel plus an overflow slice the receiving end refills
  from, so an unbounded direction costs memory, not a relay goroutine.
- **Public calls** are safe for concurrent use; each engine serialises what
  its pattern requires (REQ/REP reject a concurrent call that would break
  lock-step with `ErrFSMViolation`). One goroutine per socket is still the
  recommended shape, as with libzmq.
- **Proxy** runs a goroutine per forwarding direction plus one for the capture
  tap, for the duration of the call.

### 3.3 Pipes and the idle notification

`pipe.NewPair` returns two `*pipe.End` values. Each carries the peer's
`PeerInfo` (its ZMTP `Identity`) and, once a ROUTER or STREAM engine has
assigned one, its routing identity, so a message received after detach is
still stamped with the right peer.

A receiver multiplexing many pipes must neither poll them nor run a goroutine
per pipe. Each `End` therefore has an `idle` flag and a `notify` callback:
`RecvOrIdle` takes a message or marks the end idle and re-checks; `MarkIdle`
does the same without consuming. After every enqueue, and on `Close`, the
sender loads `idle` and runs `notify` only on a true→false CAS, on its own
goroutine. The receiver sets `idle` with a full barrier and only then
re-checks; the sender publishes and only then loads `idle`; one of the two
always sees the other, so no wakeup is lost. `SendDrained` is the linger
signal for inproc (§3.7).

### 3.4 Fair queueing and load balancing

`FairQueue` (PULL, DEALER, REP, ROUTER, STREAM, SUB, XSUB, and the PUB/XPUB
subscription path) follows libzmq's `fq_t`: the attached pipes sit in a slice
whose first `nActive` entries may hold a message; activation swaps across
that boundary in O(1). A receiver walks the active entries round-robin under
the queue's mutex with `RecvOrIdle`; an empty pipe is deactivated, a closed
and empty one removed. A pipe's notification reactivates its entry and arms
the socket's in-gate; with nothing active, a receiver parks on that gate, the
close channel and its context. While exactly one pipe is attached, `Recv`
blocks in it directly without the mutex; attach or detach closes a generation
channel that aborts such receivers so they re-evaluate. A pipe that closes
with messages queued stays attached until drained, so a departed peer's
messages are still delivered. Receive cost depends on the active pipes, not
the attached ones.

PAIR and REQ have one pipe at a time and register their in-gate directly as
its notification. `LoadBalancer` (PUSH, DEALER) follows `lb_t`: it rotates
past peers at HWM and blocks only when every peer is full.

### 3.5 Readiness and subscriptions

Each engine has two cap-1 channels, `in` and `out` (`Engine.Gates()`). A
token in a gate is a hint that the direction may have become ready: pipe
notifications arm `in`; engines arm `out` on attach and when REQ/REP enter a
can-send state. `CanRecvNow`/`CanSendNow` are level checks, and a false
`CanRecvNow` also marks the empty pipes idle so the next message arms the
gate. The `Poller` parks on the gates with one `reflect.Select` and re-checks
with those methods; a waiter that takes the token and finds the direction
ready puts it back. `broadcast`, a close-and-replace channel, wakes every
waiter at once where one token would not do.

The session turns inbound SUBSCRIBE/CANCEL commands into `0x01`/`0x00`-
prefixed messages on the pipe, but only for PUB and XPUB; every other socket
type drops them. Queued commands are applied to each peer's `SubTree` on the
socket's next `Send` (for XPUB also on `Recv` and on a readiness check), as
libzmq applies them on its next send, recv or poll; when nothing is queued,
`Send` pays one atomic load. XPUB settles refcounts as each command is
applied and queues the surfaced events without a bound. A PUB that never
sends therefore leaves subscriptions queued up to the receive HWM.

### 3.6 Public API over engines

`SendMsg`/`RecvMsg` take a `*msg.Msg` of frames. By default received frames
are ordinary Go memory the caller owns. With `WithRecvBufferPool(true)` large
frames on NULL/PLAIN sessions arrive in pooled buffers and the message is
*lent*: valid until `Release`, reference-counted through `Retain` and
`Derive`, with `SendMsg` holding its own reference until the write completes.
The contract, including why there is no finalizer, is in package `msg` and
`gozmq.WithRecvBufferPool`.

### 3.7 Links and teardown

A socket records every peer as a link: a session (accepted, a reconnector's,
or a STREAM Connect's) or its side of an inproc connection, including a
Connect still waiting for its Bind. A link has `Drain() <-chan struct{}`,
which starts flushing what this side has queued (a session cancels its
writer's context and the writer writes what is queued, leaving the conn open;
an inproc link returns `SendDrained`), and `Stop() error`, the hard teardown.
A link that ends on its own removes itself; whoever takes a link out of the
map under the socket's mutex (`CloseCtx`, `Disconnect`) must stop it, so
long-lived sockets do not accumulate links under churn.

`CloseCtx`: quiesce the reconnectors; close the listeners and release the
inproc names; `engine.Close`, so nothing new is queued; drain every link,
bounded by `WithLinger` and the caller's context; cancel the run context and
stop reconnectors and links; wait for the accept loops; emit `EventClosed`.

### 3.8 STREAM

A STREAM connection is a session started with `Config.Raw`, fixed at
construction so the ZMTP loops carry no branch for it: no greeting, handshake
or heartbeat. The raw reader publishes each `conn.Read` (up to 32 KiB) as one
single-frame message; the raw writer writes each body verbatim, and an empty
body closes the conn. The engine stamps and strips the routing identity like
ROUTER. A STREAM `Connect` dials once with no reconnector.

## 4. Protocol notes

- The greeting negotiates down to ZMTP 3.0 (heartbeats off) and rejects a
  peer announcing a different mechanism, or the same server role as ours.
- The handshake is bounded by `WithHandshakeTimeout` (default 30 s) through
  a deadline on the connection; for `tls+tcp` that also bounds the lazy TLS
  handshake. Handshake frames are capped at 64 KiB regardless of
  `WithMaxMessageSize`.
- Heartbeats: with both peers at 3.1, `PING` every `HeartbeatInterval`
  (default 30 s); no frame within `HeartbeatTimeout` (default 3× the
  interval) tears the session down. A session parked on local backpressure is
  not treated as peer-dead.
- Traffic phase: frames accumulate into a `Msg` until one without `MORE`.
  Frames up to 4 KiB are read into a per-reader scratch buffer and copied
  out; larger ones get their own allocation (or a pooled buffer). A body
  above 128 KiB is allocated as its bytes arrive, so a peer that announces a
  huge frame and stalls pins at most 17× what it sent plus 128 KiB, not the
  cap. `MaxMessageSize` bounds the whole multipart message on both sides.

## 5. Security

- **NULL**: a `READY` exchange, no cryptography. The default.
- **PLAIN**: `HELLO` (credentials) → `WELCOME` or `ERROR` → `INITIATE` →
  `READY`, with `WithPlainAuth(func(user, pass string) bool)`. The role comes
  from the option, not from Bind/Connect, as with `ZMQ_PLAIN_SERVER`.
- **CURVE** ([RFC 26](https://rfc.zeromq.org/spec/26)): `nacl/box` for the
  primitive, our code for the CurveZMQ envelope (nonces, cookie, vouch). Every
  low-order peer key is rejected in any encoding; there is no rekeying, so a
  session whose nonce counter would wrap ends with `ErrNonceExhausted`. Under
  CURVE every traffic frame travels inside a `MESSAGE` box with outer flags
  `0x00`; `MORE` and `COMMAND` live in the encrypted inner flags byte and the
  outer flags are ignored on decode, because they are not under the MAC.
  Without a ZAP handler a CURVE server accepts any client that holds its key.
- **ZAP** ([RFC 27](https://rfc.zeromq.org/spec/27)) is in-process:
  `security.ZAPHandler` is a function installed per Context with
  `WithZAPHandler`; each socket names its domain with `WithZAPDomain`. It runs
  on the server side of every PLAIN and CURVE handshake and on NULL only when
  a domain is set, as in libzmq. The user-id becomes `User-Id`; handler
  metadata cannot override `Identity` or `Socket-Type`.

A fourth mechanism can be installed with `WithSecurityMech`, but it
interoperates with nothing else and a mistake in one is a security bug; the
supported extension point for authorization is the ZAP handler.

## 6. Transports

### 6.1 TCP

`net.Dial`/`net.Listen` on `tcp` or `tcp6`; `tcp://*:port` binds all
interfaces, port 0 picks an ephemeral one recoverable via `LastEndpoint()`.
Keepalive (30 s idle) and `TCP_NODELAY` are on; buffer sizes default to the
kernel's. Tune them on a `transport.TCPTransport` and install it with
`Context.RegisterTransport`.

### 6.2 IPC

Unix domain sockets; `ipc://@name` is the Linux abstract namespace. Closing
the listener unlinks the file; a stale file from a crashed process is
reclaimed on `Bind`; `IPCTransport.FilePerm` sets permissions.

### 6.3 inproc

Built into the Context, not a transport. `Connect` to a bound name creates
one pipe pair and attaches an end to each engine: no session, framing, copy
or goroutine. A `Connect` before the `Bind` waits with no time limit. Each
direction queues the sender's send HWM plus the receiver's recv HWM
(unlimited if either is 0). No security mechanism runs, so a socket with
PLAIN, CURVE or a custom mechanism cannot use `inproc://`. Closing either
side detaches both engines synchronously; queued messages stay receivable;
`Close` lingers only until the peer has received what this side sent.

### 6.4 TLS (tls+tcp)

Not in ZMTP. `tls+tcp://host:port` wraps TCP in `crypto/tls` with a
user-supplied `*tls.Config` (`MinVersion` floored at 1.2); not preregistered,
since there is no sensible default config. The TLS handshake is lazy, so it
runs inside the ZMTP handshake and under `WithHandshakeTimeout`, and a silent
peer cannot stall the accept loop. TLS runs underneath the ZMTP mechanism, so
`tls+tcp` plus CURVE is legal.

### 6.5 Extensibility

`transport.Transport`, `Conn` and `Listener` name only standard-library
types, and `Context.RegisterTransport` installs an implementation for its
scheme; `gozmq_customtransport_test.go` has a working example. A
`Listener.Accept` should return an error matching `net.ErrClosed` once
closed; every other error is treated as per-connection and retried.

## 7. Socket semantics worth knowing

- **REQ/REP**: strict lock-step; REQ prepends the empty delimiter, REP
  records and replays the identity stack; out-of-order calls return
  `ErrFSMViolation`.
- **ROUTER**: a peer without an identity gets a synthesized 5-byte one (a
  zero byte, then a per-socket counter with a random start). A peer claiming
  an identity in use takes over outbound routing for it; see
  `docs/COMPATIBILITY.md`. `WithRouterMandatory` turns silent drops into
  `ErrRouteUnknown`.
- **PUB/SUB, XPUB/XSUB**: subscriptions take effect on the PUB's next `Send`
  (§3.5). XPUB surfaces them as messages and synthesizes `CANCEL` when a
  departing peer's refcount drops to zero; XSUB never surfaces its
  publishers' commands.
- **PAIR**: exclusive 1:1; a peer disconnect is not surfaced, `Recv` first
  delivers what the old peer left, then waits for the next peer.

## 8. Testing

- Every layer has unit tests; the pipe layer checks its HWM invariants with
  a seeded random operation sequence.
- `interop/` (build tag `interop`, cgo) runs every pattern against a real
  libzmq in both directions; it is the suite that matters for wire changes,
  because round-trip-with-ourselves tests pass when both sides are wrong the
  same way. CURVE's byte-exact handshake vectors are self-recorded golden
  files; agreement with libzmq comes from interop.
- Go-native fuzzing covers the frame, command and metadata codecs, endpoint
  parsing, the three handshakes on both sides, CURVE `MESSAGE` decoding, Z85
  and the ZAP codecs.
- Every package's `TestMain` runs goleak (`testutil.VerifyMain`), so a
  goroutine that outlives `Socket.Close` fails the suite; `soak/` churns
  connections and checks goroutines and heap return to baseline.
- `perf/` holds the benchmark suite, a libzmq-comparable tool, and
  `benchgate.sh`, whose header states the regression rule a hot-path change
  must pass. CI is in `.github/workflows/`.

## 9. Performance

- Plain memory by default; `WithRecvBufferPool` trades the per-frame
  allocation for the explicit `Release` contract.
- Reads go through a 16 KiB `bufio.Reader`; the writer coalesces up to 32
  queued messages (2 MiB) into one `net.Buffers` write, appending NULL/PLAIN
  frame headers into a reusable scratch buffer so bodies are not copied.
- inproc passes `*Msg` values with no framing, copy or goroutine.
- A multi-peer receive walks only the pipes that have data; a single-peer
  receive blocks in the one pipe without a lock. Bounded channels express the
  HWM directly.
- PUB/XPUB `Send` reads a copy-on-write snapshot of the peers: no lock, no
  per-message slice.
- CURVE precomputes the shared key once per session and seals into the
  pre-sized wire buffer.
- A bound ROUTER costs two goroutines per TCP peer and none per inproc peer;
  shared-goroutine I/O for hundreds of thousands of peers is the open
  scaling question.

Numbers are in `perf/REPORT.md`.

## 10. Open questions

- **Async DNS**: libzmq resolves names lazily at connect time; expose
  `WithResolver(*net.Resolver)` or defer?
- **Epoll-level I/O**: is a shared-goroutine I/O model worth it beyond tens
  of thousands of peers, or is that out of scope?

## 11. References

- ZMTP 3.1: https://rfc.zeromq.org/spec/23
- CurveZMQ: https://rfc.zeromq.org/spec/26
- ZAP: https://rfc.zeromq.org/spec/27
- libzmq: https://github.com/zeromq/libzmq
