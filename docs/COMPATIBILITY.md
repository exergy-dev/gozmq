# libzmq compatibility

`gozmq` speaks ZMTP 3.1 (negotiating down to 3.0) and targets wire
compatibility with libzmq ≥ 4.2; it is tested against libzmq 4.3.4. The
[`interop/`](../interop) suite runs gozmq against a real libzmq in CI, in
both directions, for every socket pattern over NULL and for the PLAIN,
CURVE and ZAP combinations listed in [`interop/README.md`](../interop/README.md).

This page is the honest inventory: what is implemented, what is deliberately
absent, and where behaviour differs from libzmq even when both work.

---

## Socket types

| Type | Supported | Notes |
| --- | :---: | --- |
| `REQ` / `REP` | ✅ | Strict lock-step alternation, enforced with `ErrFSMViolation`. |
| `DEALER` / `ROUTER` | ✅ | Synthesized identities as in libzmq; see the handover divergence below. |
| `PUB` / `SUB` | ✅ | Refcounted subscriptions; slow subscribers dropped at HWM. PUB applies incoming subscriptions on its next `Send`, as libzmq does. |
| `XPUB` / `XSUB` | ✅ | Subscriptions surface as messages on XPUB; `WithXPubVerbose` for per-peer. Pending subscription events are queued without a bound, as in libzmq. XSUB does not surface SUBSCRIBE/CANCEL commands from its publishers, as in libzmq. |
| `PUSH` / `PULL` | ✅ | Round-robin out, fair-queue in. |
| `PAIR` | ✅ | Exclusive 1:1. On disconnect, waits for the next peer (libzmq semantics). |
| `STREAM` | ✅ | Raw TCP bridge; several divergences, see below. |
| `CLIENT` / `SERVER` | ❌ | Draft (RFC 41/42). Not implemented. |
| `RADIO` / `DISH` | ❌ | Draft. Not implemented. |
| `SCATTER` / `GATHER` | ❌ | Draft. Not implemented. |
| `PEER` / `CHANNEL` | ❌ | Draft. Not implemented. |

The draft types are unstable in libzmq itself (they require
`--enable-drafts` and change between releases), so they are out of scope for
now. They are the most likely future addition if there is demand.

## Transports

| Scheme | Supported | Notes |
| --- | :---: | --- |
| `tcp://` | ✅ | IPv4/IPv6 per the OS. |
| `tcp6://` | ✅ | Forces IPv6. |
| `ipc://` | ✅ | Unix domain sockets, including Linux abstract sockets. |
| `inproc://` | ✅ | One pipe pair between two sockets of a Context; no security mechanism, HWMs summed, Connect before Bind waits indefinitely, all as in libzmq (see below). |
| `tls+tcp://` | ✅ | **gozmq-specific**, no libzmq equivalent. Opt-in via `transport.NewTLSTransport`. |
| `pgm://` / `epgm://` | ❌ | Reliable multicast. Needs OpenPGM; not implemented. |
| `norm://` | ❌ | Not implemented. |
| `udp://` | ❌ | Only meaningful with the draft RADIO/DISH types. |
| `vmci://` | ❌ | VMware VM-to-host. Not implemented. |

Third-party transports are supported: `transport.Transport` is a public
interface naming only standard-library types, and
`(*Context).RegisterTransport` installs one. See
[`gozmq_customtransport_test.go`](../gozmq_customtransport_test.go) for a
working example.

## Security

| Mechanism | Supported | Notes |
| --- | :---: | --- |
| `NULL` | ✅ | Default. |
| `PLAIN` | ✅ | RFC 24. Cleartext credentials — pair with `tls+tcp://`. |
| `CURVE` | ✅ | RFC 25/26, mutual auth + forward secrecy. Traffic-phase commands travel in the `MESSAGE` inner flags, as in libzmq (see below). No rekey. |
| `GSSAPI` | ❌ | RFC 38. Not implemented. |
| ZAP (RFC 27) | ✅ | `WithZAPHandler` + `WithZAPDomain`: PLAIN and CURVE servers (bound or connected), and accepting NULL sockets only when they set a ZAP domain (as in libzmq). Called in-process, not over `inproc://zeromq.zap.01`; `ZAPHandler.ServeRequest` serves that socket for libzmq peers. The user-id surfaces as the `User-Id` peer property; ZAP metadata never overrides `Identity` or `Socket-Type`. |

Custom mechanisms are possible via `security.Mech` and `WithSecurityMech`,
but are almost never the right answer — see the
[`security` package docs](https://pkg.go.dev/github.com/exergy-dev/gozmq/security).

---

## Socket options

### Supported

| libzmq option | gozmq equivalent |
| --- | --- |
| `ZMQ_SNDHWM` | `WithSendHWM` (0 = unbounded, as in libzmq) |
| `ZMQ_RCVHWM` | `WithRecvHWM` (0 = unbounded, as in libzmq) |
| `ZMQ_LINGER` | `WithLinger` (different default, see below) |
| `ZMQ_IDENTITY` / `ZMQ_ROUTING_ID` | `WithIdentity` |
| `ZMQ_SUBSCRIBE` | `WithSubscribe`, `Socket.Subscribe` |
| `ZMQ_UNSUBSCRIBE` | `Socket.Unsubscribe` |
| `ZMQ_RECONNECT_IVL` | `WithReconnectInterval` |
| `ZMQ_RECONNECT_IVL_MAX` | `WithReconnectIntervalMax` (different 0-semantics, see below) |
| `ZMQ_MAXMSGSIZE` | `WithMaxMessageSize` (bounds the whole message, not each frame; see below) |
| `ZMQ_ROUTER_MANDATORY` | `WithRouterMandatory` |
| `ZMQ_XPUB_VERBOSE` | `WithXPubVerbose` |
| `ZMQ_HEARTBEAT_IVL` / `ZMQ_HEARTBEAT_TIMEOUT` | `WithHeartbeat` |
| `ZMQ_HANDSHAKE_IVL` | `WithHandshakeTimeout` (also bounds the `tls+tcp` TLS handshake) |
| `ZMQ_PLAIN_SERVER` / `ZMQ_PLAIN_USERNAME` / `ZMQ_PLAIN_PASSWORD` | `WithPlainAuth`, `WithPlainClient` (role independent of Bind/Connect, as in libzmq) |
| `ZMQ_CURVE_SERVER` / `ZMQ_CURVE_*KEY` | `WithCurveServer`, `WithCurveClient` (likewise) |
| `ZMQ_ZAP_DOMAIN` | `WithZAPDomain` |
| `ZMQ_LAST_ENDPOINT` | `Socket.LastEndpoint` |
| `ZMQ_TCP_KEEPALIVE` / `_IDLE` | `transport.TCPTransport.KeepAlive` / `.KeepAlivePeriod` † |
| `ZMQ_SNDBUF` / `ZMQ_RCVBUF` | `transport.TCPTransport.WriteBufferSize` / `.ReadBufferSize` † (default 0: OS autotuning, as in libzmq) |
| `ZMQ_TCP_NODELAY` (implicit in libzmq) | `transport.TCPTransport.NoDelay` † |

† These are transport-level rather than socket-level. Configure and register
the transport on the Context:

```go
tcp := transport.NewTCPTransport()
tcp.KeepAlivePeriod = 10 * time.Second
tcp.ReadBufferSize = 1 << 20

ctx := gozmq.NewContext()
ctx.RegisterTransport(tcp) // replaces the default tcp:// entry
```

`transport.IPCTransport.FilePerm` works the same way. The TLS handshake has
no separate timeout: it runs lazily inside the ZMTP handshake and is bounded
by `WithHandshakeTimeout`.

### Not applicable in Go

| libzmq option | Why |
| --- | --- |
| `ZMQ_SNDTIMEO` / `ZMQ_RCVTIMEO` | Superseded by the `context.Context` every `SendMsg`/`RecvMsg` takes. |
| `ZMQ_IO_THREADS` | No I/O thread pool; the runtime schedules goroutines. |
| `ZMQ_MAX_SOCKETS` | No fixed socket table. |
| `ZMQ_AFFINITY` | No I/O threads to pin to. |
| `ZMQ_FD` | Readiness is exposed through `Poller`, not a pollable descriptor. |
| `ZMQ_THREAD_*` | No library-managed threads. |
| `ZMQ_IPV6` | `tcp://` follows the OS; `tcp6://` forces IPv6. |
| `ZMQ_BLOCKY` | Linger already defaults to finite, so shutdown cannot hang. |

### Not implemented

Each is a deliberate deferral, not an oversight. All touch engine
internals and none is needed for the common patterns.

| libzmq option | What you lose | Workaround |
| --- | --- | --- |
| `ZMQ_CONFLATE` | Last-value-caching queues (depth 1). | No close equivalent. `WithRecvHWM(1)` bounds the queue to one message but keeps the *oldest* and backpressures (or, on PUB, drops) the rest — conflate keeps the newest. Conflate semantics need application-level handling for now. |
| `ZMQ_ROUTER_HANDOVER` | Choosing the handover policy. | gozmq always hands over; see below. |
| `ZMQ_REQ_RELAXED` / `ZMQ_REQ_CORRELATE` | Relaxed REQ that can re-send without a reply. | Use `DEALER`, which has no FSM to relax. |
| `ZMQ_PROBE_ROUTER` | Auto-probe message on connect so a ROUTER learns a peer early. | Send an application-level hello. |
| `ZMQ_IMMEDIATE` | Queueing only to completed connections. | gozmq attaches pipes only after the handshake completes, so it already behaves as `IMMEDIATE=1`. Not configurable. |
| `ZMQ_HEARTBEAT_TTL` | Setting the TTL advertised in PING; honouring the TTL a peer advertises. | gozmq advertises its own `WithHeartbeat` timeout as the TTL, per the spec's SHOULD, and ignores the peer's TTL: only the local timeout decides when a silent peer is dropped. |
| `ZMQ_BACKLOG` | Tuning the listen backlog. | OS default. |
| `ZMQ_TOS` | Setting IP type-of-service. | — |
| `ZMQ_STREAM_NOTIFY` | Synthetic connect/disconnect messages on STREAM. | Use a monitor channel; see below. |
| `ZMQ_CURVE_*` rekey | Rekeying a long-lived CURVE session. | See the nonce-exhaustion note below. |
| `ZMQ_GSSAPI_*` | GSSAPI. | — |
| `ZMQ_MULTICAST_*` | PGM tuning. | No multicast transport. |

---

## Behavioural divergences

These apply where both libzmq and gozmq implement a feature but behave
differently. Each is intentional. A few parity notes are included where
gozmq's own behaviour changed to match libzmq and the change is visible.

### ROUTER identity handover

gozmq always hands over: when a peer attaches claiming an identity already
in use, the new peer takes over outbound routing for it. libzmq's
**default** (`ZMQ_ROUTER_HANDOVER=0`) is the opposite — it rejects the new
peer and keeps the stale pipe bound.

This is also not quite libzmq's `ZMQ_ROUTER_HANDOVER=1`, which terminates the
old pipe. gozmq leaves the old connection attached until it disconnects on
its own; messages it still sends arrive under the same identity, and replies
go to the new peer.

Handover is the better default in practice: a client that reconnects before
the server's TCP stack has noticed the old connection died is otherwise
stranded until the stale pipe times out, which is the common case behind a
NAT or load balancer. The cost is that two clients sharing a `WithIdentity`
value silently take each other's replies instead of one failing loudly.
**Identities must be unique.**

Peers that advertise no identity get one synthesized as libzmq does: five
bytes, a leading `0` then a per-socket 32-bit counter that starts at a random
value and skips identities in use. A peer-advertised identity must follow
ZMTP 3.1 (1–255 bytes, no leading zero byte) or the handshake fails.

### `MaxMessageSize` bounds the whole message

`ZMQ_MAXMSGSIZE` is a per-frame limit. gozmq's `WithMaxMessageSize` bounds
the total of all frames of one logical message, on send (a synchronous
error) and on receive (the session is torn down), so the two sides always
agree. For CURVE it is a plaintext bound; the 33-byte per-frame overhead is
allowed for on the wire. A libzmq peer that sends a multipart message whose
frames are each under the cap but whose total exceeds it is disconnected.

### `HWM 0` means unbounded

As in libzmq, `WithSendHWM(0)` / `WithRecvHWM(0)` remove the limit for that
pipe direction: messages queue in memory without bound and senders never
block. Do not use it with untrusted peers.

### `LINGER` defaults to 1s, not infinite

libzmq's `ZMQ_LINGER` default of −1 (wait forever) means one forgotten
`Close` with an unreachable peer hangs process shutdown indefinitely. gozmq
defaults to 1 second. `WithLinger(-1)` restores libzmq's behaviour; pair it
with `Socket.CloseCtx` so there is an escape hatch.

### `RECONNECT_IVL_MAX == 0`

In libzmq, 0 is the default and means "no exponential backoff, use a
constant interval". In gozmq, 0 means "no cap — back off without bound", and
the default is 30s. Setting it to 0 is almost never what you want.

### `PollOut` ignores high-water marks

`Poller`'s `PollOut` reports only that at least one peer is attached to
dispatch to. Unlike `ZMQ_POLLOUT`, it does **not** account for HWM pressure,
so a `SendMsg` after a `PollOut` wakeup can still block if every peer pipe is
full. Event loops ported from `zmq_poll` should pair `PollOut` sends with a
context deadline.

### STREAM sockets

- No synthetic `[identity, empty]` connect/disconnect messages
  (`ZMQ_STREAM_NOTIFY`). Use a monitor channel for peer lifecycle.
- `Connect` dials exactly once and never reconnects; a dial failure is
  returned synchronously and a later disconnect is final. Reconnect
  supervision is the application's job.
- ZMTP-layer options (security, heartbeat, identity, handshake timeout, ZAP
  domain) and an explicit `WithRecvBufferPool(true)` are **rejected** by `NewSocket` rather
  than silently ignored — accepting `WithPlainAuth` on a raw-TCP listener
  that authenticates nobody would be a security hazard.
- STREAM cannot use `inproc://`.

- `WithMaxMessageSize` and the reconnect options are accepted but have no
  effect: raw sessions have no frame codec and never reconnect.

`WithSendHWM`/`WithRecvHWM` apply as given, with 0 meaning unlimited;
`Close` honours `WithLinger`, flushing queued outbound data before it closes
each connection; and `Disconnect` closes the connection a `Connect` made.

### inproc

`inproc://` connections are one in-memory pipe pair between two sockets in
the same Context: no ZMTP handshake, and therefore no ZAP hook. libzmq also
runs no mechanism or ZAP check on inproc, so this matches — but it does mean
a Context-level ZAP handler never sees inproc peers.

A socket configured with PLAIN, CURVE, or a `WithSecurityMech` mechanism
cannot use inproc: its `Bind` or `Connect` on `inproc://` fails with
`ErrEndpointInvalid`, rather than silently running unauthenticated.

The rest matches libzmq:

- A `Connect` before the `Bind` waits with no time limit, until the `Bind`,
  a `Disconnect` or `Close`.
- Each direction queues up to the sender's send HWM plus the receiver's
  recv HWM, unlimited if either is 0.
- Both sides see each other's `WithIdentity`, so ROUTER↔ROUTER can address
  either way.
- Connecting to a bound socket of an incompatible type fails with
  `ErrFSMViolation`, or `EventConnectFailed` for a waiting Connect.
- `Disconnect` works on inproc endpoints and detaches both sides; `Unbind`
  releases the name and keeps established connections.

### PUB applies subscriptions on its next send

A PUB or XPUB applies the SUBSCRIBE/CANCEL commands its subscribers sent on
its next `Send` (XPUB also on `Recv` and on a `Poller` check), as libzmq
applies them on its next send, recv or poll. A message published right after
a subscriber connects is filtered against the subscriptions that had arrived
by then. A PUB that never sends leaves its subscribers' commands queued, up
to the receive HWM.

### XSUB does not surface upstream subscriptions

XSUB delivers what its publishers send as data and never turns their
SUBSCRIBE/CANCEL commands into messages; libzmq does the same, and
conforming publishers never send them. Data that starts with `0x00` or
`0x01` reaches SUB/XSUB applications unchanged.

### CURVE commands in the `MESSAGE` flags

This matches libzmq. Once the CURVE handshake is done, libzmq boxes every
frame, commands included, in a `MESSAGE` sent with outer flags `0x00`;
`MORE` (`0x01`) and `COMMAND` (`0x02`) travel only in the encrypted inner
flags byte. gozmq does the same, which is what makes heartbeats (on by
default) work over CURVE against libzmq. The outer wire flags of a
`MESSAGE` are not authenticated and are ignored on receipt, as libzmq does.

CurveZMQ, as libzmq implements it, tolerates gaps in the per-direction nonce
counter (only replays and reordering are rejected), so an on-path attacker
can delete whole frames from a CURVE stream undetected. gozmq matches that
behaviour; applications that need to detect loss must sequence their own
messages.

### CURVE nonce exhaustion

RFC 26 expects rekeying long before the per-direction 64-bit nonce counter
wraps. gozmq does not implement rekey; a session that would wrap is torn
down cleanly with `ErrNonceExhausted`. Reaching this requires ~2^64 frames on
one session, so it is a defensive check rather than an operational limit —
but if you see it in logs, rotate keys and reconnect.

### libzmq's malformed ERROR command

libzmq's `plain_server_t::produce_error` writes the C literal `"\x5ERROR"`
where `"\5ERROR"` was intended. C hex escapes consume every following hex
digit, so `\x5E` swallows the `E`: the command that reaches the wire
declares a 94-byte name and supplies four bytes of it. Confirmed against
libzmq 4.3.4.

A strict ZMTP 3.1 §5.1 reading rejects that frame, which would make every
bad-password handshake against a real libzmq server surface as a malformed
frame rather than an authentication failure. gozmq therefore accepts this
one byte sequence — matched in full, only after the strict parse has already
failed — and reports `ErrAuthFailed` as it should. No other malformed
command name is accepted. The NULL and CURVE clients share the same ERROR
recogniser (`security.DecodeError`), so the tolerance applies to them too.

gozmq itself always emits well-formed ERROR commands.

### Message ownership

`RecvMsg` returns frames of up to 4 KiB in ordinary Go memory owned by the
caller; nothing needs releasing, so there is no counterpart to
`zmq_msg_close`. By default (opt out with `WithRecvBufferPool(false)`),
frames over 4 KiB on NULL/PLAIN sessions are received into pooled buffers and
the message is lent: forgetting `Release` is safe, but the
receiver should call `Release` when done, and the frames are valid until the last
reference is released. `SendMsg` holds its own reference, so forwarding and
then releasing is safe. `RecvBytes` avoids the bookkeeping entirely for
single-frame payloads.

### Socket metrics

libzmq has no per-socket counters; `Socket.Metrics` is gozmq's own.
`QueueDepth` is the number of messages waiting in the socket's inbound pipes
(PAIR and REQ included). `Dropped` counts only PUB/XPUB fan-out drops at a
full subscriber pipe and ROUTER sends to an unknown identity without
`WithRouterMandatory`; nothing is dropped between a pipe and `RecvMsg`.

---

## Future candidates

Ordered roughly by likely demand:

1. `ZMQ_CONFLATE` — genuinely useful for last-value-cache telemetry.
2. `ZMQ_ROUTER_HANDOVER` as a configurable option, for parity.
3. Draft socket types (`CLIENT`/`SERVER`, `RADIO`/`DISH`), if they stabilise.
4. `ZMQ_REQ_RELAXED` / `ZMQ_REQ_CORRELATE`.
5. CURVE rekey.
6. UDP and PGM transports.
