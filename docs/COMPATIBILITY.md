# libzmq compatibility

`gozmq` speaks ZMTP 3.1 (negotiating down to 3.0) and is wire-compatible with
libzmq ≥ 4.2. The [`interop/`](../interop) suite runs gozmq against a real
libzmq in CI, in both directions, for every socket pattern and security
mechanism listed below.

This page is the honest inventory: what is implemented, what is deliberately
absent, and where behaviour differs from libzmq even when both work.

---

## Socket types

| Type | Supported | Notes |
| --- | :---: | --- |
| `REQ` / `REP` | ✅ | Strict lock-step alternation, enforced with `ErrFSMViolation`. |
| `DEALER` / `ROUTER` | ✅ | See the handover divergence below. |
| `PUB` / `SUB` | ✅ | Refcounted subscriptions; slow subscribers dropped at HWM. |
| `XPUB` / `XSUB` | ✅ | Subscriptions surface as messages; `WithXPubVerbose` for per-peer. |
| `PUSH` / `PULL` | ✅ | Round-robin out, fair-queue in. |
| `PAIR` | ✅ | Exclusive 1:1. |
| `STREAM` | ✅ | Raw TCP bridge; several divergences, see below. |
| `CLIENT` / `SERVER` | ❌ | Draft (RFC 41/42). Not implemented. |
| `RADIO` / `DISH` | ❌ | Draft. Not implemented. |
| `SCATTER` / `GATHER` | ❌ | Draft. Not implemented. |
| `PEER` / `CHANNEL` | ❌ | Draft. Not implemented. |

The draft types are unstable in libzmq itself (they require
`--enable-drafts` and change between releases), so they are out of scope for
v1.0. They are the most likely post-1.0 addition if there is demand.

## Transports

| Scheme | Supported | Notes |
| --- | :---: | --- |
| `tcp://` | ✅ | IPv4/IPv6 per the OS. |
| `tcp6://` | ✅ | Forces IPv6. |
| `ipc://` | ✅ | Unix domain sockets, including Linux abstract sockets. |
| `inproc://` | ✅ | Direct-pipe bypass within a Context; see the ZAP divergence. |
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
| `CURVE` | ✅ | RFC 25/26, mutual auth + forward secrecy. No rekey (see below). |
| `GSSAPI` | ❌ | RFC 38. Not implemented. |
| ZAP (RFC 27) | ✅ | `WithZapHandler` + `WithZapDomain`, for PLAIN and CURVE. |

Custom mechanisms are possible via `security.Mech` and `WithSecurityMech`,
but are almost never the right answer — see the
[`security` package docs](https://pkg.go.dev/github.com/exergy-dev/gozmq/security).

---

## Socket options

### Supported

| libzmq option | gozmq equivalent |
| --- | --- |
| `ZMQ_SNDHWM` | `WithSendHWM` |
| `ZMQ_RCVHWM` | `WithRecvHWM` |
| `ZMQ_LINGER` | `WithLinger` (different default, see below) |
| `ZMQ_IDENTITY` / `ZMQ_ROUTING_ID` | `WithIdentity` |
| `ZMQ_SUBSCRIBE` | `WithSubscribe`, `Socket.Subscribe` |
| `ZMQ_UNSUBSCRIBE` | `Socket.Unsubscribe` |
| `ZMQ_RECONNECT_IVL` | `WithReconnectInterval` |
| `ZMQ_RECONNECT_IVL_MAX` | `WithReconnectIntervalMax` (different 0-semantics, see below) |
| `ZMQ_MAXMSGSIZE` | `WithMaxMessageSize` |
| `ZMQ_ROUTER_MANDATORY` | `WithRouterMandatory` |
| `ZMQ_XPUB_VERBOSE` | `WithXPubVerbose` |
| `ZMQ_HEARTBEAT_IVL` / `ZMQ_HEARTBEAT_TIMEOUT` | `WithHeartbeat` |
| `ZMQ_HANDSHAKE_IVL` | `WithHandshakeTimeout` |
| `ZMQ_PLAIN_SERVER` / `ZMQ_PLAIN_USERNAME` / `ZMQ_PLAIN_PASSWORD` | `WithPlainAuth`, `WithPlainClient` |
| `ZMQ_CURVE_SERVER` / `ZMQ_CURVE_*KEY` | `WithCurveServer`, `WithCurveClient` |
| `ZMQ_ZAP_DOMAIN` | `WithZapDomain` |
| `ZMQ_LAST_ENDPOINT` | `Socket.LastEndpoint` |
| `ZMQ_TCP_KEEPALIVE` / `_IDLE` | `transport.TCPTransport.KeepAlive` / `.KeepAlivePeriod` † |
| `ZMQ_SNDBUF` / `ZMQ_RCVBUF` | `transport.TCPTransport.WriteBufferSize` / `.ReadBufferSize` † |
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

`transport.IPCTransport.FilePerm` and `transport.TLSTransport.HandshakeTimeout`
work the same way.

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

Each is a deliberate v1.0 deferral, not an oversight. All touch engine
internals and none is needed for the common patterns.

| libzmq option | What you lose | Workaround |
| --- | --- | --- |
| `ZMQ_CONFLATE` | Last-value-caching queues (depth 1). | No close equivalent. `WithRecvHWM(1)` bounds the queue to one message but keeps the *oldest* and backpressures (or, on PUB, drops) the rest — conflate keeps the newest. Conflate semantics need application-level handling for now. |
| `ZMQ_ROUTER_HANDOVER` | Choosing the handover policy. | gozmq always hands over; see below. |
| `ZMQ_REQ_RELAXED` / `ZMQ_REQ_CORRELATE` | Relaxed REQ that can re-send without a reply. | Use `DEALER`, which has no FSM to relax. |
| `ZMQ_PROBE_ROUTER` | Auto-probe message on connect so a ROUTER learns a peer early. | Send an application-level hello. |
| `ZMQ_IMMEDIATE` | Queueing only to completed connections. | gozmq attaches pipes only after the handshake completes, so it already behaves as `IMMEDIATE=1`. Not configurable. |
| `ZMQ_HEARTBEAT_TTL` | Setting the TTL advertised in PING. | gozmq advertises its own `WithHeartbeat` timeout as the TTL, per the spec's SHOULD. |
| `ZMQ_BACKLOG` | Tuning the listen backlog. | OS default. |
| `ZMQ_TOS` | Setting IP type-of-service. | — |
| `ZMQ_STREAM_NOTIFY` | Synthetic connect/disconnect messages on STREAM. | Use a monitor channel; see below. |
| `ZMQ_CURVE_*` rekey | Rekeying a long-lived CURVE session. | See the nonce-exhaustion note below. |
| `ZMQ_GSSAPI_*` | GSSAPI. | — |
| `ZMQ_MULTICAST_*` | PGM tuning. | No multicast transport. |

---

## Behavioural divergences

These apply where both libzmq and gozmq implement a feature but behave
differently. Each is intentional.

### ROUTER identity handover

gozmq always hands over: a peer claiming an identity already in use
displaces the existing pipe. libzmq's **default** (`ZMQ_ROUTER_HANDOVER=0`)
is the opposite — it rejects the new peer and keeps the stale pipe bound.

Handover is the better default in practice: a client that reconnects before
the server's TCP stack has noticed the old connection died is otherwise
stranded until the stale pipe times out, which is the common case behind a
NAT or load balancer. The cost is that two clients sharing a `WithIdentity`
value silently evict each other instead of one failing loudly. **Identities
must be unique.**

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
  domain) are **rejected** by `NewSocket` rather than silently ignored —
  accepting `WithPlainAuth` on a raw-TCP listener that authenticates nobody
  would be a security hazard.

### inproc bypasses ZAP

`inproc://` connections between NULL-mechanism sockets in the same Context
use a direct-pipe bypass that skips the session layer, and therefore the ZAP
hook. libzmq also performs no ZAP check on inproc, so this matches — but it
does mean a Context-level ZAP handler never sees inproc peers.

### CURVE nonce exhaustion

RFC 26 expects rekeying long before the per-direction 64-bit nonce counter
wraps. gozmq does not implement rekey; a session that would wrap is torn
down cleanly with `ErrNonceExhausted`. Reaching this requires ~2^64 frames on
one session, so it is a defensive check rather than an operational limit —
but if you see it in logs, rotate keys and reconnect.

### libzmq's malformed PLAIN ERROR command

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
command name is accepted.

gozmq itself always emits well-formed ERROR commands.

### Message ownership

`RecvMsg` may return a `*Msg` backed by pooled buffers. Call `Release` when
done. This has no libzmq analogue (`zmq_msg_close` is closest) and is a
performance concession; `RecvBytes` avoids the bookkeeping entirely for
single-frame payloads.

---

## Post-1.0 candidates

Ordered roughly by likely demand:

1. `ZMQ_CONFLATE` — genuinely useful for last-value-cache telemetry.
2. `ZMQ_ROUTER_HANDOVER` as a configurable option, for parity.
3. Draft socket types (`CLIENT`/`SERVER`, `RADIO`/`DISH`), if they stabilise.
4. `ZMQ_REQ_RELAXED` / `ZMQ_REQ_CORRELATE`.
5. CURVE rekey.
6. UDP and PGM transports.
