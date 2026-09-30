# gozmq examples

These programs are small, runnable demos for the public `gozmq` API. Each
directory is self-contained so you can copy one `main.go` into an application
and trim it down from there.

Run any demo from the repository root:

```sh
go run ./examples/pair_ping
go run ./examples/reqrep_service
go run ./examples/pubsub_topics
go run ./examples/pushpull_pipeline
go run ./examples/router_dealer_workers
go run ./examples/monitor_events
go run ./examples/proxy_xsub_xpub
go run ./examples/security_plain
go run ./examples/security_curve
go run ./examples/stream_echo
go run ./examples/typed_pubsub
go run ./examples/typed_topicmux
```

## Demos

- `pair_ping`: minimal PAIR round trip with a multipart request and single-frame reply.
- `reqrep_service`: REQ/REP client and service loop, showing strict request/reply alternation.
- `pubsub_topics`: PUB/SUB topic filters with `WithSubscribe`, `Subscribe`, and `Unsubscribe`.
- `pushpull_pipeline`: PUSH/PULL work distribution across multiple workers.
- `router_dealer_workers`: ROUTER/DEALER identity routing and multipart envelopes.
- `monitor_events`: socket lifecycle monitoring with buffered event channels.
- `proxy_xsub_xpub`: `gozmq.Proxy` as a PUB/SUB forwarder, with an optional capture tap.
- `security_plain`: username/password authentication with PLAIN.
- `security_curve`: CurveZMQ key generation and encrypted message exchange.
- `stream_echo`: STREAM socket talking to a plain TCP client.
- `typed_pubsub`: generic typed PUB/SUB helpers with the JSON codec subpackage.
- `typed_topicmux`: heterogeneous typed topic dispatch with `typed.TopicMux`
  and longest-prefix matching.

Most examples bind to `tcp://127.0.0.1:0` by default and use
`Socket.LastEndpoint()` to discover the selected port. `pair_ping` can also run
over `inproc://`.

Frames returned by `Socket.RecvMsg` are ordinary Go memory owned by the
caller; nothing needs releasing. (The exception is a socket created with
`WithRecvBufferPool(true)`, which none of these demos use; see its
documentation.) `Socket.RecvBytes` (see `monitor_events`) is the
single-frame shortcut.
