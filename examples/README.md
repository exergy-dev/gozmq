# gozmq examples

Runnable programs for the parts of the API that the `Example*` functions in
the package's own tests (`example_test.go`: PAIR, REQ/REP, PUB/SUB,
ROUTER/DEALER, Proxy) do not show. Each directory is self-contained.

```sh
go run ./examples/security -mech curve   # PLAIN or CURVE authenticated exchange
go run ./examples/monitor_events         # socket lifecycle events on a channel
go run ./examples/proxy_xsub_xpub        # gozmq.Proxy as a PUB/SUB forwarder with a capture tap
go run ./examples/stream_echo            # STREAM socket talking to a plain TCP client
go run ./examples/typed_topicmux         # typed topic dispatch with typed.TopicMux
```

The examples bind to `tcp://127.0.0.1:0` and discover the port through
`Socket.LastEndpoint()`. Frames from `RecvMsg` are ordinary Go memory owned by
the caller unless the socket uses `WithRecvBufferPool(true)`, which none of
these do.
