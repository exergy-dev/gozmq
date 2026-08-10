package gozmq_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
)

// startProxy runs Proxy in the background and returns a stop function that
// cancels it and waits for the goroutine to finish, asserting a clean exit.
func startProxy(t *testing.T, frontend, backend, capture *gozmq.Socket) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gozmq.Proxy(ctx, frontend, backend, capture) }()

	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				require.NoError(t, err, "Proxy returned an error on cancellation")
			case <-time.After(5 * time.Second):
				t.Error("Proxy did not return within 5s of cancellation")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// TestProxyXSubXPub covers the canonical pub/sub forwarder. The interesting
// property is that subscriptions propagate upstream through the proxy with
// no special handling: SUB's SUBSCRIBE reaches XPUB, is surfaced as a
// message, is forwarded to XSUB, and re-emitted to the real PUB.
func TestProxyXSubXPub(t *testing.T) {
	zctx := gozmq.NewContext()
	defer func() { _ = zctx.Close() }()

	// Upstream publisher.
	pub, err := zctx.NewSocket(gozmq.Pub)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }()
	require.NoError(t, pub.Bind("tcp://127.0.0.1:0"))

	// Proxy frontend (subscribes upstream) and backend (serves clients).
	xsub, err := zctx.NewSocket(gozmq.XSub)
	require.NoError(t, err)
	defer func() { _ = xsub.Close() }()
	require.NoError(t, xsub.Connect(pub.LastEndpoint()))

	xpub, err := zctx.NewSocket(gozmq.XPub)
	require.NoError(t, err)
	defer func() { _ = xpub.Close() }()
	require.NoError(t, xpub.Bind("tcp://127.0.0.1:0"))

	startProxy(t, xsub, xpub, nil)

	sub, err := zctx.NewSocket(gozmq.Sub, gozmq.WithSubscribe([]byte("topic.")))
	require.NoError(t, err)
	defer func() { _ = sub.Close() }()
	require.NoError(t, sub.Connect(xpub.LastEndpoint()))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// PUB/SUB has no delivery guarantee before the subscription has
	// propagated through both hops, so publish until one arrives.
	got := recvWithRetry(ctx, t, sub, func() {
		require.NoError(t, pub.SendBytes(ctx, []byte("topic.a|payload")))
	})
	require.Equal(t, "topic.a|payload", string(got))

	// A message outside the subscription must not come through.
	require.NoError(t, pub.SendBytes(ctx, []byte("other|nope")))
	require.NoError(t, pub.SendBytes(ctx, []byte("topic.b|yes")))

	next, err := sub.RecvBytes(ctx)
	require.NoError(t, err)
	require.Equal(t, "topic.b|yes", string(next), "unsubscribed topic leaked through the proxy")
}

// TestProxyRouterDealer covers the request/reply broker. The ROUTER
// identity envelope must survive both hops for the reply to route home.
func TestProxyRouterDealer(t *testing.T) {
	zctx := gozmq.NewContext()
	defer func() { _ = zctx.Close() }()

	frontend, err := zctx.NewSocket(gozmq.Router)
	require.NoError(t, err)
	defer func() { _ = frontend.Close() }()
	require.NoError(t, frontend.Bind("tcp://127.0.0.1:0"))

	backend, err := zctx.NewSocket(gozmq.Dealer)
	require.NoError(t, err)
	defer func() { _ = backend.Close() }()
	require.NoError(t, backend.Bind("tcp://127.0.0.1:0"))

	startProxy(t, frontend, backend, nil)

	// Two workers, so the DEALER's round-robin is exercised.
	const workers = 2
	var wg sync.WaitGroup
	workerCtx, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()

	for i := range workers {
		w, err := zctx.NewSocket(gozmq.Rep)
		require.NoError(t, err)
		defer func() { _ = w.Close() }()
		require.NoError(t, w.Connect(backend.LastEndpoint()))

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				req, err := w.RecvMsg(workerCtx)
				if err != nil {
					return
				}
				body := string(req.Frames()[len(req.Frames())-1])
				req.Release()
				if err := w.SendBytes(workerCtx, []byte("re:"+body)); err != nil {
					return
				}
			}
		}()
		_ = i
	}
	defer wg.Wait()

	client, err := zctx.NewSocket(gozmq.Req)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	require.NoError(t, client.Connect(frontend.LastEndpoint()))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := range 6 {
		body := fmt.Sprintf("req-%d", i)
		require.NoError(t, client.SendBytes(ctx, []byte(body)))
		reply, err := client.RecvBytes(ctx)
		require.NoError(t, err, "no reply for %s", body)
		require.Equal(t, "re:"+body, string(reply))
	}

	stopWorkers()
}

// TestProxyPushPullWithCapture covers the pipeline forwarder and the
// capture socket: every forwarded message must also reach the tap.
func TestProxyPushPullWithCapture(t *testing.T) {
	zctx := gozmq.NewContext()
	defer func() { _ = zctx.Close() }()

	pull, err := zctx.NewSocket(gozmq.Pull)
	require.NoError(t, err)
	defer func() { _ = pull.Close() }()
	require.NoError(t, pull.Bind("tcp://127.0.0.1:0"))

	push, err := zctx.NewSocket(gozmq.Push)
	require.NoError(t, err)
	defer func() { _ = push.Close() }()
	require.NoError(t, push.Bind("tcp://127.0.0.1:0"))

	// The capture tap: PAIR rather than PUB so delivery is deterministic
	// and the assertion cannot be flaky on subscription timing.
	capSrv, err := zctx.NewSocket(gozmq.Pair)
	require.NoError(t, err)
	defer func() { _ = capSrv.Close() }()
	require.NoError(t, capSrv.Bind("inproc://proxy-capture"))

	capSink, err := zctx.NewSocket(gozmq.Pair)
	require.NoError(t, err)
	defer func() { _ = capSink.Close() }()
	require.NoError(t, capSink.Connect("inproc://proxy-capture"))

	startProxy(t, pull, push, capSrv)

	source, err := zctx.NewSocket(gozmq.Push)
	require.NoError(t, err)
	defer func() { _ = source.Close() }()
	require.NoError(t, source.Connect(pull.LastEndpoint()))

	sink, err := zctx.NewSocket(gozmq.Pull)
	require.NoError(t, err)
	defer func() { _ = sink.Close() }()
	require.NoError(t, sink.Connect(push.LastEndpoint()))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const n = 20
	for i := range n {
		require.NoError(t, source.SendBytes(ctx, []byte(fmt.Sprintf("m-%d", i))))
	}
	for i := range n {
		got, err := sink.RecvBytes(ctx)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("m-%d", i), string(got))
	}
	for i := range n {
		got, err := capSink.RecvBytes(ctx)
		require.NoError(t, err, "capture socket missed message %d", i)
		require.Equal(t, fmt.Sprintf("m-%d", i), string(got))
	}
}

// TestProxyMultipartPreserved checks that frame boundaries survive the hop
// — a proxy that flattened multipart messages would silently corrupt every
// ROUTER envelope.
func TestProxyMultipartPreserved(t *testing.T) {
	zctx := gozmq.NewContext()
	defer func() { _ = zctx.Close() }()

	front, err := zctx.NewSocket(gozmq.Pull)
	require.NoError(t, err)
	defer func() { _ = front.Close() }()
	require.NoError(t, front.Bind("tcp://127.0.0.1:0"))

	back, err := zctx.NewSocket(gozmq.Push)
	require.NoError(t, err)
	defer func() { _ = back.Close() }()
	require.NoError(t, back.Bind("tcp://127.0.0.1:0"))

	startProxy(t, front, back, nil)

	source, err := zctx.NewSocket(gozmq.Push)
	require.NoError(t, err)
	defer func() { _ = source.Close() }()
	require.NoError(t, source.Connect(front.LastEndpoint()))

	sink, err := zctx.NewSocket(gozmq.Pull)
	require.NoError(t, err)
	defer func() { _ = sink.Close() }()
	require.NoError(t, sink.Connect(back.LastEndpoint()))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	want := [][]byte{[]byte("envelope"), {}, []byte("body-1"), []byte("body-2")}
	require.NoError(t, source.SendMsg(ctx, gozmq.NewMsgFromFrames(want...)))

	got, err := sink.RecvMsg(ctx)
	require.NoError(t, err)
	defer got.Release()
	require.Equal(t, len(want), got.NumFrames())
	for i, f := range got.Frames() {
		require.Equal(t, want[i], f, "frame %d", i)
	}
}

func TestProxyRejectsBadArguments(t *testing.T) {
	zctx := gozmq.NewContext()
	defer func() { _ = zctx.Close() }()

	s, err := zctx.NewSocket(gozmq.Pair)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	ctx := context.Background()

	require.Error(t, gozmq.Proxy(ctx, nil, s, nil))
	require.Error(t, gozmq.Proxy(ctx, s, nil, nil))

	err = gozmq.Proxy(ctx, s, s, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "distinct sockets")
}

// Closing a proxied socket is the documented alternative to cancelling the
// context; it must be reported as a clean stop, not an error.
func TestProxyStopsCleanlyOnSocketClose(t *testing.T) {
	zctx := gozmq.NewContext()
	defer func() { _ = zctx.Close() }()

	front, err := zctx.NewSocket(gozmq.Pull)
	require.NoError(t, err)
	require.NoError(t, front.Bind("inproc://proxy-close-front"))

	back, err := zctx.NewSocket(gozmq.Push)
	require.NoError(t, err)
	defer func() { _ = back.Close() }()
	require.NoError(t, back.Bind("inproc://proxy-close-back"))

	done := make(chan error, 1)
	go func() { done <- gozmq.Proxy(context.Background(), front, back, nil) }()

	// Give both directions time to park in Recv before pulling the rug.
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, front.Close())

	select {
	case err := <-done:
		require.NoError(t, err, "socket close should be a clean proxy stop")
	case <-time.After(5 * time.Second):
		t.Fatal("Proxy did not return after the frontend socket was closed")
	}
}

// recvWithRetry re-runs send until sub receives something or ctx expires.
// PUB/SUB drops messages published before the subscription has propagated,
// which through a proxy takes two hops.
func recvWithRetry(ctx context.Context, t *testing.T, sub *gozmq.Socket, send func()) []byte {
	t.Helper()
	for {
		if ctx.Err() != nil {
			t.Fatal("subscription never propagated through the proxy")
		}
		send()
		attempt, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		got, err := sub.RecvBytes(attempt)
		cancel()
		if err == nil {
			return got
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			require.NoError(t, err)
		}
	}
}
