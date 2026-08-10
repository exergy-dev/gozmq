package gozmq_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
	"github.com/stretchr/testify/require"
)

// newTestSocket creates a Context plus a single socket of the given type.
// Cleanup of both is registered via t.Cleanup.
func newTestSocket(t *testing.T, typ gozmq.SocketType, opts ...gozmq.Option) (*gozmq.Context, *gozmq.Socket) {
	t.Helper()
	ctx := gozmq.NewContext()
	t.Cleanup(func() { _ = ctx.Close() })
	s, err := ctx.NewSocket(typ, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return ctx, s
}

// newSocketPair creates a Context with a bound server and connected client of
// the given type over the specified transport (e.g. "tcp://127.0.0.1:0" or
// "inproc://test"). Cleanup is registered via t.Cleanup.
func newSocketPair(t *testing.T, typ gozmq.SocketType, transport string, opts ...gozmq.Option) (*gozmq.Context, *gozmq.Socket, *gozmq.Socket) {
	t.Helper()
	return newSocketPairTypes(t, typ, typ, transport, opts, opts)
}

// newSocketPairTypes creates a Context with a bound server of serverType and a
// connected client of clientType over the specified transport. Server and
// client receive independent option lists. Cleanup is registered via
// t.Cleanup.
func newSocketPairTypes(
	t *testing.T,
	serverType, clientType gozmq.SocketType,
	transport string,
	serverOpts, clientOpts []gozmq.Option,
) (*gozmq.Context, *gozmq.Socket, *gozmq.Socket) {
	t.Helper()
	ctx := gozmq.NewContext()
	t.Cleanup(func() { _ = ctx.Close() })
	server, err := ctx.NewSocket(serverType, serverOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	require.NoError(t, server.Bind(transport))
	client, err := ctx.NewSocket(clientType, clientOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Connect(server.LastEndpoint()))
	return ctx, server, client
}

// newPubSubPair creates a bound PUB and a connected SUB subscribed to every
// prefix in topics (a prefix may repeat to bump its refcount), then BLOCKS
// until the subscription has demonstrably propagated to the PUB: probe
// messages on the first topic are published until one is delivered, and any
// probes still in flight are drained before returning. No probe messages
// remain queued on return, so tests can immediately assert on their own
// payloads.
func newPubSubPair(
	t *testing.T,
	endpoint string,
	topics [][]byte,
	pubOpts, subOpts []gozmq.Option,
) (*gozmq.Context, *gozmq.Socket, *gozmq.Socket) {
	t.Helper()
	ctx := gozmq.NewContext()
	t.Cleanup(func() { _ = ctx.Close() })
	pub, err := ctx.NewSocket(gozmq.Pub, pubOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })
	require.NoError(t, pub.Bind(endpoint))
	sub, err := ctx.NewSocket(gozmq.Sub, subOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })
	if len(topics) == 0 {
		topics = [][]byte{nil}
	}
	for _, topic := range topics {
		require.NoError(t, sub.Subscribe(topic))
	}
	require.NoError(t, sub.Connect(pub.LastEndpoint()))
	syncSub(t, pub, sub, topics[0])
	return ctx, pub, sub
}

// syncSub waits for a SUB's subscription to propagate to the PUB without a
// fixed sleep: it publishes sequence-numbered probe messages on topic until
// the SUB receives one, then drains every probe published after that point.
// PUB-side filtering drops the probes published before propagation; the pipe
// is FIFO, so once probe j is delivered every later probe is also delivered,
// and draining up to the final sequence number leaves the stream empty.
func syncSub(t *testing.T, pub, sub *gozmq.Socket, topic []byte) {
	t.Helper()
	callCtx := testCtx(t)
	probe := func(i int) []byte {
		return fmt.Appendf(nil, "%s\x00gozmq-sync-probe\x00%d", topic, i)
	}
	last, got := -1, []byte(nil)
	for got == nil {
		last++
		if err := pub.SendMsg(callCtx, gozmq.NewMsgFromBytes(probe(last))); err != nil {
			t.Fatalf("syncSub probe send %d: %v", last, err)
		}
		tryCtx, cancel := context.WithTimeout(callCtx, 25*time.Millisecond)
		m, err := sub.RecvMsg(tryCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && callCtx.Err() == nil {
				continue
			}
			t.Fatalf("syncSub probe recv: %v", err)
		}
		got = append([]byte(nil), m.Bytes()...)
		m.Release()
	}
	// Drain the remaining probes (those between the first delivered one and
	// the last published one).
	want := probe(last)
	for string(got) != string(want) {
		m, err := sub.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("syncSub drain recv: %v", err)
		}
		got = append(got[:0], m.Bytes()...)
		m.Release()
	}
}

// testCtx returns a context.Context with a 5-second timeout suitable for
// most test operations. Cancellation is registered via t.Cleanup.
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
