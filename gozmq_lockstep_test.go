package gozmq_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
)

// requireRecv waits for a parked RecvMsg and asserts it delivered want.
func requireRecv(t *testing.T, ch <-chan testutil.Result[*gozmq.Msg], want string) {
	t.Helper()
	r := <-ch // the RecvMsg's own context bounds the wait
	require.False(t, errors.Is(r.Err, io.EOF), "RecvMsg surfaced io.EOF on a transient disconnect")
	require.NoError(t, r.Err, "RecvMsg did not deliver the next peer's message")
	require.Equal(t, want, string(r.V.Bytes()))
	r.V.Release()
}

// TestPairReconnectingPeerKeepsWaiting: a connecting PAIR parked in RecvMsg
// when its bound peer goes away keeps waiting and receives from the peer
// that binds in its place, rather than failing with io.EOF.
func TestPairReconnectingPeerKeepsWaiting(t *testing.T) {
	t.Parallel()
	ctx := gozmq.NewContext()
	t.Cleanup(func() { _ = ctx.Close() })
	callCtx := testutil.Ctx(t)

	server, err := ctx.NewSocket(gozmq.Pair)
	require.NoError(t, err)
	require.NoError(t, server.Bind("tcp://127.0.0.1:0"))
	endpoint := server.LastEndpoint()

	client, err := ctx.NewSocket(gozmq.Pair, gozmq.WithReconnectInterval(10*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Connect(endpoint))

	require.NoError(t, server.SendBytes(callCtx, []byte("first")))
	got, err := client.RecvBytes(callCtx)
	require.NoError(t, err)
	require.Equal(t, "first", string(got))

	parked := testutil.AsyncErr(func() (*gozmq.Msg, error) { return client.RecvMsg(callCtx) })
	require.NoError(t, server.Close())
	testutil.RequireNoRecv(t, parked, 100*time.Millisecond, "RecvMsg returned while no peer was attached")

	server2, err := ctx.NewSocket(gozmq.Pair)
	require.NoError(t, err)
	t.Cleanup(func() { _ = server2.Close() })
	require.NoError(t, server2.Bind(endpoint))
	require.NoError(t, server2.SendBytes(callCtx, []byte("second")))
	requireRecv(t, parked, "second")
}

// TestPairBoundPeerAcceptsNextPeer: a bound PAIR parked in RecvMsg when its
// connected peer disconnects receives from the next peer to connect.
func TestPairBoundPeerAcceptsNextPeer(t *testing.T) {
	t.Parallel()
	ctx, server, client := newSocketPair(t, gozmq.Pair, "tcp://127.0.0.1:0")
	callCtx := testutil.Ctx(t)

	require.NoError(t, client.SendBytes(callCtx, []byte("first")))
	got, err := server.RecvBytes(callCtx)
	require.NoError(t, err)
	require.Equal(t, "first", string(got))

	parked := testutil.AsyncErr(func() (*gozmq.Msg, error) { return server.RecvMsg(callCtx) })
	require.NoError(t, client.Close())
	testutil.RequireNoRecv(t, parked, 100*time.Millisecond, "RecvMsg returned while no peer was attached")

	client2, err := ctx.NewSocket(gozmq.Pair)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client2.Close() })
	require.NoError(t, client2.Connect(server.LastEndpoint()))
	require.NoError(t, client2.SendBytes(callCtx, []byte("second")))
	requireRecv(t, parked, "second")
}
