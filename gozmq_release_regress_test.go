package gozmq_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
)

// curveOpts returns matching WithCurveServer / WithCurveClient options for a
// fresh server and client keypair.
func curveOpts(t *testing.T) (server, client gozmq.Option) {
	t.Helper()
	serverPub, serverSec := testutil.CurveKeys(t)
	clientPub, clientSec := testutil.CurveKeys(t)
	return gozmq.WithCurveServer(serverSec[:]), gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:])
}

// TestCurve_SmallMaxMessageSizeStillHandshakes: handshake frames (a CURVE
// INITIATE is ~280 bytes) are bounded independently of WithMaxMessageSize,
// which used to cap the handshake writer too and break CURVE below ~300 B.
func TestCurve_SmallMaxMessageSizeStillHandshakes(t *testing.T) {
	srvOpt, cliOpt := curveOpts(t)
	_, srv, cli := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair, "tcp://127.0.0.1:0",
		[]gozmq.Option{srvOpt, gozmq.WithMaxMessageSize(128)},
		[]gozmq.Option{cliOpt, gozmq.WithMaxMessageSize(128)})
	ctx := testutil.Ctx(t)
	require.NoError(t, cli.SendBytes(ctx, make([]byte, 100)))
	got, err := srv.RecvBytes(ctx)
	require.NoError(t, err)
	require.Len(t, got, 100)
}

// TestCurve_MaxMessageSizeBoundsPlaintext: WithMaxMessageSize is a plaintext
// bound. A frame of exactly the cap must be delivered over CURVE (the 33-byte
// MESSAGE overhead used to make the writer reject it and tear the session
// down after SendMsg had accepted it), one byte more is a synchronous error,
// and the session stays up.
func TestCurve_MaxMessageSizeBoundsPlaintext(t *testing.T) {
	const limit = 1000
	srvOpt, cliOpt := curveOpts(t)
	_, srv, cli := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair, "tcp://127.0.0.1:0",
		[]gozmq.Option{srvOpt, gozmq.WithMaxMessageSize(limit)},
		[]gozmq.Option{cliOpt, gozmq.WithMaxMessageSize(limit)})
	ctx := testutil.Ctx(t)
	for i := 0; i < 2; i++ {
		require.NoError(t, cli.SendBytes(ctx, make([]byte, limit)), "send #%d of exactly the cap", i)
		got, err := srv.RecvBytes(ctx)
		require.NoError(t, err, "recv #%d", i)
		require.Len(t, got, limit)
		require.ErrorIs(t, cli.SendBytes(ctx, make([]byte, limit+1)), gozmq.ErrMalformedFrame)
	}
}

// TestCurve_TwoServersFailFast: a client and a server that both carry
// WithCurveServer used to sit waiting for each other's HELLO until
// WithHandshakeTimeout (30 s by default). The greeting's as-server flags
// now expose the misconfiguration immediately.
func TestCurve_TwoServersFailFast(t *testing.T) {
	_, serverSec := testutil.CurveKeys(t)
	_, otherSec := testutil.CurveKeys(t)
	ctx := gozmq.NewContext()
	t.Cleanup(func() { _ = ctx.Close() })
	srv, err := ctx.NewSocket(gozmq.Router, gozmq.WithCurveServer(serverSec[:]))
	require.NoError(t, err)
	require.NoError(t, srv.Bind("tcp://127.0.0.1:0"))

	events := make(chan gozmq.Event, 64)
	cli, err := ctx.NewSocket(gozmq.Dealer, gozmq.WithCurveServer(otherSec[:]),
		gozmq.WithMonitor(events, gozmq.EventHandshakeFailedProtocol|gozmq.EventHandshakeFailedAuth))
	require.NoError(t, err)
	require.NoError(t, cli.Connect(srv.LastEndpoint()))
	select {
	case ev := <-events:
		require.Contains(t, ev.Reason, "as-server")
	case <-time.After(5 * time.Second):
		t.Fatal("two CURVE servers did not fail the handshake promptly")
	}
}

// TestCloseCtx_NilContext: a nil ctx must behave as context.Background,
// not panic after the socket is already marked closed.
func TestCloseCtx_NilContext(t *testing.T) {
	_, _, cli := newSocketPair(t, gozmq.Pair, "tcp://127.0.0.1:0")
	require.NoError(t, cli.SendBytes(testutil.Ctx(t), []byte("x")))
	var nilCtx context.Context // a nil interface, as a careless caller would pass
	require.NoError(t, cli.CloseCtx(nilCtx))
	require.NoError(t, cli.CloseCtx(context.Background()))
}

// TestInproc_WiringRaceLeavesNoStalePeer: a pending inproc Connect that is
// resolved by a Bind on one goroutine while the connector is torn down on
// another must not leave the binder with a dead pipe attached. For a PUB
// binder such a pipe counted a drop on every later publish.
func TestInproc_WiringRaceLeavesNoStalePeer(t *testing.T) {
	ctx := gozmq.NewContext()
	t.Cleanup(func() { _ = ctx.Close() })
	callCtx := testutil.Ctx(t)
	for i := 0; i < 200; i++ {
		ep := "inproc://wiring-race"
		pub, err := ctx.NewSocket(gozmq.Pub)
		require.NoError(t, err)
		sub, err := ctx.NewSocket(gozmq.Sub, gozmq.WithSubscribe(nil))
		require.NoError(t, err)
		require.NoError(t, sub.Connect(ep)) // pending: nothing bound yet

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); require.NoError(t, pub.Bind(ep)) }()
		go func() { defer wg.Done(); _ = sub.Disconnect(ep) }()
		wg.Wait()
		require.NoError(t, sub.Close())

		// With no live subscriber a publish must count no drop: a drop here
		// means a closed pipe is still attached to the publisher.
		require.NoError(t, pub.SendBytes(callCtx, []byte("after")))
		require.Zero(t, pub.Metrics().Dropped, "iteration %d: stale inproc pipe left attached to PUB", i)
		require.NoError(t, pub.Close())
	}
}
