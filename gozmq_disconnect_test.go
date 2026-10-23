package gozmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
)

func TestSocketDisconnect_StopsReconnectAndDetachesPeer(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	srvEvents := make(chan gozmq.Event, 64)
	srv, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithMonitor(srvEvents, gozmq.EventAll))
	require.NoError(t, err, "new server")
	defer srv.Close()
	require.NoError(t, srv.Bind("tcp://127.0.0.1:0"), "bind")
	ep := srv.LastEndpoint()

	cliEvents := make(chan gozmq.Event, 64)
	cli, err := ctxZmq.NewSocket(
		gozmq.Pair,
		gozmq.WithMonitor(cliEvents, gozmq.EventAll),
		gozmq.WithReconnectInterval(10*time.Millisecond),
		gozmq.WithReconnectIntervalMax(20*time.Millisecond),
	)
	require.NoError(t, err, "new client")
	defer cli.Close()
	require.NoError(t, cli.Connect(ep), "connect")

	callCtx := testutil.Ctx(t)
	require.NoError(t, cli.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("hello"))), "send")
	_, err = srv.RecvMsg(callCtx)
	require.NoError(t, err, "recv")

	srvCollected := waitForEvents(t, srvEvents, 3*time.Second, func(evts []gozmq.Event) bool {
		return hasType(evts, gozmq.EventAccepted) && hasType(evts, gozmq.EventHandshakeSucceeded)
	})
	require.True(t, hasType(srvCollected, gozmq.EventAccepted) && hasType(srvCollected, gozmq.EventHandshakeSucceeded),
		"initial server events = %v", eventTypes(srvCollected))
	cliCollected := waitForEvents(t, cliEvents, 3*time.Second, func(evts []gozmq.Event) bool {
		return hasType(evts, gozmq.EventConnected) && hasType(evts, gozmq.EventHandshakeSucceeded)
	})
	require.True(t, hasType(cliCollected, gozmq.EventConnected) && hasType(cliCollected, gozmq.EventHandshakeSucceeded),
		"initial client events = %v", eventTypes(cliCollected))

	require.NoError(t, cli.Disconnect(ep), "Disconnect")

	postDisconnect := waitForEvents(t, srvEvents, 2*time.Second, func(evts []gozmq.Event) bool {
		return hasType(evts, gozmq.EventDisconnected)
	})
	require.True(t, hasType(postDisconnect, gozmq.EventDisconnected),
		"server did not observe disconnect; events=%v", eventTypes(postDisconnect))

	srvQuietCtx, srvQuietCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	srvTail := drainEvents(srvQuietCtx, srvEvents)
	srvQuietCancel()
	cliQuietCtx, cliQuietCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	cliTail := drainEvents(cliQuietCtx, cliEvents)
	cliQuietCancel()
	require.False(t, hasType(srvTail, gozmq.EventAccepted),
		"server accepted a reconnect after Disconnect; tail=%v", eventTypes(srvTail))
	require.False(t, hasType(cliTail, gozmq.EventConnectDelayed) || hasType(cliTail, gozmq.EventConnectRetried),
		"client retried after Disconnect; tail=%v", eventTypes(cliTail))

	require.ErrorIs(t, cli.Disconnect(ep), gozmq.ErrEndpointUnknown, "second Disconnect")
}
