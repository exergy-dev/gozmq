package gozmq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rkm/gozmq"
)

func TestSocketDisconnect_StopsReconnectAndDetachesPeer(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	srvEvents := make(chan gozmq.Event, 64)
	srv, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithMonitor(srvEvents, gozmq.EventAll))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()
	if err := srv.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	ep := srv.LastEndpoint()

	cliEvents := make(chan gozmq.Event, 64)
	cli, err := ctxZmq.NewSocket(
		gozmq.Pair,
		gozmq.WithMonitor(cliEvents, gozmq.EventAll),
		gozmq.WithReconnectInterval(10*time.Millisecond),
		gozmq.WithReconnectIntervalMax(20*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cli.Close()
	if err := cli.Connect(ep); err != nil {
		t.Fatalf("connect: %v", err)
	}

	callCtx, cancel := testCtx(t)
	defer cancel()
	if err := cli.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("hello"))); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := srv.RecvMsg(callCtx); err != nil {
		t.Fatalf("recv: %v", err)
	}

	srvCollected := waitForEvents(t, srvEvents, 3*time.Second, func(evts []gozmq.Event) bool {
		return hasType(evts, gozmq.EventAccepted) && hasType(evts, gozmq.EventHandshakeSucceeded)
	})
	if !hasType(srvCollected, gozmq.EventAccepted) || !hasType(srvCollected, gozmq.EventHandshakeSucceeded) {
		t.Fatalf("initial server events = %v", eventTypes(srvCollected))
	}
	cliCollected := waitForEvents(t, cliEvents, 3*time.Second, func(evts []gozmq.Event) bool {
		return hasType(evts, gozmq.EventConnected) && hasType(evts, gozmq.EventHandshakeSucceeded)
	})
	if !hasType(cliCollected, gozmq.EventConnected) || !hasType(cliCollected, gozmq.EventHandshakeSucceeded) {
		t.Fatalf("initial client events = %v", eventTypes(cliCollected))
	}

	if err := cli.Disconnect(ep); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	postDisconnect := waitForEvents(t, srvEvents, 2*time.Second, func(evts []gozmq.Event) bool {
		return hasType(evts, gozmq.EventDisconnected)
	})
	if !hasType(postDisconnect, gozmq.EventDisconnected) {
		t.Fatalf("server did not observe disconnect; events=%v", eventTypes(postDisconnect))
	}

	srvQuietCtx, srvQuietCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	srvTail := drainEvents(srvQuietCtx, srvEvents)
	srvQuietCancel()
	cliQuietCtx, cliQuietCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	cliTail := drainEvents(cliQuietCtx, cliEvents)
	cliQuietCancel()
	if hasType(srvTail, gozmq.EventAccepted) {
		t.Fatalf("server accepted a reconnect after Disconnect; tail=%v", eventTypes(srvTail))
	}
	if hasType(cliTail, gozmq.EventConnectDelayed) || hasType(cliTail, gozmq.EventConnectRetried) {
		t.Fatalf("client retried after Disconnect; tail=%v", eventTypes(cliTail))
	}

	if err := cli.Disconnect(ep); !errors.Is(err, gozmq.ErrEndpointInvalid) {
		t.Fatalf("second Disconnect err = %v, want ErrEndpointInvalid", err)
	}
}
