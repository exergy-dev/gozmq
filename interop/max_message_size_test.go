//go:build interop

package interop

import (
	"context"
	"testing"
	"time"

	"github.com/rkm/gozmq"
)

func TestMaxMessageSize_LibzmqClient_GozmqServer(t *testing.T) {
	const limit = 1024

	gCtx := newGozmqCtx(t)
	events := make(chan gozmq.Event, 16)
	server, err := gCtx.NewSocket(
		gozmq.Pair,
		gozmq.WithMaxMessageSize(limit),
		gozmq.WithMonitor(events, gozmq.EventAll),
	)
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("gozmq bind: %v", err)
	}
	ep := server.LastEndpoint()
	if ep == "" {
		t.Fatal("gozmq LastEndpoint empty after bind")
	}

	libCtx := newLibCtx(t)
	client := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(ep); err != nil {
		t.Fatalf("libzmq connect: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, events)
	if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
		t.Fatalf("handshake event = %v, want %v", ev, gozmq.EventHandshakeSucceeded)
	}

	if err := client.Send([][]byte{make([]byte, limit+1)}); err != nil {
		t.Fatalf("libzmq oversize send: %v", err)
	}
	if ev := waitForEventType(waitCtx, events, gozmq.EventDisconnected); ev == nil {
		t.Fatal("missing EventDisconnected after libzmq oversize send")
	}
}

func TestMaxMessageSize_GozmqClient_LibzmqServer(t *testing.T) {
	const limit = 1024

	libCtx := newLibCtx(t)
	server := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server.Close() })
	if err := server.SetOption(ZMQ_MAXMSGSIZE, int64(limit)); err != nil {
		t.Fatalf("libzmq MAXMSGSIZE: %v", err)
	}
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("libzmq bind: %v", err)
	}
	ep, err := server.LastEndpoint()
	if err != nil {
		t.Fatalf("LastEndpoint: %v", err)
	}

	mon, monEP := newLibMonitor(t, libCtx, server)
	t.Cleanup(func() { _ = mon.Close() })

	gCtx := newGozmqCtx(t)
	events := make(chan gozmq.Event, 16)
	client, err := gCtx.NewSocket(gozmq.Pair, gozmq.WithMonitor(events, gozmq.EventAll))
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(ep); err != nil {
		t.Fatalf("gozmq connect: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, events)
	if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
		t.Fatalf("handshake event = %v, want %v", ev, gozmq.EventHandshakeSucceeded)
	}
	if ev := waitForLibMonitorEvent(t, mon, ZMQ_EVENT_ACCEPTED, 5*time.Second); ev == nil {
		t.Fatalf("missing ZMQ_EVENT_ACCEPTED on libzmq monitor %q", monEP)
	}

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()
	if err := client.SendMsg(callCtx, gozmq.NewMsgFromBytes(make([]byte, limit+1))); err != nil {
		t.Fatalf("gozmq oversize send: %v", err)
	}
	if ev := waitForLibMonitorEvent(t, mon, ZMQ_EVENT_DISCONNECTED, 5*time.Second); ev == nil {
		t.Fatalf("missing ZMQ_EVENT_DISCONNECTED on libzmq monitor %q", monEP)
	}
}
