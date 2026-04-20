//go:build interop

package interop

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/rkm/gozmq"
)

func TestReconnect_GozmqClient_LibzmqServer(t *testing.T) {
	const backoff = 25 * time.Millisecond
	endpoint := fixedTCPEndpoint(t)

	libCtx := newLibCtx(t)
	server := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server.Close() })
	bindLibWithRetry(t, server, endpoint, time.Second)

	gCtx := newGozmqCtx(t)
	events := make(chan gozmq.Event, 32)
	client, err := gCtx.NewSocket(
		gozmq.Pair,
		gozmq.WithMonitor(events, gozmq.EventAll),
		gozmq.WithReconnectInterval(backoff),
		gozmq.WithReconnectIntervalMax(backoff),
	)
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(endpoint); err != nil {
		t.Fatalf("gozmq connect: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, events)
	if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
		t.Fatalf("initial handshake event = %v, want %v", ev, gozmq.EventHandshakeSucceeded)
	}

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()
	if err := client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("before-rebind"))); err != nil {
		t.Fatalf("gozmq send before rebind: %v", err)
	}
	got, err := server.Recv()
	if err != nil {
		t.Fatalf("libzmq recv before rebind: %v", err)
	}
	assertFramesEqual(t, got, [][]byte{[]byte("before-rebind")})

	if err := server.Close(); err != nil {
		t.Fatalf("libzmq close: %v", err)
	}
	if ev := waitForEventType(waitCtx, events, gozmq.EventDisconnected); ev == nil {
		t.Fatal("missing EventDisconnected after libzmq close")
	}

	server2 := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server2.Close() })
	bindLibWithRetry(t, server2, endpoint, time.Second)

	if ev := waitForEventType(waitCtx, events, gozmq.EventConnected); ev == nil {
		t.Fatal("missing EventConnected after libzmq rebind")
	}

	if err := client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("after-rebind"))); err != nil {
		t.Fatalf("gozmq send after rebind: %v", err)
	}
	got, err = server2.Recv()
	if err != nil {
		t.Fatalf("libzmq recv after rebind: %v", err)
	}
	assertFramesEqual(t, got, [][]byte{[]byte("after-rebind")})
}

func TestReconnect_LibzmqClient_GozmqServer(t *testing.T) {
	const backoff = 25 * time.Millisecond
	endpoint := fixedTCPEndpoint(t)

	gCtx := newGozmqCtx(t)
	serverEvents := make(chan gozmq.Event, 32)
	server, err := gCtx.NewSocket(gozmq.Pair, gozmq.WithMonitor(serverEvents, gozmq.EventAll))
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	bindGozmqWithRetry(t, server, endpoint, time.Second)

	libCtx := newLibCtx(t)
	client := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.SetOption(ZMQ_RECONNECT_IVL, int(backoff/time.Millisecond)); err != nil {
		t.Fatalf("libzmq RECONNECT_IVL: %v", err)
	}
	mon, monEP := newLibMonitor(t, libCtx, client)
	t.Cleanup(func() { _ = mon.Close() })
	if err := client.Connect(endpoint); err != nil {
		t.Fatalf("libzmq connect: %v", err)
	}
	if ev := waitForLibMonitorEvent(t, mon, ZMQ_EVENT_CONNECTED, 5*time.Second); ev == nil {
		t.Fatalf("missing initial ZMQ_EVENT_CONNECTED on libzmq monitor %q", monEP)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, serverEvents)
	if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
		t.Fatalf("initial handshake event = %v, want %v", ev, gozmq.EventHandshakeSucceeded)
	}

	if err := client.Send([][]byte{[]byte("before-rebind")}); err != nil {
		t.Fatalf("libzmq send before rebind: %v", err)
	}
	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()
	msg, err := server.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv before rebind: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), [][]byte{[]byte("before-rebind")})

	if err := server.Close(); err != nil {
		t.Fatalf("gozmq close: %v", err)
	}
	if ev := waitForLibMonitorEvent(t, mon, ZMQ_EVENT_DISCONNECTED, 5*time.Second); ev == nil {
		t.Fatalf("missing ZMQ_EVENT_DISCONNECTED on libzmq monitor %q", monEP)
	}

	server2, err := gCtx.NewSocket(gozmq.Pair, gozmq.WithMonitor(serverEvents, gozmq.EventAll))
	if err != nil {
		t.Fatalf("gozmq new rebind: %v", err)
	}
	t.Cleanup(func() { _ = server2.Close() })
	bindGozmqWithRetry(t, server2, endpoint, time.Second)

	if ev := waitForLibMonitorEvent(t, mon, ZMQ_EVENT_CONNECTED, 5*time.Second); ev == nil {
		t.Fatalf("missing reconnect ZMQ_EVENT_CONNECTED on libzmq monitor %q", monEP)
	}

	if err := client.Send([][]byte{[]byte("after-rebind")}); err != nil {
		t.Fatalf("libzmq send after rebind: %v", err)
	}
	msg, err = server2.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv after rebind: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), [][]byte{[]byte("after-rebind")})
}

func fixedTCPEndpoint(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("release reserved port: %v", err)
	}
	return "tcp://" + addr
}

func bindLibWithRetry(t *testing.T, s *Sock, endpoint string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := s.Bind(endpoint)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("libzmq bind %q: %v", endpoint, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func bindGozmqWithRetry(t *testing.T, s *gozmq.Socket, endpoint string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := s.Bind(endpoint)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("gozmq bind %q: %v", endpoint, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
