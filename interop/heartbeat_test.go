//go:build interop

package interop

import (
	"context"
	"testing"
	"time"

	"github.com/rkm/gozmq"
)

const (
	heartbeatInterval = 200 * time.Millisecond
	heartbeatTimeout  = time.Second
)

func TestHeartbeat_GozmqClient_LibzmqServer(t *testing.T) {
	requireLibzmqHeartbeat(t)

	libCtx := newLibCtx(t)
	server := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server.Close() })
	configureLibHeartbeat(t, server)
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("libzmq bind: %v", err)
	}
	ep, err := server.LastEndpoint()
	if err != nil {
		t.Fatalf("LastEndpoint: %v", err)
	}

	gCtx := newGozmqCtx(t)
	events := make(chan gozmq.Event, 16)
	client, err := gCtx.NewSocket(
		gozmq.Pair,
		gozmq.WithHeartbeat(heartbeatInterval, heartbeatTimeout),
		gozmq.WithMonitor(events, gozmq.EventAll),
	)
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

	time.Sleep(5 * heartbeatInterval)

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()

	want := [][]byte{[]byte("idle-still-alive")}
	if err := client.SendMsg(callCtx, gozmq.NewMsgFromFrames(want...)); err != nil {
		t.Fatalf("gozmq send after idle: %v", err)
	}
	got, err := server.Recv()
	if err != nil {
		t.Fatalf("libzmq recv after idle: %v", err)
	}
	assertFramesEqual(t, got, want)

	if err := server.Close(); err != nil {
		t.Fatalf("libzmq close: %v", err)
	}
	if ev := waitForEventType(waitCtx, events, gozmq.EventDisconnected); ev == nil {
		t.Fatal("missing EventDisconnected after libzmq peer close")
	}
}

func TestHeartbeat_LibzmqClient_GozmqServer(t *testing.T) {
	requireLibzmqHeartbeat(t)

	gCtx := newGozmqCtx(t)
	events := make(chan gozmq.Event, 16)
	server, err := gCtx.NewSocket(
		gozmq.Pair,
		gozmq.WithHeartbeat(heartbeatInterval, heartbeatTimeout),
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
	configureLibHeartbeat(t, client)
	if err := client.Connect(ep); err != nil {
		t.Fatalf("libzmq connect: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, events)
	if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
		t.Fatalf("handshake event = %v, want %v", ev, gozmq.EventHandshakeSucceeded)
	}

	time.Sleep(5 * heartbeatInterval)

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()

	want := [][]byte{[]byte("idle-still-alive")}
	if err := client.Send(want); err != nil {
		t.Fatalf("libzmq send after idle: %v", err)
	}
	msg, err := server.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv after idle: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), want)

	if err := client.Close(); err != nil {
		t.Fatalf("libzmq close: %v", err)
	}
	if ev := waitForEventType(waitCtx, events, gozmq.EventDisconnected); ev == nil {
		t.Fatal("missing EventDisconnected after libzmq peer close")
	}
}

func requireLibzmqHeartbeat(t *testing.T) {
	t.Helper()
	maj, min, pat := Version()
	if maj < 4 || (maj == 4 && min < 2) {
		t.Skipf("libzmq heartbeat requires >= 4.2.0; have %d.%d.%d", maj, min, pat)
	}
}

func configureLibHeartbeat(t *testing.T, s *Sock) {
	t.Helper()
	if err := s.SetOption(ZMQ_HEARTBEAT_IVL, int(heartbeatInterval/time.Millisecond)); err != nil {
		t.Fatalf("set HEARTBEAT_IVL: %v", err)
	}
	if err := s.SetOption(ZMQ_HEARTBEAT_TIMEOUT, int(heartbeatTimeout/time.Millisecond)); err != nil {
		t.Fatalf("set HEARTBEAT_TIMEOUT: %v", err)
	}
	if err := s.SetOption(ZMQ_HEARTBEAT_TTL, int(heartbeatTimeout/time.Millisecond)); err != nil {
		t.Fatalf("set HEARTBEAT_TTL: %v", err)
	}
}

func waitForEventType(ctx context.Context, ch <-chan gozmq.Event, typ gozmq.EventType) *gozmq.Event {
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return nil
			}
			if ev.Type == typ {
				ev := ev
				return &ev
			}
		case <-ctx.Done():
			return nil
		}
	}
}
