//go:build interop

package interop

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
)

func TestTCP6_GozmqClient_LibzmqServer(t *testing.T) {
	libCtx := newLibCtx(t)
	server := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server.Close() })
	if err := server.SetOption(ZMQ_IPV6, 1); err != nil {
		t.Fatalf("libzmq IPV6: %v", err)
	}
	if err := server.Bind("tcp://[::1]:0"); err != nil {
		t.Skipf("tcp6 not available: %v", err)
	}
	ep, err := server.LastEndpoint()
	if err != nil {
		t.Fatalf("LastEndpoint: %v", err)
	}

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

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()

	want := [][]byte{[]byte("tcp6-gozmq"), []byte("to-libzmq")}
	if err := client.SendMsg(callCtx, gozmq.NewMsgFromFrames(want...)); err != nil {
		t.Fatalf("gozmq send: %v", err)
	}
	got, err := server.Recv()
	if err != nil {
		t.Fatalf("libzmq recv: %v", err)
	}
	assertFramesEqual(t, got, want)
}

func TestTCP6_LibzmqClient_GozmqServer(t *testing.T) {
	gCtx := newGozmqCtx(t)
	events := make(chan gozmq.Event, 16)
	server, err := gCtx.NewSocket(gozmq.Pair, gozmq.WithMonitor(events, gozmq.EventAll))
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Bind("tcp6://[::1]:0"); err != nil {
		t.Skipf("tcp6 not available: %v", err)
	}
	ep := server.LastEndpoint()
	if ep == "" {
		t.Fatal("gozmq LastEndpoint empty after bind")
	}

	libCtx := newLibCtx(t)
	client := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.SetOption(ZMQ_IPV6, 1); err != nil {
		t.Fatalf("libzmq IPV6: %v", err)
	}
	if err := client.SetOption(ZMQ_RCVTIMEO, 5000); err != nil {
		t.Fatalf("libzmq RCVTIMEO: %v", err)
	}
	if err := client.Connect(libzmqTCP6Endpoint(ep)); err != nil {
		t.Fatalf("libzmq connect: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	accepted := waitForEventType(waitCtx, events, gozmq.EventAccepted)
	handshake := waitHandshakeEvent(waitCtx, events)
	if handshake == nil || handshake.Type != gozmq.EventHandshakeSucceeded {
		t.Fatalf("accepted=%v handshake=%v, want handshake=%v", accepted, handshake, gozmq.EventHandshakeSucceeded)
	}

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()

	want := [][]byte{[]byte("tcp6-libzmq"), []byte("to-gozmq")}
	if err := client.Send(want); err != nil {
		t.Fatalf("libzmq send: %v", err)
	}
	msg, err := server.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), want)
}

func libzmqTCP6Endpoint(ep string) string {
	return strings.Replace(ep, "tcp6://", "tcp://", 1)
}
