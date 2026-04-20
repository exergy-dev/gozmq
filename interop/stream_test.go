//go:build interop

package interop

import (
	"context"
	"testing"
	"time"

	"github.com/rkm/gozmq"
)

func TestStream_LibzmqClient_GozmqServer(t *testing.T) {
	gCtx := newGozmqCtx(t)
	server, err := gCtx.NewSocket(gozmq.Stream)
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
	client := libCtx.NewSocket(ZMQ_STREAM)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.SetOption(ZMQ_RCVTIMEO, 5000); err != nil {
		t.Fatalf("libzmq RCVTIMEO: %v", err)
	}
	if err := client.Connect(ep); err != nil {
		t.Fatalf("libzmq connect: %v", err)
	}

	connected, err := client.Recv()
	if err != nil {
		t.Fatalf("libzmq connect notification: %v", err)
	}
	if len(connected) != 2 || len(connected[1]) != 0 {
		t.Fatalf("libzmq connect notification = %v, want [identity empty]", connected)
	}
	peerID := connected[0]

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Send([][]byte{peerID, []byte("ping-from-libzmq")}); err != nil {
		t.Fatalf("libzmq send: %v", err)
	}
	msg, err := server.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv: %v", err)
	}
	if msg.NumFrames() != 2 {
		t.Fatalf("gozmq recv frames = %d, want 2", msg.NumFrames())
	}
	if string(msg.Frames()[1]) != "ping-from-libzmq" {
		t.Fatalf("gozmq recv body = %q", msg.Frames()[1])
	}

	if err := server.SendMsg(callCtx, gozmq.NewMsgFromFrames(msg.Frames()[0], []byte("pong-from-gozmq"))); err != nil {
		t.Fatalf("gozmq send: %v", err)
	}
	reply, err := client.Recv()
	if err != nil {
		t.Fatalf("libzmq recv reply: %v", err)
	}
	assertFramesEqual(t, reply, [][]byte{peerID, []byte("pong-from-gozmq")})
}

func TestStream_GozmqClient_LibzmqServer(t *testing.T) {
	libCtx := newLibCtx(t)
	server := libCtx.NewSocket(ZMQ_STREAM)
	t.Cleanup(func() { _ = server.Close() })
	if err := server.SetOption(ZMQ_RCVTIMEO, 5000); err != nil {
		t.Fatalf("libzmq RCVTIMEO: %v", err)
	}
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("libzmq bind: %v", err)
	}
	ep, err := server.LastEndpoint()
	if err != nil {
		t.Fatalf("LastEndpoint: %v", err)
	}

	gCtx := newGozmqCtx(t)
	client, err := gCtx.NewSocket(gozmq.Stream)
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(ep); err != nil {
		t.Fatalf("gozmq connect: %v", err)
	}

	connected, err := server.Recv()
	if err != nil {
		t.Fatalf("libzmq connect notification: %v", err)
	}
	if len(connected) != 2 || len(connected[1]) != 0 {
		t.Fatalf("libzmq connect notification = %v, want [identity empty]", connected)
	}
	peerID := connected[0]

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Send([][]byte{peerID, []byte("ping-from-libzmq")}); err != nil {
		t.Fatalf("libzmq send: %v", err)
	}
	msg, err := client.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv: %v", err)
	}
	if msg.NumFrames() != 2 {
		t.Fatalf("gozmq recv frames = %d, want 2", msg.NumFrames())
	}
	if string(msg.Frames()[1]) != "ping-from-libzmq" {
		t.Fatalf("gozmq recv body = %q", msg.Frames()[1])
	}

	if err := client.SendMsg(callCtx, gozmq.NewMsgFromFrames(msg.Frames()[0], []byte("pong-from-gozmq"))); err != nil {
		t.Fatalf("gozmq send: %v", err)
	}
	reply, err := server.Recv()
	if err != nil {
		t.Fatalf("libzmq recv reply: %v", err)
	}
	assertFramesEqual(t, reply, [][]byte{peerID, []byte("pong-from-gozmq")})
}
