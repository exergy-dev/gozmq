//go:build interop

package interop

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
)

// newGozmqCtx returns a fresh gozmq.Context wired through t.Cleanup.
func newGozmqCtx(t *testing.T) *gozmq.Context {
	t.Helper()
	c := gozmq.NewContext()
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// newLibCtx returns a fresh libzmq Ctx wired through t.Cleanup. Separate
// from the gozmq one so leak assertions over one do not mask issues in
// the other.
func newLibCtx(t *testing.T) *Ctx {
	t.Helper()
	c := NewCtx()
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestPair_GozmqClient_LibzmqServer: libzmq PAIR binds, gozmq PAIR connects.
func TestPair_GozmqClient_LibzmqServer(t *testing.T) {
	libCtx := newLibCtx(t)
	server := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("libzmq bind: %v", err)
	}
	ep, err := server.LastEndpoint()
	if err != nil {
		t.Fatalf("LastEndpoint: %v", err)
	}

	gCtx := newGozmqCtx(t)
	client, err := gCtx.NewSocket(gozmq.Pair)
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(ep); err != nil {
		t.Fatalf("gozmq connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// gozmq → libzmq.
	want := [][]byte{[]byte("ping-a"), []byte("ping-b")}
	if err := client.SendMsg(callCtx, gozmq.NewMsgFromFrames(want...)); err != nil {
		t.Fatalf("gozmq send: %v", err)
	}
	got, err := server.Recv()
	if err != nil {
		t.Fatalf("libzmq recv: %v", err)
	}
	assertFramesEqual(t, got, want)

	// libzmq → gozmq.
	reply := [][]byte{[]byte("pong")}
	if err := server.Send(reply); err != nil {
		t.Fatalf("libzmq send: %v", err)
	}
	msg, err := client.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), reply)
}

// TestPair_LibzmqClient_GozmqServer: gozmq PAIR binds, libzmq PAIR connects.
func TestPair_LibzmqClient_GozmqServer(t *testing.T) {
	gCtx := newGozmqCtx(t)
	server, err := gCtx.NewSocket(gozmq.Pair)
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

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// libzmq → gozmq.
	want := [][]byte{[]byte("hello"), []byte(""), []byte("world")}
	if err := client.Send(want); err != nil {
		t.Fatalf("libzmq send: %v", err)
	}
	msg, err := server.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), want)

	// gozmq → libzmq.
	reply := [][]byte{[]byte("ack")}
	if err := server.SendMsg(callCtx, gozmq.NewMsgFromFrames(reply...)); err != nil {
		t.Fatalf("gozmq send: %v", err)
	}
	got, err := client.Recv()
	if err != nil {
		t.Fatalf("libzmq recv: %v", err)
	}
	assertFramesEqual(t, got, reply)
}

func assertFramesEqual(t *testing.T, got, want [][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("frame count: got %d, want %d", len(got), len(want))
	}
	for i := range got {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("frame %d: got %q, want %q", i, got[i], want[i])
		}
	}
}
