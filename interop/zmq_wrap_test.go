//go:build interop

package interop

import (
	"bytes"
	"testing"
	"time"
)

// TestWrap_PairSmoke exercises the CGO wrapper end-to-end: two libzmq PAIR
// sockets in one process round-trip a multipart message over TCP. If this
// fails every other test in the package is suspect — the wrapper itself is
// broken.
func TestWrap_PairSmoke(t *testing.T) {
	ctx := NewCtx()
	t.Cleanup(func() { _ = ctx.Close() })

	server := ctx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("server bind: %v", err)
	}
	ep, err := server.LastEndpoint()
	if err != nil {
		t.Fatalf("server LastEndpoint: %v", err)
	}
	if ep == "" {
		t.Fatal("server LastEndpoint empty")
	}

	client := ctx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(ep); err != nil {
		t.Fatalf("client connect: %v", err)
	}

	// Give the PAIR peers a beat to complete the ZMTP handshake so the
	// first send is not silently queued against a pre-connect buffer. 200ms
	// is enough on a loopback; if this is ever flaky the right fix is to
	// use an XPUB/XSUB sync pair, but for PAIR-over-loopback it's overkill.
	time.Sleep(200 * time.Millisecond)

	frames := [][]byte{[]byte("hello"), []byte("world")}
	if err := client.Send(frames); err != nil {
		t.Fatalf("client send: %v", err)
	}
	got, err := server.Recv()
	if err != nil {
		t.Fatalf("server recv: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("server got %d frames, want 2", len(got))
	}
	if !bytes.Equal(got[0], frames[0]) || !bytes.Equal(got[1], frames[1]) {
		t.Fatalf("server got %q / %q, want hello / world", got[0], got[1])
	}

	// Reply.
	if err := server.Send([][]byte{[]byte("ack")}); err != nil {
		t.Fatalf("server send: %v", err)
	}
	got, err = client.Recv()
	if err != nil {
		t.Fatalf("client recv: %v", err)
	}
	if len(got) != 1 || !bytes.Equal(got[0], []byte("ack")) {
		t.Fatalf("client got %v, want [ack]", got)
	}
}

// TestWrap_SetOption_IntAndBytes covers the reflection-style option
// dispatcher for both int and []byte values.
func TestWrap_SetOption_IntAndBytes(t *testing.T) {
	ctx := NewCtx()
	t.Cleanup(func() { _ = ctx.Close() })
	s := ctx.NewSocket(ZMQ_SUB)
	t.Cleanup(func() { _ = s.Close() })

	if err := s.SetOption(ZMQ_LINGER, 100); err != nil {
		t.Fatalf("set LINGER: %v", err)
	}
	if err := s.SetOption(ZMQ_SUBSCRIBE, []byte("topic.")); err != nil {
		t.Fatalf("set SUBSCRIBE bytes: %v", err)
	}
	if err := s.Subscribe(nil); err != nil {
		t.Fatalf("Subscribe(nil): %v", err)
	}
	if err := s.SetOption(ZMQ_SUBSCRIBE, "another"); err != nil {
		t.Fatalf("set SUBSCRIBE string: %v", err)
	}
	if err := s.SetOption(999999, 1); err == nil {
		t.Fatal("set bogus option: want error, got nil")
	}
}
