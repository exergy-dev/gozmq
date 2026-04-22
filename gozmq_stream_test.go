package gozmq_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/rkm/gozmq"
)

// TestSocketStream_TCP_HTTPEcho validates the end-to-end STREAM wiring:
// Context.NewSocket(Stream) constructs a functional Socket, Bind + accept
// loop routes new TCP conns through internal/stream.Adapter, and Send/Recv
// carry raw bytes between the app and a non-ZMQ TCP peer. A matching
// STREAM client dials out via a second Socket.
func TestSocketStream_TCP_HTTPEcho(t *testing.T) {
	zctx := gozmq.NewContext()
	defer zctx.Close()

	srv, err := zctx.NewSocket(gozmq.Stream)
	if err != nil {
		t.Fatalf("new stream srv: %v", err)
	}
	defer srv.Close()

	if err := srv.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	addr := srv.LastEndpoint()

	// --- Raw TCP client (not gozmq) --------------------------------
	// Use a bare net.Dial to prove the STREAM socket talks to a non-
	// ZMQ peer — the core reason STREAM exists.
	cli, err := net.Dial("tcp", stripScheme(addr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
	if _, err := cli.Write([]byte(request)); err != nil {
		t.Fatalf("client write: %v", err)
	}

	// Server: drain bytes until the request is complete.
	var gotReq bytes.Buffer
	var peerID []byte
	for gotReq.Len() < len(request) {
		m, err := srv.RecvMsg(ctx)
		if err != nil {
			t.Fatalf("srv recv: %v", err)
		}
		if m.NumFrames() != 2 {
			t.Fatalf("recv frames = %d, want 2 (identity + body)", m.NumFrames())
		}
		peerID = m.Frames()[0]
		gotReq.Write(m.Frames()[1])
	}
	if gotReq.String() != request {
		t.Fatalf("srv got %q, want %q", gotReq.String(), request)
	}

	// Echo back.
	if err := srv.SendMsg(ctx, gozmq.NewMsgFromFrames(peerID, []byte(request))); err != nil {
		t.Fatalf("srv send: %v", err)
	}

	buf := make([]byte, len(request))
	if _, err := io.ReadFull(cli, buf); err != nil {
		t.Fatalf("client read: %v", err)
	}
	if string(buf) != request {
		t.Fatalf("client got %q, want %q", buf, request)
	}

	// Half-close: identity-only Msg → adapter closes the TCP conn.
	if err := srv.SendMsg(ctx, gozmq.NewMsgFromFrames(peerID)); err != nil {
		t.Fatalf("srv half-close: %v", err)
	}
	// Reads on the client should now return EOF (possibly after the
	// echo flushes).
	_, err = io.ReadAll(cli)
	if err != nil && err != io.EOF {
		// net may return a "use of closed network connection" if we
		// already closed, but io.ReadAll translates EOF to nil err.
		t.Fatalf("client ReadAll after half-close: %v", err)
	}
}

// TestSocketStream_TCP_Connect validates the client-side wiring: a
// Socket(Stream).Connect dials out to a plain TCP listener, and bytes
// written via SendMsg reach the peer verbatim.
func TestSocketStream_TCP_Connect(t *testing.T) {
	// Listener that speaks plain TCP.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	type accepted struct {
		c   net.Conn
		err error
	}
	accCh := make(chan accepted, 1)
	go func() {
		c, err := l.Accept()
		accCh <- accepted{c, err}
	}()

	zctx := gozmq.NewContext()
	defer zctx.Close()

	cli, err := zctx.NewSocket(gozmq.Stream)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer cli.Close()

	if err := cli.Connect("tcp://" + l.Addr().String()); err != nil {
		t.Fatalf("connect: %v", err)
	}

	a := <-accCh
	if a.err != nil {
		t.Fatalf("accept: %v", a.err)
	}
	defer a.c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send via gozmq; read raw on the other side.
	//
	// On the client side the adapter attaches the pipe synchronously
	// in connectStream, so we can Send immediately. The first Msg the
	// app receives from this conn carries the synthesized identity
	// the engine assigned — we learn it by prompting the peer to send
	// something first.
	if _, err := a.c.Write([]byte("hi from peer\n")); err != nil {
		t.Fatalf("peer write: %v", err)
	}

	m, err := cli.RecvMsg(ctx)
	if err != nil {
		t.Fatalf("cli recv: %v", err)
	}
	peerID := m.Frames()[0]
	if !bytes.Equal(m.Frames()[1], []byte("hi from peer\n")) {
		t.Fatalf("cli body = %q", m.Frames()[1])
	}

	if err := cli.SendMsg(ctx, gozmq.NewMsgFromFrames(peerID, []byte("hi from gozmq\n"))); err != nil {
		t.Fatalf("cli send: %v", err)
	}

	buf := make([]byte, len("hi from gozmq\n"))
	if _, err := io.ReadFull(a.c, buf); err != nil {
		t.Fatalf("peer read: %v", err)
	}
	if string(buf) != "hi from gozmq\n" {
		t.Fatalf("peer got %q", buf)
	}
}

// stripScheme removes a leading "tcp://" if present (LastEndpoint
// returns the ZMQ form, net.Dial wants just "host:port").
func stripScheme(ep string) string {
	const p = "tcp://"
	if len(ep) >= len(p) && ep[:len(p)] == p {
		return ep[len(p):]
	}
	return ep
}
