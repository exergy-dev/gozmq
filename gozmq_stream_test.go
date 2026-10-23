package gozmq_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
)

// TestSocketStream_TCP_HTTPEcho validates the end-to-end STREAM wiring:
// Context.NewSocket(Stream) constructs a functional Socket, Bind + accept
// loop runs new TCP conns as raw-mode sessions, and Send/Recv
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

	ctx := testutil.Ctx(t)

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

	// Half-close: identity-only Msg → the session closes the TCP conn.
	if err := srv.SendMsg(ctx, gozmq.NewMsgFromFrames(peerID)); err != nil {
		t.Fatalf("srv half-close: %v", err)
	}
	// Reads on the client should now return EOF (possibly after the
	// echo flushes).
	_, err = io.ReadAll(cli)
	if err != nil && !errors.Is(err, io.EOF) {
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

	ctx := testutil.Ctx(t)

	// Send via gozmq; read raw on the other side.
	//
	// The first Msg the app receives from this conn carries the
	// synthesized identity the engine assigned — we learn it by
	// prompting the peer to send something first.
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

// streamServerPeer binds a STREAM socket with opts, dials it from a raw TCP
// client, and returns both plus the client's routing identity, learned from
// a first byte the client sends.
func streamServerPeer(t *testing.T, opts ...gozmq.Option) (*gozmq.Socket, net.Conn, []byte) {
	t.Helper()
	_, srv := newTestSocket(t, gozmq.Stream, opts...)
	require.NoError(t, srv.Bind("tcp://127.0.0.1:0"))
	cli, err := net.Dial("tcp", stripScheme(srv.LastEndpoint()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	_, err = cli.Write([]byte("x"))
	require.NoError(t, err)
	m, err := srv.RecvMsg(testutil.Ctx(t))
	require.NoError(t, err)
	return srv, cli, m.Frames()[0]
}

// TestSocketStream_SendHWMZeroUnlimited: STREAM honours WithSendHWM(0) as
// unlimited (the old adapter substituted 1024): with the peer not reading,
// far more than the kernel buffers can take still queues without blocking.
func TestSocketStream_SendHWMZeroUnlimited(t *testing.T) {
	srv, cli, id := streamServerPeer(t, gozmq.WithSendHWM(0))
	const n, size = 2048, 16 << 10 // 32 MiB
	chunk := bytes.Repeat([]byte("z"), size)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		require.NoError(t, srv.SendMsg(ctx, gozmq.NewMsgFromFrames(id, chunk)), "send %d", i)
	}
	got, err := io.CopyN(io.Discard, cli, n*size)
	require.NoError(t, err)
	require.EqualValues(t, n*size, got)
}

// TestSocketStream_LingerFlushes: Close drains what STREAM still has queued
// before closing the connection (the old adapter dropped it).
func TestSocketStream_LingerFlushes(t *testing.T) {
	srv, cli, id := streamServerPeer(t, gozmq.WithLinger(10*time.Second))
	const n, size = 1024, 32 << 10 // 32 MiB, well past the kernel buffers
	read := make(chan int64, 1)
	go func() {
		got, _ := io.Copy(io.Discard, cli) // until the server closes
		read <- got
	}()
	chunk := bytes.Repeat([]byte("q"), size)
	ctx := testutil.Ctx(t)
	for i := 0; i < n; i++ {
		require.NoError(t, srv.SendMsg(ctx, gozmq.NewMsgFromFrames(id, chunk)))
	}
	require.NoError(t, srv.Close())
	select {
	case got := <-read:
		require.EqualValues(t, n*size, got)
	case <-time.After(10 * time.Second):
		t.Fatal("peer never saw the connection close")
	}
}

// TestSocketStream_Disconnect: Disconnect closes a STREAM Connect's
// connection, and a second Disconnect finds nothing.
func TestSocketStream_Disconnect(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err == nil {
			accepted <- c
		}
	}()

	_, cli := newTestSocket(t, gozmq.Stream)
	mon := make(chan gozmq.Event, 16)
	require.NoError(t, cli.Monitor(mon, gozmq.EventDisconnected))
	ep := "tcp://" + l.Addr().String()
	require.NoError(t, cli.Connect(ep))
	peer := <-accepted
	defer peer.Close()

	require.NoError(t, cli.Disconnect(ep))
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "peer should see the connection close")
	select {
	case ev := <-mon:
		require.Equal(t, ep, ev.Endpoint)
	case <-time.After(5 * time.Second):
		t.Fatal("no EventDisconnected")
	}
	require.ErrorIs(t, cli.Disconnect(ep), gozmq.ErrEndpointUnknown)
}
