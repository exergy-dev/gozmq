package gozmq_test

import (
	"bytes"
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
	t.Parallel()
	zctx := gozmq.NewContext()
	defer zctx.Close()

	srv, err := zctx.NewSocket(gozmq.Stream)
	require.NoError(t, err, "new stream srv")
	defer srv.Close()

	require.NoError(t, srv.Bind("tcp://127.0.0.1:0"), "bind")
	addr := srv.LastEndpoint()

	// --- Raw TCP client (not gozmq) --------------------------------
	// Use a bare net.Dial to prove the STREAM socket talks to a non-
	// ZMQ peer — the core reason STREAM exists.
	cli, err := net.Dial("tcp", stripScheme(addr))
	require.NoError(t, err, "dial")
	defer cli.Close()

	ctx := testutil.Ctx(t)

	const request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
	_, err = cli.Write([]byte(request))
	require.NoError(t, err, "client write")

	// Server: drain bytes until the request is complete.
	var gotReq bytes.Buffer
	var peerID []byte
	for gotReq.Len() < len(request) {
		m, err := srv.RecvMsg(ctx)
		require.NoError(t, err, "srv recv")
		require.Equal(t, 2, m.NumFrames(), "recv frames: want identity + body")
		peerID = m.Frames()[0]
		gotReq.Write(m.Frames()[1])
	}
	require.Equal(t, request, gotReq.String(), "srv got")

	// Echo back.
	require.NoError(t, srv.SendMsg(ctx, gozmq.NewMsgFromFrames(peerID, []byte(request))), "srv send")

	buf := make([]byte, len(request))
	_, err = io.ReadFull(cli, buf)
	require.NoError(t, err, "client read")
	require.Equal(t, request, string(buf), "client got")

	// Half-close: identity-only Msg → the session closes the TCP conn.
	require.NoError(t, srv.SendMsg(ctx, gozmq.NewMsgFromFrames(peerID)), "srv half-close")
	// Reads on the client should now return EOF (possibly after the
	// echo flushes).
	_, err = io.ReadAll(cli)
	if err != nil {
		// net may return a "use of closed network connection" if we
		// already closed, but io.ReadAll translates EOF to nil err.
		require.ErrorIs(t, err, io.EOF, "client ReadAll after half-close")
	}
}

// TestSocketStream_TCP_Connect validates the client-side wiring: a
// Socket(Stream).Connect dials out to a plain TCP listener, and bytes
// written via SendMsg reach the peer verbatim.
func TestSocketStream_TCP_Connect(t *testing.T) {
	t.Parallel()
	// Listener that speaks plain TCP.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
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
	require.NoError(t, err, "new")
	defer cli.Close()

	require.NoError(t, cli.Connect("tcp://"+l.Addr().String()), "connect")

	a := <-accCh
	require.NoError(t, a.err, "accept")
	defer a.c.Close()

	ctx := testutil.Ctx(t)

	// Send via gozmq; read raw on the other side.
	//
	// The first Msg the app receives from this conn carries the
	// synthesized identity the engine assigned — we learn it by
	// prompting the peer to send something first.
	_, err = a.c.Write([]byte("hi from peer\n"))
	require.NoError(t, err, "peer write")

	m, err := cli.RecvMsg(ctx)
	require.NoError(t, err, "cli recv")
	peerID := m.Frames()[0]
	require.Equal(t, []byte("hi from peer\n"), m.Frames()[1], "cli body")

	require.NoError(t, cli.SendMsg(ctx, gozmq.NewMsgFromFrames(peerID, []byte("hi from gozmq\n"))), "cli send")

	buf := make([]byte, len("hi from gozmq\n"))
	_, err = io.ReadFull(a.c, buf)
	require.NoError(t, err, "peer read")
	require.Equal(t, "hi from gozmq\n", string(buf), "peer got")
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
	t.Parallel()
	srv, cli, id := streamServerPeer(t, gozmq.WithSendHWM(0))
	const n, size = 2048, 16 << 10 // 32 MiB
	chunk := bytes.Repeat([]byte("z"), size)
	ctx := testutil.Ctx(t)
	for i := 0; i < n; i++ {
		require.NoError(t, srv.SendMsg(ctx, gozmq.NewMsgFromFrames(id, chunk)), "send %d", i)
	}
	got, err := io.CopyN(io.Discard, cli, n*size)
	require.NoError(t, err)
	require.EqualValues(t, n*size, got)
}

// TestSocketStream_BurstThenHalfClose: a burst of small sends, which the
// writer coalesces, then an identity-only half-close: the peer reads
// exactly the burst's bytes, in order, then EOF.
func TestSocketStream_BurstThenHalfClose(t *testing.T) {
	t.Parallel()
	srv, cli, id := streamServerPeer(t)
	const n = 10000
	var want bytes.Buffer
	ctx := testutil.Ctx(t)
	read := testutil.Async(func() []byte {
		b, _ := io.ReadAll(cli) // until the half-close
		return b
	})
	for i := 0; i < n; i++ {
		body := []byte{byte('a' + i%26), byte('0' + i%10), '\n'}
		want.Write(body)
		require.NoError(t, srv.SendMsg(ctx, gozmq.NewMsgFromFrames(id, body)), "send %d", i)
	}
	require.NoError(t, srv.SendMsg(ctx, gozmq.NewMsgFromFrames(id)), "half-close")
	got := testutil.RequireRecv(t, read, 10*time.Second, "peer never saw the half-close")
	require.Equal(t, want.Len(), len(got))
	require.Equal(t, want.Bytes(), got)
}

// TestSocketStream_LargeAfterSmall: a 2 MiB send behind small ones (a
// body the writer sends by reference, past its batch byte cap) arrives
// intact and in order.
func TestSocketStream_LargeAfterSmall(t *testing.T) {
	t.Parallel()
	srv, cli, id := streamServerPeer(t)
	ctx := testutil.Ctx(t)
	var want bytes.Buffer
	for i := 0; i < 100; i++ {
		body := []byte{byte('a' + i%26)}
		want.Write(body)
		require.NoError(t, srv.SendMsg(ctx, gozmq.NewMsgFromFrames(id, body)))
	}
	large := make([]byte, 2<<20)
	for i := range large {
		large[i] = byte(i * 7)
	}
	want.Write(large)
	require.NoError(t, srv.SendMsg(ctx, gozmq.NewMsgFromFrames(id, large)))
	require.NoError(t, srv.SendMsg(ctx, gozmq.NewMsgFromFrames(id, []byte("end"))))
	want.WriteString("end")

	got := make([]byte, want.Len())
	require.NoError(t, cli.SetReadDeadline(time.Now().Add(10*time.Second)))
	_, err := io.ReadFull(cli, got)
	require.NoError(t, err)
	require.True(t, bytes.Equal(want.Bytes(), got), "wire bytes differ")
}

// TestSocketStream_LingerFlushes: Close drains what STREAM still has queued
// before closing the connection (the old adapter dropped it).
func TestSocketStream_LingerFlushes(t *testing.T) {
	t.Parallel()
	srv, cli, id := streamServerPeer(t, gozmq.WithLinger(10*time.Second))
	const n, size = 1024, 32 << 10 // 32 MiB, well past the kernel buffers
	read := testutil.Async(func() int64 {
		got, _ := io.Copy(io.Discard, cli) // until the server closes
		return got
	})
	chunk := bytes.Repeat([]byte("q"), size)
	ctx := testutil.Ctx(t)
	for i := 0; i < n; i++ {
		require.NoError(t, srv.SendMsg(ctx, gozmq.NewMsgFromFrames(id, chunk)))
	}
	require.NoError(t, srv.Close())
	got := testutil.RequireRecv(t, read, 10*time.Second, "peer never saw the connection close")
	require.EqualValues(t, n*size, got)
}

// TestSocketStream_Disconnect: Disconnect closes a STREAM Connect's
// connection, and a second Disconnect finds nothing.
func TestSocketStream_Disconnect(t *testing.T) {
	t.Parallel()
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
	ev := testutil.RequireRecv(t, mon, 5*time.Second, "no EventDisconnected")
	require.Equal(t, ep, ev.Endpoint)
	require.ErrorIs(t, cli.Disconnect(ep), gozmq.ErrEndpointUnknown)
}
