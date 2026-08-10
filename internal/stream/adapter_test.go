package stream

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/msg"
)

func TestMain(m *testing.M) { testutil.VerifyTestMain(m) }

// testConn wraps a net.Conn to satisfy iface.Conn. net.Pipe and
// net.TCPConn both satisfy net.Conn but not the ZMQ endpoint-string
// accessors iface.Conn requires, so we add a thin shim.
type testConn struct {
	net.Conn
	local  string
	remote string
}

func (c *testConn) LocalEndpoint() string  { return c.local }
func (c *testConn) RemoteEndpoint() string { return c.remote }

func newFakePair() (adapterSide iface.Conn, peerSide net.Conn) {
	a, b := net.Pipe()
	return &testConn{Conn: a, local: "mem://a", remote: "mem://b"}, b
}

// TestAdapter_InboundChunking writes N bytes to the peer end; the
// adapter's reader goroutine must surface exactly those bytes (though
// possibly across several 1-frame Msgs, since the OS decides Read
// boundaries). The test concatenates what the engine end sees and
// asserts equality.
func TestAdapter_InboundChunking(t *testing.T) {
	t.Parallel()
	ac, pc := newFakePair()

	ad := NewAdapter(context.Background(), ac, Config{
		Identity:    []byte("peer-1"),
		ReadBufSize: 4 * 1024,
	})
	defer ad.Close()
	eng := ad.EnginePipe()

	payload := bytes.Repeat([]byte("HELLO WORLD\n"), 300) // ~3.6 KiB — one or a few reads.

	writerDone := make(chan error, 1)
	go func() {
		_, err := pc.Write(payload)
		// Close our side after write so the reader eventually hits EOF
		// (drives the test to completion without relying on timeouts).
		_ = pc.Close()
		writerDone <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var got bytes.Buffer
	for got.Len() < len(payload) {
		m, err := eng.Recv(ctx)
		require.NoError(t, err, "eng recv after %d bytes", got.Len())
		require.Equal(t, 1, m.NumFrames(), "inbound msg frames")
		got.Write(m.Bytes())
	}
	require.Equal(t, payload, got.Bytes(), "reassembled body mismatch")

	require.NoError(t, <-writerDone, "peer write")
}

// TestAdapter_OutboundWrite pushes a Msg through the engine pipe; the
// writer must deliver the body bytes verbatim to the other conn end.
func TestAdapter_OutboundWrite(t *testing.T) {
	t.Parallel()
	ac, pc := newFakePair()

	ad := NewAdapter(context.Background(), ac, Config{})
	defer ad.Close()
	eng := ad.EnginePipe()

	const body = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"

	// engineEngine -> adapter: the streamEngine hands a single-frame
	// body Msg down the pipe; emulate that here.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte(body))), "eng send")

	buf := make([]byte, len(body))
	_, err := io.ReadFull(pc, buf)
	require.NoError(t, err, "peer read")
	require.Equal(t, body, string(buf), "peer body")
	_ = pc.Close()
}

// TestAdapter_HalfClose_EmptyBody asserts that an empty-body Msg pushed
// through the engine pipe causes the adapter to close the conn; the
// far side then sees EOF on subsequent Read.
func TestAdapter_HalfClose_EmptyBody(t *testing.T) {
	t.Parallel()
	ac, pc := newFakePair()

	ad := NewAdapter(context.Background(), ac, Config{})
	// Do NOT defer ad.Close() first — we want the half-close path to
	// drive shutdown. Still call it at the end to collect any error.
	eng := ad.EnginePipe()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Send non-empty data first so we can assert ordering: the empty
	// sentinel must NOT jump ahead of queued bytes.
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("last-packet"))), "data send")
	require.NoError(t, eng.Send(ctx, msg.FromBytes(nil)), "half-close send")

	// Peer must see "last-packet" then EOF.
	buf, err := io.ReadAll(pc)
	require.NoError(t, err, "peer read")
	require.Equal(t, []byte("last-packet"), buf, "peer body")

	// Writer goroutine should have exited (conn closed). Close the
	// adapter explicitly so we surface any stashed err and wait on
	// goroutine exit.
	require.NoError(t, ad.Close(), "adapter close")
}

// TestAdapter_ReaderEOF_ClosesInboundPipe asserts that closing the conn
// from the far side makes the adapter-side engine pipe surface io.EOF
// on Recv (the Socket's DetachPipe trigger).
func TestAdapter_ReaderEOF_ClosesInboundPipe(t *testing.T) {
	t.Parallel()
	ac, pc := newFakePair()

	ad := NewAdapter(context.Background(), ac, Config{})
	defer ad.Close()
	eng := ad.EnginePipe()

	// Close the peer end; reader should see EOF and close its pipe.
	_ = pc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := eng.Recv(ctx)
	require.ErrorIs(t, err, io.EOF, "eng recv after peer close")

	// Closed() channel should also fire so the engine's AttachPipe
	// watcher can call DetachPipe.
	select {
	case <-eng.Closed():
	case <-time.After(time.Second):
		require.Fail(t, "engine pipe Closed() did not fire after peer disconnect")
	}
}

// TestAdapter_CtxCancel_ExitsBothGoroutines asserts that cancelling the
// parent ctx tears the adapter down cleanly (goleak-friendly). The
// TestMain wrapper fails the test if any goroutine leaks past return.
func TestAdapter_CtxCancel_ExitsBothGoroutines(t *testing.T) {
	t.Parallel()
	ac, pc := newFakePair()
	defer pc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	ad := NewAdapter(ctx, ac, Config{})
	eng := ad.EnginePipe()

	// Cancel the parent ctx; Close drains the goroutines.
	cancel()
	require.NoError(t, ad.Close(), "adapter close")

	// After close, Recv on the engine pipe must return promptly.
	rctx, rcancel := context.WithTimeout(context.Background(), time.Second)
	defer rcancel()
	_, err := eng.Recv(rctx)
	require.Error(t, err, "eng recv after close should return an error")
}

// TestAdapter_TCPRoundTrip brings up two real *net.TCPConn endpoints,
// wraps each in an Adapter, and round-trips an HTTP-style payload to
// confirm the raw-bytes path works over a live socket, not just net.Pipe.
func TestAdapter_TCPRoundTrip(t *testing.T) {
	t.Parallel()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	defer l.Close()

	addr := l.Addr().String()

	type result struct {
		conn net.Conn
		err  error
	}
	accepted := make(chan result, 1)
	go func() {
		c, err := l.Accept()
		accepted <- result{c, err}
	}()

	clientConn, err := net.Dial("tcp", addr)
	require.NoError(t, err, "dial")
	srv := <-accepted
	require.NoError(t, srv.err, "accept")

	clientSide := &testConn{Conn: clientConn, local: clientConn.LocalAddr().String(), remote: clientConn.RemoteAddr().String()}
	serverSide := &testConn{Conn: srv.conn, local: srv.conn.LocalAddr().String(), remote: srv.conn.RemoteAddr().String()}

	clientAd := NewAdapter(context.Background(), clientSide, Config{Identity: []byte("client")})
	defer clientAd.Close()
	serverAd := NewAdapter(context.Background(), serverSide, Config{Identity: []byte("server")})
	defer serverAd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
	const response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"

	// Client writes the request via its engine pipe.
	require.NoError(t, clientAd.EnginePipe().Send(ctx, msg.FromBytes([]byte(request))), "client send")

	// Server drains conn.Read chunks until it has the whole request.
	var gotReq bytes.Buffer
	for gotReq.Len() < len(request) {
		m, err := serverAd.EnginePipe().Recv(ctx)
		require.NoError(t, err, "server recv")
		gotReq.Write(m.Bytes())
	}
	require.Equal(t, request, gotReq.String(), "server request body")

	// Server writes the response.
	require.NoError(t, serverAd.EnginePipe().Send(ctx, msg.FromBytes([]byte(response))), "server send")

	var gotResp bytes.Buffer
	for gotResp.Len() < len(response) {
		m, err := clientAd.EnginePipe().Recv(ctx)
		require.NoError(t, err, "client recv")
		gotResp.Write(m.Bytes())
	}
	require.Equal(t, response, gotResp.String(), "client response body")
}

// TestAdapter_ConcurrentCloseIdempotent smashes Close from multiple
// goroutines to verify the sync.Once guard and WaitGroup plumbing.
func TestAdapter_ConcurrentCloseIdempotent(t *testing.T) {
	t.Parallel()
	ac, pc := newFakePair()
	defer pc.Close()

	ad := NewAdapter(context.Background(), ac, Config{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, ad.Close(), "close")
		}()
	}
	wg.Wait()
}

// (unused helper placeholder to avoid test-time complaints during
// iterative edits; intentionally a no-op reference.)
var _ = assert.Equal
