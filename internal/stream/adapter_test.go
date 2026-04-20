package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/testutil"
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
		Endpoint:    "tcp://peer-1",
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
		if err != nil {
			t.Fatalf("eng recv after %d bytes: %v", got.Len(), err)
		}
		if m.NumFrames() != 1 {
			t.Fatalf("inbound msg frames = %d, want 1", m.NumFrames())
		}
		got.Write(m.Bytes())
	}
	if !bytes.Equal(got.Bytes(), payload) {
		t.Fatalf("reassembled body mismatch (len got=%d want=%d)", got.Len(), len(payload))
	}

	if err := <-writerDone; err != nil {
		t.Fatalf("peer write: %v", err)
	}
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
	if err := eng.Send(ctx, msg.FromBytes([]byte(body))); err != nil {
		t.Fatalf("eng send: %v", err)
	}

	buf := make([]byte, len(body))
	if _, err := io.ReadFull(pc, buf); err != nil {
		t.Fatalf("peer read: %v", err)
	}
	if string(buf) != body {
		t.Fatalf("peer got %q, want %q", buf, body)
	}
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
	if err := eng.Send(ctx, msg.FromBytes([]byte("last-packet"))); err != nil {
		t.Fatalf("data send: %v", err)
	}
	if err := eng.Send(ctx, msg.FromBytes(nil)); err != nil {
		t.Fatalf("half-close send: %v", err)
	}

	// Peer must see "last-packet" then EOF.
	buf, err := io.ReadAll(pc)
	if err != nil {
		t.Fatalf("peer read: %v", err)
	}
	if !bytes.Equal(buf, []byte("last-packet")) {
		t.Fatalf("peer got %q, want %q", buf, "last-packet")
	}

	// Writer goroutine should have exited (conn closed). Close the
	// adapter explicitly so we surface any stashed err and wait on
	// goroutine exit.
	if err := ad.Close(); err != nil {
		t.Fatalf("adapter close: %v", err)
	}
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
	if !errors.Is(err, io.EOF) {
		t.Fatalf("eng recv after peer close = %v, want io.EOF", err)
	}

	// Closed() channel should also fire so the engine's AttachPipe
	// watcher can call DetachPipe.
	select {
	case <-eng.Closed():
	case <-time.After(time.Second):
		t.Fatal("engine pipe Closed() did not fire after peer disconnect")
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
	if err := ad.Close(); err != nil {
		t.Fatalf("adapter close: %v", err)
	}

	// After close, Recv on the engine pipe must return promptly.
	rctx, rcancel := context.WithTimeout(context.Background(), time.Second)
	defer rcancel()
	if _, err := eng.Recv(rctx); err == nil {
		t.Fatal("eng recv after close returned no error")
	}
}

// TestAdapter_TCPRoundTrip brings up two real *net.TCPConn endpoints,
// wraps each in an Adapter, and round-trips an HTTP-style payload to
// confirm the raw-bytes path works over a live socket (not just in-mem
// net.Pipe). This is the integration end of Wave-6 before the gozmq.go
// wiring catches the rest.
func TestAdapter_TCPRoundTrip(t *testing.T) {
	t.Parallel()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
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
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	srv := <-accepted
	if srv.err != nil {
		t.Fatalf("accept: %v", srv.err)
	}

	clientSide := &testConn{Conn: clientConn, local: clientConn.LocalAddr().String(), remote: clientConn.RemoteAddr().String()}
	serverSide := &testConn{Conn: srv.conn, local: srv.conn.LocalAddr().String(), remote: srv.conn.RemoteAddr().String()}

	clientAd := NewAdapter(context.Background(), clientSide, Config{Identity: []byte("client"), Endpoint: "tcp://" + addr})
	defer clientAd.Close()
	serverAd := NewAdapter(context.Background(), serverSide, Config{Identity: []byte("server"), Endpoint: "tcp://" + srv.conn.RemoteAddr().String()})
	defer serverAd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
	const response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"

	// Client writes the request via its engine pipe.
	if err := clientAd.EnginePipe().Send(ctx, msg.FromBytes([]byte(request))); err != nil {
		t.Fatalf("client send: %v", err)
	}

	// Server drains conn.Read chunks until it has the whole request.
	var gotReq bytes.Buffer
	for gotReq.Len() < len(request) {
		m, err := serverAd.EnginePipe().Recv(ctx)
		if err != nil {
			t.Fatalf("server recv: %v", err)
		}
		gotReq.Write(m.Bytes())
	}
	if gotReq.String() != request {
		t.Fatalf("server got req %q, want %q", gotReq.String(), request)
	}

	// Server writes the response.
	if err := serverAd.EnginePipe().Send(ctx, msg.FromBytes([]byte(response))); err != nil {
		t.Fatalf("server send: %v", err)
	}

	var gotResp bytes.Buffer
	for gotResp.Len() < len(response) {
		m, err := clientAd.EnginePipe().Recv(ctx)
		if err != nil {
			t.Fatalf("client recv: %v", err)
		}
		gotResp.Write(m.Bytes())
	}
	if gotResp.String() != response {
		t.Fatalf("client got resp %q, want %q", gotResp.String(), response)
	}
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
			if err := ad.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
		}()
	}
	wg.Wait()
}

// (unused helper placeholder to avoid test-time complaints during
// iterative edits; intentionally a no-op reference.)
var _ = fmt.Sprintf
