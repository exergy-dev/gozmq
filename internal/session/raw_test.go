package session

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/msg"
)

// rawSession is a started raw-mode session and the engine end OnReady got.
type rawSession struct {
	*Session
	pipe     *pipe.End
	attached bool // as reported by OnClosed
}

// startRaw starts a raw session over conn, stopped at test end, and waits
// for OnReady, which raw mode calls without any handshake.
func startRaw(t *testing.T, conn *fakeConn, hwm int) *rawSession {
	t.Helper()
	rs := &rawSession{}
	ready := make(chan struct{})
	rs.Session = New(Config{
		Conn: conn, Raw: true, SendHWM: hwm, RecvHWM: hwm,
		OnReady:  func(p *pipe.End) error { rs.pipe = p; close(ready); return nil },
		OnClosed: func(_ error, attached bool) { rs.attached = attached },
	})
	rs.Start()
	t.Cleanup(func() { _ = rs.Stop() })
	waitClosed(t, ready, 2*time.Second, "OnReady not called")
	return rs
}

// TestRaw_InboundChunking: every byte the peer writes surfaces, in order,
// as single-frame messages (the OS decides the read boundaries).
func TestRaw_InboundChunking(t *testing.T) {
	connA, connB := newFakeConnPair()
	rs := startRaw(t, connA, 0)
	payload := bytes.Repeat([]byte("HELLO WORLD\n"), 300)
	go func() {
		_, _ = connB.Write(payload)
		_ = connB.Close()
	}()

	ctx := testCtx(t, 2*time.Second)
	var got bytes.Buffer
	for got.Len() < len(payload) {
		m, err := rs.pipe.Recv(ctx)
		require.NoError(t, err, "recv after %d bytes", got.Len())
		require.Equal(t, 1, m.NumFrames())
		got.Write(m.Bytes())
	}
	require.Equal(t, payload, got.Bytes())
}

// TestRaw_InboundChunkSized: a chunk owns only the bytes read, so a queued
// 1-byte read does not pin the 32 KiB read buffer, and chunks do not alias
// a reused buffer.
func TestRaw_InboundChunkSized(t *testing.T) {
	connA, connB := newFakeConnPair()
	defer connB.Close()
	rs := startRaw(t, connA, 0)

	ctx := testCtx(t, 2*time.Second)
	var chunks []*msg.Msg
	for _, b := range []string{"a", "b"} {
		_, err := connB.Write([]byte(b))
		require.NoError(t, err)
		m, err := rs.pipe.Recv(ctx)
		require.NoError(t, err)
		chunks = append(chunks, m)
	}
	for i, want := range []string{"a", "b"} {
		require.Equal(t, want, string(chunks[i].Bytes()), "chunk %d", i)
		require.Less(t, cap(chunks[i].Bytes()), 64, "chunk %d pins its read buffer", i)
	}
}

// TestRaw_OutboundVerbatim: a message's body reaches the peer as is, with
// no greeting or framing ahead of it.
func TestRaw_OutboundVerbatim(t *testing.T) {
	connA, connB := newFakeConnPair()
	defer connB.Close()
	rs := startRaw(t, connA, 0)

	const body = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
	require.NoError(t, rs.pipe.Send(testCtx(t, 2*time.Second), msg.FromBytes([]byte(body))))
	buf := make([]byte, len(body))
	_, err := io.ReadFull(connB, buf)
	require.NoError(t, err)
	require.Equal(t, body, string(buf))
}

// TestRaw_EmptyBodyCloses: an empty body is the STREAM half-close. Data
// queued ahead of it is written first, then the conn closes and the
// session ends, attached.
func TestRaw_EmptyBodyCloses(t *testing.T) {
	connA, connB := newFakeConnPair()
	rs := startRaw(t, connA, 0)

	ctx := testCtx(t, 2*time.Second)
	require.NoError(t, rs.pipe.Send(ctx, msg.FromBytes([]byte("last-packet"))))
	require.NoError(t, rs.pipe.Send(ctx, msg.FromBytes(nil)))

	got, err := io.ReadAll(connB)
	require.NoError(t, err)
	require.Equal(t, "last-packet", string(got))
	waitClosed(t, rs.Done(), 2*time.Second, "session did not end after empty body")
	require.True(t, rs.attached)
}

// TestRaw_PeerEOFClosesPipe: the peer closing its end closes the engine
// pipe (the engine's cue to detach) and ends the session.
func TestRaw_PeerEOFClosesPipe(t *testing.T) {
	connA, connB := newFakeConnPair()
	rs := startRaw(t, connA, 0)
	_ = connB.Close()

	_, err := rs.pipe.Recv(testCtx(t, 2*time.Second))
	require.ErrorIs(t, err, io.EOF)
	waitClosed(t, rs.Done(), 2*time.Second, "session did not end after peer EOF")
}

// TestRaw_DrainFlushesThenKeepsConn: Drain writes everything queued and
// leaves the conn open until Stop, as in ZMTP mode.
func TestRaw_DrainFlushesThenKeepsConn(t *testing.T) {
	connA, connB := newFakeConnPair()
	defer connB.Close()
	rs := startRaw(t, connA, 0)

	const n = 50
	ctx := testCtx(t, 2*time.Second)
	var want bytes.Buffer
	for i := range n {
		b := []byte{byte('a' + i%26)}
		want.Write(b)
		require.NoError(t, rs.pipe.Send(ctx, msg.FromBytes(b)))
	}
	drained := rs.Drain()
	got := make([]byte, want.Len())
	_, err := io.ReadFull(connB, got) // io.Pipe: the writer waits on this read
	require.NoError(t, err)
	require.Equal(t, want.Bytes(), got)
	waitClosed(t, drained, 2*time.Second, "Drain did not finish")

	select {
	case <-connA.Closed():
		t.Fatal("Drain closed the conn")
	default:
	}
	require.NoError(t, rs.Stop())
	waitClosed(t, connA.Closed(), time.Second, "Stop did not close the conn")
}

// TestRaw_HWMZeroUnlimited: HWM 0 queues without bound in both directions.
func TestRaw_HWMZeroUnlimited(t *testing.T) {
	connA, connB := newFakeConnPair()
	defer connB.Close()
	rs := startRaw(t, connA, 0)
	// The writer blocks on the peer's first read, so everything after it
	// stays queued.
	for i := range 5000 {
		require.True(t, rs.pipe.TrySend(msg.FromBytes([]byte{1})), "send %d refused", i)
	}
}

// TestRaw_TwoGoroutines: raw mode keeps the session at a reader and a
// writer.
func TestRaw_TwoGoroutines(t *testing.T) {
	requireSessionGoroutines(t, 0, "baseline")
	connA, connB := newFakeConnPair()
	rs := startRaw(t, connA, 0)
	requireSessionGoroutines(t, 2, "raw session")
	require.NoError(t, rs.Stop())
	_ = connB.Close()
	requireSessionGoroutines(t, 0, "after Stop")
}

// TestRaw_OnReadyErrorTearsDown: a refused pipe ends the session unattached
// and closes the conn.
func TestRaw_OnReadyErrorTearsDown(t *testing.T) {
	connA, connB := newFakeConnPair()
	defer connB.Close()
	attached := true
	s := New(Config{
		Conn: connA, Raw: true,
		OnReady:  func(*pipe.End) error { return errors.New("refused") },
		OnClosed: func(_ error, a bool) { attached = a },
	})
	s.Start()
	waitClosed(t, s.Done(), 2*time.Second, "session did not end")
	require.False(t, attached)
	waitClosed(t, connA.Closed(), time.Second, "conn not closed")
}

// TestRaw_ConcurrentStop: Stop is idempotent under concurrency.
func TestRaw_ConcurrentStop(t *testing.T) {
	connA, connB := newFakeConnPair()
	defer connB.Close()
	rs := startRaw(t, connA, 0)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, rs.Stop())
		}()
	}
	wg.Wait()
	_, err := rs.pipe.Recv(context.Background())
	require.ErrorIs(t, err, io.EOF)
}
