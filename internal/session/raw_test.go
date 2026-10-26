package session

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/msg"
	"github.com/exergy-dev/gozmq/transport"
)

// rawSession is a raw-mode session and the engine end OnReady got.
type rawSession struct {
	*Session
	pipe     *pipe.End
	ready    chan struct{} // closed by OnReady
	attached bool          // as reported by OnClosed
}

// newRaw builds a raw session over conn without starting it, so a test can
// queue outbound messages first (see queue) and have the writer find them
// all on its first pull.
func newRaw(conn transport.Conn, hwm int) *rawSession {
	rs := &rawSession{ready: make(chan struct{})}
	rs.Session = New(Config{
		Conn: conn, Raw: true, SendHWM: hwm, RecvHWM: hwm,
		OnReady:  func(p *pipe.End) error { rs.pipe = p; close(rs.ready); return nil },
		OnClosed: func(_ error, attached bool) { rs.attached = attached },
	})
	return rs
}

// start starts rs, stopped at test end, and waits for OnReady, which raw
// mode calls without any handshake.
func (rs *rawSession) start(t *testing.T) {
	t.Helper()
	rs.Start()
	t.Cleanup(func() { _ = rs.Stop() })
	waitClosed(t, rs.ready, 2*time.Second, "OnReady not called")
}

// startRaw starts a raw session over conn (see start).
func startRaw(t *testing.T, conn transport.Conn, hwm int) *rawSession {
	t.Helper()
	rs := newRaw(conn, hwm)
	rs.start(t)
	return rs
}

// queue puts ms on the engine end without blocking (the session pipe was
// made in New, so this works before start too).
func (rs *rawSession) queue(t *testing.T, ms ...*msg.Msg) {
	t.Helper()
	for i, m := range ms {
		require.True(t, rs.enginePipe.TrySend(m), "queue %d refused", i)
	}
}

// rawOut is the outbound pipe message the STREAM engine hands down for body.
func rawOut(body string) *msg.Msg { return msg.FromBytes([]byte(body)) }

// rawHalfClose is the outbound STREAM half-close.
func rawHalfClose() *msg.Msg { return msg.FromBytes(nil) }

// rawLentOut is rawOut for a lent body, given back through put.
func rawLentOut(put func([]byte), body []byte) *msg.Msg {
	return msg.NewLent(put, [][]byte{body}, body)
}

// readAll collects everything the peer end receives until EOF.
func readAll(c io.Reader) <-chan []byte {
	return testutil.Async(func() []byte {
		b, _ := io.ReadAll(c)
		return b
	})
}

// TestRaw_InboundChunking: every byte the peer writes surfaces, in order,
// as single-frame messages (the OS decides the read boundaries).
func TestRaw_InboundChunking(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	rs := startRaw(t, connA, 0)
	payload := bytes.Repeat([]byte("HELLO WORLD\n"), 300)
	go func() {
		_, _ = connB.Write(payload)
		_ = connB.Close()
	}()

	ctx := testutil.Ctx(t, 2*time.Second)
	var got bytes.Buffer
	for got.Len() < len(payload) {
		m, err := rs.pipe.RecvOr(ctx, nil)
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
	t.Parallel()
	connA, connB := newFakeConnPair()
	defer connB.Close()
	rs := startRaw(t, connA, 0)

	ctx := testutil.Ctx(t, 2*time.Second)
	var chunks []*msg.Msg
	for _, b := range []string{"a", "b"} {
		_, err := connB.Write([]byte(b))
		require.NoError(t, err)
		m, err := rs.pipe.RecvOr(ctx, nil)
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
	t.Parallel()
	connA, connB := newFakeConnPair()
	defer connB.Close()
	rs := startRaw(t, connA, 0)

	const body = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
	require.NoError(t, rs.pipe.Send(testutil.Ctx(t, 2*time.Second), rawOut(body)))
	buf := make([]byte, len(body))
	_, err := io.ReadFull(connB, buf)
	require.NoError(t, err)
	require.Equal(t, body, string(buf))
}

// TestRaw_EmptyBodyCloses: an empty body is the STREAM half-close. Data
// queued ahead of it is written first, then the conn closes and the
// session ends, attached.
func TestRaw_EmptyBodyCloses(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	rs := startRaw(t, connA, 0)

	ctx := testutil.Ctx(t, 2*time.Second)
	require.NoError(t, rs.pipe.Send(ctx, rawOut("last-packet")))
	require.NoError(t, rs.pipe.Send(ctx, rawHalfClose()))

	got, err := io.ReadAll(connB)
	require.NoError(t, err)
	require.Equal(t, "last-packet", string(got))
	waitClosed(t, rs.Done(), 2*time.Second, "session did not end after empty body")
	require.True(t, rs.attached)
}

// TestRaw_PeerEOFClosesPipe: the peer closing its end closes the engine
// pipe (the engine's cue to detach) and ends the session.
func TestRaw_PeerEOFClosesPipe(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	rs := startRaw(t, connA, 0)
	_ = connB.Close()

	_, err := rs.pipe.RecvOr(testutil.Ctx(t, 2*time.Second), nil)
	require.ErrorIs(t, err, io.EOF)
	waitClosed(t, rs.Done(), 2*time.Second, "session did not end after peer EOF")
}

// TestRaw_DrainFlushesThenKeepsConn: Drain writes everything queued and
// leaves the conn open until Stop, as in ZMTP mode.
func TestRaw_DrainFlushesThenKeepsConn(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	defer connB.Close()
	rs := startRaw(t, connA, 0)

	const n = 50
	ctx := testutil.Ctx(t, 2*time.Second)
	var want bytes.Buffer
	for i := range n {
		b := []byte{byte('a' + i%26)}
		want.Write(b)
		require.NoError(t, rs.pipe.Send(ctx, rawOut(string(b))))
	}
	drained := rs.Drain()
	got := make([]byte, want.Len())
	_, err := io.ReadFull(connB, got) // io.Pipe: the writer waits on this read
	require.NoError(t, err)
	require.Equal(t, want.Bytes(), got)
	waitClosed(t, drained, 2*time.Second, "Drain did not finish")

	testutil.RequireNoRecv(t, connA.Closed(), 0, "Drain closed the conn")
	require.NoError(t, rs.Stop())
	waitClosed(t, connA.Closed(), time.Second, "Stop did not close the conn")
}

// TestRaw_HWMZeroUnlimited: HWM 0 queues without bound in both directions.
func TestRaw_HWMZeroUnlimited(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	defer connB.Close()
	rs := startRaw(t, connA, 0)
	// The writer blocks on the peer's first read, so everything after it
	// stays queued.
	for i := range 5000 {
		require.True(t, rs.pipe.TrySend(rawOut("x")), "send %d refused", i)
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
	t.Parallel()
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
	t.Parallel()
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
	_, err := rs.pipe.RecvOr(context.Background(), nil)
	require.ErrorIs(t, err, io.EOF)
}

// --- writer batching --------------------------------------------------------

// rawBodies returns n distinct size-byte outbound messages and the bytes
// they put on the wire.
func rawBodies(n, size int) ([]*msg.Msg, []byte) {
	var want bytes.Buffer
	ms := make([]*msg.Msg, n)
	for i := range ms {
		b := bytes.Repeat([]byte{byte('a' + i%26)}, size)
		want.Write(b)
		ms[i] = rawOut(string(b))
	}
	return ms, want.Bytes()
}

// TestRaw_WriterCoalescesQueued: small bodies already queued when the
// writer pulls go out in one WriteBuffers call, in order; once the batch's
// scratch has grown (its first fill may relocate it, starting a new iovec
// each time), such a burst is one iovec.
func TestRaw_WriterCoalescesQueued(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	defer connB.Close()
	wc := newWritevConn(connA)
	rs := newRaw(wc, 0)
	ms, want := rawBodies(100, 32)
	rs.queue(t, ms...)
	rs.start(t)
	got := make([]byte, len(want))
	_, err := io.ReadFull(connB, got)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Len(t, wc.BufCounts(), 1, "one write for the queued burst")

	// Warm: a stalled lone message, then the same burst queued behind it.
	gate := make(chan struct{})
	wc.setGate(gate)
	rs.queue(t, rawOut("!"))
	require.Eventually(t, func() bool { return wc.gateWaiting.Load() == 1 },
		2*time.Second, time.Millisecond, "writer never reached the write")
	ms, want = rawBodies(100, 32)
	rs.queue(t, ms...)
	read := testutil.Async(func() []byte {
		b := make([]byte, 1+len(want))
		_, _ = io.ReadFull(connB, b)
		return b
	})
	close(gate)
	require.Equal(t, append([]byte("!"), want...), testutil.RequireRecv(t, read, 2*time.Second, "peer read stalled"))
	require.Equal(t, []int{1, 1}, wc.BufCounts()[1:], "the lone message, then the burst as one iovec")
}

// TestRaw_WriterLargeBodyByReference: a body over coalesceBodyMax goes to
// the wire as its own iovec between the small runs around it.
func TestRaw_WriterLargeBodyByReference(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	defer connB.Close()
	wc := newWritevConn(connA)
	rs := newRaw(wc, 0)
	large := string(bytes.Repeat([]byte("L"), coalesceBodyMax+1))
	rs.queue(t, rawOut("head"), rawOut(large), rawOut("tail"))
	rs.start(t)

	want := "head" + large + "tail"
	got := make([]byte, len(want))
	_, err := io.ReadFull(connB, got)
	require.NoError(t, err)
	require.Equal(t, want, string(got))
	require.Equal(t, []int{3}, wc.BufCounts(), "small, large, small in one writev")
}

// TestRaw_WriterSingleMessageImmediate: a lone message is written at once,
// as one write; the writer never waits for company.
func TestRaw_WriterSingleMessageImmediate(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	defer connB.Close()
	wc := newWritevConn(connA)
	rs := startRaw(t, wc, 0)
	require.NoError(t, rs.pipe.Send(testutil.Ctx(t, 2*time.Second), rawOut("ping")))
	got := make([]byte, 4)
	_, err := io.ReadFull(connB, got)
	require.NoError(t, err)
	require.Equal(t, "ping", string(got))
	require.Equal(t, []int{1}, wc.BufCounts())
}

// TestRaw_HalfCloseEndsBatch: a half-close inside a coalesced batch flushes
// the bodies ahead of it, then closes the conn; what was queued after it is
// dropped and the session ends, attached.
func TestRaw_HalfCloseEndsBatch(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	wc := newWritevConn(connA)
	rs := newRaw(wc, 0)
	rs.queue(t, rawOut("one,"), rawOut("two"), rawHalfClose(), rawOut("after"))
	read := readAll(connB)
	rs.start(t)

	got := testutil.RequireRecv(t, read, 2*time.Second, "peer never saw EOF")
	require.Equal(t, "one,two", string(got))
	waitClosed(t, rs.Done(), 2*time.Second, "session did not end after half-close")
	waitClosed(t, connA.Closed(), time.Second, "conn not closed")
	require.True(t, rs.attached)
	require.Equal(t, []int{1}, wc.BufCounts(), "the two bodies in one write")
}

// TestRaw_HalfCloseFirstInBatch: a half-close at the head of the queue
// closes the conn with nothing written, even with data queued behind it.
func TestRaw_HalfCloseFirstInBatch(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	wc := newWritevConn(connA)
	rs := newRaw(wc, 0)
	rs.queue(t, rawHalfClose(), rawOut("dropped"))
	read := readAll(connB)
	rs.start(t)

	got := testutil.RequireRecv(t, read, 2*time.Second, "peer never saw EOF")
	require.Empty(t, got)
	waitClosed(t, rs.Done(), 2*time.Second, "session did not end after half-close")
	require.True(t, rs.attached)
	require.Empty(t, wc.BufCounts(), "nothing to write")
}

// TestRaw_LentReleasedAfterFlush: a lent body in a batch, copied or sent by
// reference, is given back only once the write carrying it has returned.
func TestRaw_LentReleasedAfterFlush(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	defer connB.Close()
	wc := newWritevConn(connA)
	gate := make(chan struct{})
	wc.setGate(gate)
	rs := newRaw(wc, 0)

	var puts atomic.Int32
	put := func([]byte) { puts.Add(1) }
	small := []byte("small lent body")
	large := bytes.Repeat([]byte("B"), 2*coalesceBodyMax) // by reference
	rs.queue(t, rawLentOut(put, small), rawLentOut(put, large))
	rs.start(t)

	require.Eventually(t, func() bool { return wc.gateWaiting.Load() == 1 },
		2*time.Second, time.Millisecond, "writer never reached the write")
	require.Zero(t, puts.Load(), "lent body released before its write")
	want := append(append([]byte(nil), small...), large...)
	read := testutil.Async(func() []byte {
		b := make([]byte, len(want))
		_, _ = io.ReadFull(connB, b)
		return b
	})
	close(gate)
	require.Equal(t, want, testutil.RequireRecv(t, read, 2*time.Second, "peer read stalled"))
	require.Eventually(t, func() bool { return puts.Load() == 2 },
		2*time.Second, time.Millisecond, "lent bodies not released after the flush")
	require.Equal(t, []int{2}, wc.BufCounts())
}

// TestRaw_PeerCloseMidBatch: the peer going away while a batch is being
// written ends the session and leaves no session goroutine behind.
func TestRaw_PeerCloseMidBatch(t *testing.T) {
	requireSessionGoroutines(t, 0, "baseline")
	connA, connB := newFakeConnPair()
	wc := newWritevConn(connA)
	gate := make(chan struct{})
	wc.setGate(gate)
	rs := newRaw(wc, 0)
	ms, _ := rawBodies(500, 64)
	rs.queue(t, ms...)
	rs.start(t)

	require.Eventually(t, func() bool { return wc.gateWaiting.Load() == 1 },
		2*time.Second, time.Millisecond, "writer never reached the write")
	_ = connB.Close()
	close(gate)
	waitClosed(t, rs.Done(), 2*time.Second, "session did not end after the peer closed")
	requireSessionGoroutines(t, 0, "after the peer closed")
}

// TestRaw_DrainCoalescesBurst: a burst queued behind a stalled write is
// drained as one more batch, every byte arrives, and the conn stays open.
func TestRaw_DrainCoalescesBurst(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	defer connB.Close()
	wc := newWritevConn(connA)
	gate := make(chan struct{})
	wc.setGate(gate)
	rs := startRaw(t, wc, 0)

	ms, want := rawBodies(201, 32)
	rs.queue(t, ms[0])
	require.Eventually(t, func() bool { return wc.gateWaiting.Load() == 1 },
		2*time.Second, time.Millisecond, "writer never reached the write")
	rs.queue(t, ms[1:]...)
	drained := rs.Drain()
	read := testutil.Async(func() []byte {
		b := make([]byte, len(want))
		_, _ = io.ReadFull(connB, b)
		return b
	})
	close(gate)
	require.Equal(t, want, testutil.RequireRecv(t, read, 2*time.Second, "peer read stalled"))
	waitClosed(t, drained, 2*time.Second, "Drain did not finish")
	require.Len(t, wc.BufCounts(), 2, "the stalled message, then the burst in one write")
	testutil.RequireNoRecv(t, connA.Closed(), 0, "Drain closed the conn")
}
