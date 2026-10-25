package session

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/msg"
	"github.com/exergy-dev/gozmq/security"
)

// countingConn wraps a fakeConn and records every Write call: count,
// per-call byte lengths, and an accumulated byte buffer of the full
// stream. It is the instrumented receiver used by writer-batching tests.

type countingConn struct {
	*fakeConn

	mu         sync.Mutex
	writeCalls int32
	writeSizes []int
	accum      []byte
}

func newCountingConn(fc *fakeConn) *countingConn {
	return &countingConn{fakeConn: fc}
}

func (c *countingConn) Write(p []byte) (int, error) {
	atomic.AddInt32(&c.writeCalls, 1)
	c.mu.Lock()
	c.writeSizes = append(c.writeSizes, len(p))
	c.accum = append(c.accum, p...)
	c.mu.Unlock()
	return c.fakeConn.Write(p)
}

func (c *countingConn) WriteCount() int {
	return int(atomic.LoadInt32(&c.writeCalls))
}

func (c *countingConn) Sizes() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int, len(c.writeSizes))
	copy(out, c.writeSizes)
	return out
}

// writevConn extends countingConn with WriteBuffers — the bufferWriter
// fast path flushBatch prefers — emulating writev(2): every buffer in a
// batch lands in ONE Write call on the underlying countingConn, making
// batch coalescing observable on a fake conn (net.Buffers.WriteTo only
// uses writev on real TCP/Unix conns).
//
// The optional gate stalls WriteBuffers. Because only traffic-phase
// flushes go through WriteBuffers (greeting/handshake writes use plain
// Write), the gate exerts wire backpressure without wedging the
// handshake — the phase distinction the old Write-level gate lacked.

type writevConn struct {
	*countingConn

	gate atomic.Pointer[chan struct{}]
	// gateWaiting counts WriteBuffers calls currently parked on the
	// gate, so tests can wait until the writer is provably stalled.
	gateWaiting atomic.Int32

	// bufCounts records len(*bufs) of every WriteBuffers call.
	bufMu     sync.Mutex
	bufCounts []int
}

// BufCounts returns the iovec count of each WriteBuffers call so far.
func (c *writevConn) BufCounts() []int {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()
	return append([]int(nil), c.bufCounts...)
}

func newWritevConn(fc *fakeConn) *writevConn {
	return &writevConn{countingConn: newCountingConn(fc)}
}

// setGate installs g as the blocking gate for future WriteBuffers calls,
// or clears the gate when g is nil.
func (c *writevConn) setGate(g chan struct{}) {
	if g == nil {
		c.gate.Store(nil)
		return
	}
	c.gate.Store(&g)
}

func (c *writevConn) WriteBuffers(bufs *net.Buffers) (int64, error) {
	if gp := c.gate.Load(); gp != nil {
		c.gateWaiting.Add(1)
		<-*gp
		c.gateWaiting.Add(-1)
	}
	c.bufMu.Lock()
	c.bufCounts = append(c.bufCounts, len(*bufs))
	c.bufMu.Unlock()
	var all []byte
	for _, b := range *bufs {
		all = append(all, b...)
	}
	*bufs = (*bufs)[:0] // consumed, mirroring net.Buffers.WriteTo
	n, err := c.Write(all)
	return int64(n), err
}

// newWriterTestPair builds two sessions A→B over a countingConn on the
// A side and plain fakeConn on the B side, runs both handshakes, and
// returns (sA, sB, countingConn). B drains via a goroutine that just
// calls Recv so the pipe doesn't stall.

func newWriterTestPair(t *testing.T, sendHWM, recvHWM int) (sA, sB *tSession, ccA *countingConn) {
	t.Helper()
	connA, connB := newFakeConnPair()
	ccA = newCountingConn(connA)
	sA, sB = startPair(t,
		Config{Conn: ccA, Mech: newFakeMech("NULL", false, map[string]string{"Identity": "A"}), SendHWM: sendHWM, RecvHWM: recvHWM},
		Config{Conn: connB, Mech: newFakeMech("NULL", true, map[string]string{"Identity": "B"}), SendHWM: sendHWM, RecvHWM: recvHWM},
	)
	return sA, sB, ccA
}

// newWritevTestPair is newWriterTestPair with a writev-emulating conn on
// the A side, for tests that observe batch coalescing or gate the wire.
func newWritevTestPair(t *testing.T, sendHWM, recvHWM int) (sA, sB *tSession, wcA *writevConn) {
	t.Helper()
	connA, connB := newFakeConnPair()
	wcA = newWritevConn(connA)
	sA, sB = startPair(t,
		Config{Conn: wcA, Mech: newFakeMech("NULL", false, map[string]string{"Identity": "A"}), SendHWM: sendHWM, RecvHWM: recvHWM},
		Config{Conn: connB, Mech: newFakeMech("NULL", true, map[string]string{"Identity": "B"}), SendHWM: sendHWM, RecvHWM: recvHWM},
	)
	return sA, sB, wcA
}

// resetWriteCounters clears the counting state AFTER the handshake has
// settled, so tests only measure traffic-phase writes.
func (c *countingConn) resetWriteCounters() {
	atomic.StoreInt32(&c.writeCalls, 0)
	c.mu.Lock()
	c.writeSizes = c.writeSizes[:0]
	c.accum = c.accum[:0]
	c.mu.Unlock()
}

// TestWriter_BatchRetainsBufferCapacityAcrossFlushes: net.Buffers.WriteTo
// and the tcpConn WriteBuffers fast path CONSUME the slice header they
// are handed (*v = (*v)[i:]), so flushBatch must be given a COPY of the
// batch's buffers header. Regression check: the batch's backing array
// must survive flushes — before the fix, release() truncated the
// consumed (empty) tail and every batch re-grew from zero capacity.

// consumeWritevConn emulates the consume semantics of net.Buffers.WriteTo
// faithfully: the passed slice header is advanced past everything written.
type consumeWritevConn struct{ *fakeConn }

func (c *consumeWritevConn) WriteBuffers(bufs *net.Buffers) (int64, error) {
	var n int64
	for _, b := range *bufs {
		n += int64(len(b))
	}
	*bufs = (*bufs)[len(*bufs):]
	return n, nil
}

func TestWriter_BatchRetainsBufferCapacityAcrossFlushes(t *testing.T) {
	t.Parallel()
	connA, _ := newFakeConnPair()
	// Never started: the test drives writeBatchFrom directly.
	s := New(Config{
		Conn:  &consumeWritevConn{fakeConn: connA},
		Mech:  appendMech{newFakeMech("NULL", false, nil)},
		Codec: fakeCodec{},
	})
	t.Cleanup(func() { _ = connA.Close() })
	sp, ep := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	s.sessionPipe = sp
	_ = ep

	var batch writerBatch
	require.NoError(t, s.writeBatchFrom(&batch, msg.FromBytes([]byte("first"))), "flush[0]")
	capAfterFirst := cap(batch.buffers)
	require.Greater(t, capAfterFirst, 0,
		"batch.buffers lost its backing array after a flush (consumed slice header leaked into the batch)")
	for i := 1; i <= 4; i++ {
		require.NoError(t, s.writeBatchFrom(&batch, msg.FromBytes([]byte("again"))), "flush[%d]", i)
		require.GreaterOrEqual(t, cap(batch.buffers), capAfterFirst,
			"batch.buffers capacity eroded on flush %d", i)
	}
	// Headers (and, for an appending mech like this one, bodies) go into
	// scratch; that capacity is kept too...
	require.Positive(t, cap(batch.scratch), "scratch not reused")
	// ...unless one oversized message grew it past the retention cap.
	big := make([]byte, maxRetainedScratch)
	require.NoError(t, s.writeBatchFrom(&batch, msg.FromBytes(big)), "flush(big)")
	require.Zero(t, cap(batch.scratch), "oversized scratch retained after flush")
}

// TestWriter_BatchCoalescesMultipleMessages: enqueue many messages
// rapidly; verify the writer coalesces them into fewer writev calls than
// messages via net.Buffers.
//
// Timing-deterministic via the gated writev conn: stall the writer's
// first traffic-phase flush, queue all N messages into the sessionPipe
// while it is parked, then unblock the gate — at most one more
// writer-loop iteration coalesces everything still queued.

func TestWriter_BatchCoalescesMultipleMessages(t *testing.T) {
	t.Parallel()
	const HWM = 64
	sA, sB, wcA := newWritevTestPair(t, HWM, HWM)

	gate := make(chan struct{})
	wcA.setGate(gate)
	wcA.resetWriteCounters()

	const N = 5
	ctx := testutil.Ctx(t, 2*time.Second)
	// First send: the writer pulls it and parks in the gated writev.
	require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte("msg-0"))), "Send[0]")
	require.Eventually(t, func() bool { return wcA.gateWaiting.Load() == 1 },
		2*time.Second, time.Millisecond, "writer did not park in the gated writev")
	// Queue the rest while the wire is stalled.
	for i := 1; i < N; i++ {
		m := msg.FromBytes([]byte(fmt.Sprintf("msg-%d", i)))
		require.NoError(t, sA.Pipe().Send(ctx, m), "Send[%d]", i)
	}

	// Release the gate: the pending flush proceeds, and the next loop
	// iteration coalesces all still-queued messages into one writev.
	wcA.setGate(nil)
	close(gate)

	// Drain on B, asserting order survived the batching.
	for i := 0; i < N; i++ {
		m, err := sB.Pipe().Recv(ctx)
		require.NoError(t, err, "Recv[%d]", i)
		assert.Equal(t, fmt.Sprintf("msg-%d", i), string(m.Bytes()), "got[%d]", i)
	}

	// Coalescing: 1 writev for the parked message + 1 for the batched
	// remainder. Anything < N proves coalescing; ≤ 2 proves the batch
	// picked up everything queued during the stall.
	writes := wcA.WriteCount()
	assert.GreaterOrEqual(t, writes, 1, "writer issued 0 writev calls; expected ≥1")
	assert.LessOrEqual(t, writes, 2, "writer issued %d writev calls for %d msgs; expected ≤2", writes, N)
	t.Logf("batched %d messages into %d writev calls (sizes=%v)", N, writes, wcA.Sizes())
}

// TestWriter_InteractiveSingleMessage: a solo message should be written
// immediately, NOT wait for a batch to fill. We assert latency is low
// AND the message is received promptly.

func TestWriter_InteractiveSingleMessage(t *testing.T) {
	t.Parallel()
	sA, sB, ccA := newWriterTestPair(t, 4, 4)
	defer func() { _ = sA.Stop(); _ = sB.Stop() }()

	time.Sleep(20 * time.Millisecond)
	ccA.resetWriteCounters()

	ctx := testutil.Ctx(t, time.Second)

	start := time.Now()
	require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte("solo"))), "Send")
	got, err := sB.Pipe().Recv(ctx)
	require.NoError(t, err, "Recv")
	elapsed := time.Since(start)

	assert.Equal(t, "solo", string(got.Bytes()))
	// Interactivity: no coalescing wait. On a loopback fakeConn this
	// round-trip should be well under 100ms even on a loaded CI.
	assert.LessOrEqual(t, elapsed, 200*time.Millisecond, "solo round-trip expected <200ms (no coalescing delay)")
}

// TestWriter_OrderPreservation: enqueue 20 messages with sequential
// payloads; capture bytes at the wire; decode frames; assert original
// order.

func TestWriter_OrderPreservation(t *testing.T) {
	t.Parallel()
	sA, sB, _ := newWriterTestPair(t, 64, 64)
	defer func() { _ = sA.Stop(); _ = sB.Stop() }()

	const N = 20
	ctx := testutil.Ctx(t, 2*time.Second)

	// Producer
	go func() {
		for i := 0; i < N; i++ {
			m := msg.FromBytes([]byte(fmt.Sprintf("seq-%03d", i)))
			if err := sA.Pipe().Send(ctx, m); err != nil {
				return
			}
		}
	}()

	// Consumer — must see the exact send order.
	for i := 0; i < N; i++ {
		m, err := sB.Pipe().Recv(ctx)
		require.NoError(t, err, "Recv[%d]", i)
		want := fmt.Sprintf("seq-%03d", i)
		require.Equal(t, want, string(m.Bytes()), "got[%d] (ORDER VIOLATION)", i)
	}
}

// TestWriter_Backpressure: with a gated Conn.Write, the writer stalls
// after draining its in-flight batch. Subsequent Send calls must block
// once the pipe HWM fills — this is the fundamental backpressure
// guarantee: batching must not drain the pipe to empty ahead of writes.

func TestWriter_Backpressure(t *testing.T) {
	t.Parallel()
	const HWM = 4
	sA, sB, wcA := newWritevTestPair(t, HWM, HWM)
	_ = sB

	// Install the gate AFTER the handshake (only traffic-phase flushes
	// go through WriteBuffers anyway) — the very first traffic-phase
	// writev parks on the gate, including any batch the writer coalesces
	// from the first blocking Recv + peeks.
	gate := make(chan struct{})
	wcA.setGate(gate)

	// Stop ordering: unblock gate first so the writer can complete its
	// in-flight writev, then the startPair cleanup Stops both sessions.
	defer func() {
		wcA.setGate(nil)
		close(gate)
	}()

	// Park the writer first: one priming message reaches the gated
	// writev (otherwise the writer's coalescing TryRecv loop can keep
	// pace with the pusher and swallow everything into one large batch
	// before flushing, hiding the HWM).
	primeCtx := testutil.Ctx(t, 2*time.Second)
	require.NoError(t, sA.Pipe().Send(primeCtx, msg.FromBytes([]byte("bp-prime"))), "Send(prime)")
	require.Eventually(t, func() bool { return wcA.gateWaiting.Load() == 1 },
		2*time.Second, time.Millisecond, "writer did not park in the gated writev")

	// With the writer parked, further Sends accumulate in the pipe until
	// its capacity (HWM) is reached, after which Send MUST block —
	// batching must not drain the pipe to empty ahead of the wire.
	pushCtx := testutil.Ctx(t, 200*time.Millisecond)

	var pushed int
	for i := 0; i < 4*HWM; i++ {
		err := sA.Pipe().Send(pushCtx, msg.FromBytes([]byte(fmt.Sprintf("bp-%d", i))))
		if err != nil {
			require.ErrorIs(t, err, context.DeadlineExceeded, "Send[%d]: unexpected err", i)
			break // expected — the pipe is full, Send blocks
		}
		pushed++
	}
	require.Greater(t, pushed, 0, "no messages pushed; test setup failure")
	// Backpressure: we must have blocked BEFORE pushing all 4*HWM.
	require.Less(t, pushed, 4*HWM,
		"pushed %d messages without blocking; backpressure broken (expected block after ≤%d)", pushed, 2*HWM)
	t.Logf("backpressure kicked in after %d pushes (HWM=%d, batch cap=%d)", pushed, HWM, maxBatchMessages)
}

// runCapCase sends n messages of size bytes through a session whose writer
// is parked in a gated first flush, so all but the first queue up and the
// caps decide how they are split into flushes. wrap adapts A's fake mech
// (identity framing, appending, or as is). It returns the byte and iovec
// count of every traffic-phase WriteBuffers call (the one-message first
// flush included) after checking that all n messages arrive in order.
func runCapCase(t *testing.T, wrap func(*fakeMech) security.Mech, n, size int) (sizes, bufs []int) {
	t.Helper()
	connA, connB := newFakeConnPair()
	wcA := newWritevConn(connA)
	sA, sB := startPair(t,
		Config{Conn: wcA, Mech: wrap(newFakeMech("NULL", false, nil)), SendHWM: n + 8, RecvHWM: n + 8},
		Config{Conn: connB, Mech: newFakeMech("NULL", true, nil), SendHWM: n + 8, RecvHWM: n + 8},
	)
	time.Sleep(20 * time.Millisecond)
	wcA.resetWriteCounters()
	wcA.bufMu.Lock()
	wcA.bufCounts = nil
	wcA.bufMu.Unlock()
	gate := make(chan struct{})
	wcA.setGate(gate)

	body := func(i int) []byte {
		b := make([]byte, size)
		if size >= 4 {
			binary.BigEndian.PutUint32(b, uint32(i))
		}
		return b
	}
	ctx := testutil.Ctx(t, 10*time.Second)
	require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes(body(0))), "Send[0]")
	require.Eventually(t, func() bool { return wcA.gateWaiting.Load() == 1 },
		2*time.Second, time.Millisecond, "writer did not park in the gated flush")
	for i := 1; i < n; i++ {
		require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes(body(i))), "Send[%d]", i)
	}
	wcA.setGate(nil)
	close(gate)
	for i := 0; i < n; i++ {
		m, err := sB.Pipe().Recv(ctx)
		require.NoError(t, err, "Recv[%d]", i)
		require.Len(t, m.Bytes(), size, "message %d", i)
		if size >= 4 {
			require.EqualValues(t, i, binary.BigEndian.Uint32(m.Bytes()), "message %d out of order", i)
		}
	}
	return wcA.Sizes(), wcA.BufCounts()
}

func asIs(m *fakeMech) security.Mech { return m }

// TestWriter_BatchCap: the batch is bounded by iovec count and wire bytes
// (and, for the coalescing paths, copied bytes and message count), not a
// fixed 32 messages. Each cap is checked before a message is pulled, so a
// flush may overshoot by that one message.
func TestWriter_BatchCap(t *testing.T) {
	t.Parallel()
	t.Run("iovecs", func(t *testing.T) {
		t.Parallel()
		// The default path costs two iovecs per message (header in
		// scratch, body by reference): 1500 messages must split.
		sizes, bufs := runCapCase(t, asIs, 1500, 16)
		assert.GreaterOrEqual(t, len(sizes), 2, "expected multiple flushes")
		for i, c := range bufs {
			assert.LessOrEqual(t, c, maxBatchIovecs+2, "flush %d has %d iovecs", i, c)
		}
		t.Logf("flushes=%d iovecs=%v", len(sizes), bufs)
	})
	t.Run("copied", func(t *testing.T) {
		t.Parallel()
		// Identity framing copies ~1 KiB bodies into scratch, so
		// maxBatchCopied (64 KiB) binds long before the iovec or byte cap.
		sizes, bufs := runCapCase(t, func(m *fakeMech) security.Mech { return identityFramingMech{m} }, 300, 1000)
		assert.GreaterOrEqual(t, len(sizes), 2, "expected multiple flushes")
		for i, sz := range sizes {
			assert.LessOrEqual(t, sz, maxBatchCopied+1024+16, "flush %d size=%d", i, sz)
		}
		t.Logf("flushes=%d sizes=%v iovecs=%v", len(sizes), sizes, bufs)
	})
	t.Run("bytes", func(t *testing.T) {
		t.Parallel()
		// 40 x 128 KiB = 5 MiB against the 2 MiB byte cap.
		sizes, _ := runCapCase(t, asIs, 40, 128<<10)
		assert.GreaterOrEqual(t, len(sizes), 2, "expected multiple flushes")
		for i, sz := range sizes {
			assert.LessOrEqual(t, sz, maxBatchBytes+(128<<10)+64, "flush %d size=%d", i, sz)
		}
		t.Logf("flushes=%d sizes=%v", len(sizes), sizes)
	})
}

// TestWriter_BatchLingerDrain: cancel writerCtx while messages are
// queued; assert drainSessionPipe flushes everything using the
// batched path.

func TestWriter_BatchLingerDrain(t *testing.T) {
	t.Parallel()
	sA, sB, ccA := newWriterTestPair(t, 64, 64)
	// Note: we Stop manually below after verifying drain semantics.
	defer func() { _ = sA.Stop(); _ = sB.Stop() }()

	time.Sleep(20 * time.Millisecond)
	ccA.resetWriteCounters()

	// Enqueue messages WITHOUT having B drain: the writer runs normally
	// and sends them. We want them on the wire before the Drain.
	const N = 10
	ctx := testutil.Ctx(t, 2*time.Second)
	for i := 0; i < N; i++ {
		require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte(fmt.Sprintf("d-%d", i)))), "Send[%d]", i)
	}

	// B drains to verify order + delivery.
	for i := 0; i < N; i++ {
		m, err := sB.Pipe().Recv(ctx)
		require.NoError(t, err, "Recv[%d]", i)
		want := fmt.Sprintf("d-%d", i)
		assert.Equal(t, want, string(m.Bytes()), "got[%d]", i)
	}

	// Now exercise the Drain linger path: push a few more messages, then
	// Drain; the drain path should flush them.
	for i := 0; i < 3; i++ {
		require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte(fmt.Sprintf("linger-%d", i)))), "linger Send[%d]", i)
	}
	waitClosed(t, sA.Drain(), 2*time.Second, "Drain did not finish")
	// The linger messages should have been delivered to B.
	for i := 0; i < 3; i++ {
		recvCtx, rc := context.WithTimeout(context.Background(), time.Second)
		m, err := sB.Pipe().Recv(recvCtx)
		rc()
		if assert.NoError(t, err, "linger Recv[%d]", i) {
			want := fmt.Sprintf("linger-%d", i)
			assert.Equal(t, want, string(m.Bytes()), "linger got[%d]", i)
		}
	}
}

// TestWriter_EmptyMessageStillEmitted: an empty Msg (no frames) must
// still emit one zero-length non-MORE frame so the peer sees a
// message boundary.

func TestWriter_EmptyMessageStillEmitted(t *testing.T) {
	t.Parallel()
	sA, sB, _ := newWriterTestPair(t, 4, 4)
	defer func() { _ = sA.Stop(); _ = sB.Stop() }()

	ctx := testutil.Ctx(t, time.Second)

	require.NoError(t, sA.Pipe().Send(ctx, msg.New()), "Send(empty)")
	got, err := sB.Pipe().Recv(ctx)
	require.NoError(t, err, "Recv")
	require.Equal(t, 1, got.NumFrames(), "empty msg surfaced as wrong number of frames; want 1 zero-length frame")
	require.Empty(t, got.Frames()[0], "frame not zero-length")
}

// TestWriter_MultipartBatched: multipart messages must be batched as a
// single contiguous sequence of frames with MORE flags preserved.

func TestWriter_MultipartBatched(t *testing.T) {
	t.Parallel()
	sA, sB, _ := newWriterTestPair(t, 16, 16)
	defer func() { _ = sA.Stop(); _ = sB.Stop() }()

	ctx := testutil.Ctx(t, 2*time.Second)

	// Send 4 multipart messages; each has 3 frames.
	for i := 0; i < 4; i++ {
		m := msg.FromFrames(
			[]byte(fmt.Sprintf("hdr-%d", i)),
			[]byte(fmt.Sprintf("mid-%d", i)),
			[]byte(fmt.Sprintf("tail-%d", i)),
		)
		require.NoError(t, sA.Pipe().Send(ctx, m), "Send[%d]", i)
	}

	for i := 0; i < 4; i++ {
		m, err := sB.Pipe().Recv(ctx)
		require.NoError(t, err, "Recv[%d]", i)
		require.Equal(t, 3, m.NumFrames(), "msg[%d] frames", i)
		want := []string{
			fmt.Sprintf("hdr-%d", i),
			fmt.Sprintf("mid-%d", i),
			fmt.Sprintf("tail-%d", i),
		}
		for j, f := range m.Frames() {
			assert.Equal(t, want[j], string(f), "msg[%d].frame[%d]", i, j)
		}
	}
}

// TestReader_HandlesPingWhileWriterBlocked: regression for the
// reader-writes-PONG-inline deadlock. With the writer goroutine wedged
// in a conn write (peer not draining), an incoming
// PING must NOT stall the reader — the PONG is routed to the writer via
// the command channel and the reader keeps reading.

func TestReader_HandlesPingWhileWriterBlocked(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	wcA := newWritevConn(connA)
	sA := startTestSession(Config{
		Conn: wcA, Mech: newFakeMech("NULL", false, nil), Codec: fakeCodec{},
		SendHWM: 8, RecvHWM: 8,
	})
	t.Cleanup(func() { _ = sA.Stop() })
	peer := newManualPeer(t, connB, nil)
	waitHandshake(t, sA, "A")
	frames := peer.startReadLoop()

	// Wedge the writer: gate traffic-phase writevs, then send a user
	// message so the writer parks inside WriteBuffers.
	gate := make(chan struct{})
	wcA.setGate(gate)
	ctx := testutil.Ctx(t, 2*time.Second)
	require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte("stall"))), "Send(stall)")
	require.Eventually(t, func() bool { return wcA.gateWaiting.Load() == 1 },
		2*time.Second, time.Millisecond, "writer did not park in the gated writev")

	// A PING arriving now must not block the reader (pre-fix, the inline
	// PONG write would wait behind the wedged writer) ...
	peer.sendPing(0, []byte("ctx1"))
	// ... proven by data sent AFTER the PING still flowing to the engine.
	peer.sendData([]byte("after-ping"))
	got, err := sA.Pipe().Recv(testutil.Ctx(t, 2*time.Second))
	require.NoError(t, err, "reader blocked behind the wedged writer")
	require.Equal(t, "after-ping", string(got.Bytes()))

	// Release the writer: the stalled flush and the queued PONG drain.
	wcA.setGate(nil)
	close(gate)
	require.Equal(t, []byte("ctx1"), waitCommand(t, frames, "PONG", 2*time.Second),
		"PONG must echo the PING context")
}

// TestWriter_LingerDrainUnboundedQueue: with SendHWM 0 most of a large
// outbound backlog sits in the pipe's overflow, past its channel. The
// linger drain must flush every queued message.

func TestWriter_LingerDrainUnboundedQueue(t *testing.T) {
	t.Parallel()
	for _, hwm := range []int{0, 64} {
		t.Run(fmt.Sprintf("sendHWM=%d", hwm), func(t *testing.T) {
			t.Parallel()
			sA, sB, _ := newWriterTestPair(t, hwm, 0)
			const N = 5000
			ctx := testutil.Ctx(t, 5*time.Second)
			got := make(chan int, 1)
			go func() {
				k := 0
				for {
					rc, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
					_, err := sB.Pipe().Recv(rc)
					cancel()
					if err != nil {
						got <- k
						return
					}
					k++
				}
			}()
			for i := 0; i < N; i++ {
				require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte("x"))), "Send[%d]", i)
			}
			waitClosed(t, sA.Drain(), 5*time.Second, "Drain did not finish")
			require.Equal(t, N, <-got, "messages delivered by the linger drain")
		})
	}
}

// --------------------------------------------------------------------------
// writerBatch layout tests. They drive appendMessage / writeBatchFrom on an
// unstarted session and inspect the iovec list directly.
// --------------------------------------------------------------------------

// newBatchSession builds an unstarted session around mech for driving the
// writer's batch code directly (fakeCodec framing, no wire).
func newBatchSession(t *testing.T, mech security.Mech) *Session {
	t.Helper()
	connA, _ := newFakeConnPair()
	t.Cleanup(func() { _ = connA.Close() })
	return New(Config{Conn: connA, Mech: mech, Codec: fakeCodec{}})
}

func identitySession(t *testing.T) *Session {
	return newBatchSession(t, identityFramingMech{newFakeMech("NULL", false, nil)})
}

// appendAll appends each frame list as one message and returns the batch.
func appendAll(t *testing.T, s *Session, batch *writerBatch, msgs ...[][]byte) {
	t.Helper()
	var size int
	var err error
	for i, frames := range msgs {
		size, err = batch.appendMessage(s, msg.FromFrames(frames...), size)
		require.NoError(t, err, "appendMessage[%d]", i)
	}
}

func flatten(bufs net.Buffers) []byte {
	var all []byte
	for _, b := range bufs {
		all = append(all, b...)
	}
	return all
}

// decodeFake parses a fakeCodec byte stream (flags, uvarint length, body)
// into (flags, body) pairs.
func decodeFake(t *testing.T, wire []byte) (flags []uint8, bodies [][]byte) {
	t.Helper()
	for len(wire) > 0 {
		f := wire[0]
		n, k := binary.Uvarint(wire[1:])
		require.Positive(t, k, "bad length varint")
		wire = wire[1+k:]
		require.LessOrEqual(t, int(n), len(wire), "truncated frame")
		flags = append(flags, f)
		bodies = append(bodies, wire[:n:n])
		wire = wire[n:]
	}
	return flags, bodies
}

// TestWriterBatch_CoalescesSmallFrames: small identity frames, headers and
// bodies alike, collapse into ONE iovec, and the bytes decode to the
// frames in order with MORE flags intact.
func TestWriterBatch_CoalescesSmallFrames(t *testing.T) {
	t.Parallel()
	s := identitySession(t)
	// Pre-sized like a warm batch: a cold scratch relocates as it grows,
	// and each relocation legitimately starts a new entry.
	batch := writerBatch{scratch: make([]byte, 0, 4096)}
	var want [][]byte
	for i := 0; i < 50; i++ {
		var frames [][]byte
		if i%5 == 0 {
			frames = [][]byte{[]byte(fmt.Sprintf("a-%d", i)), nil, []byte(fmt.Sprintf("b-%d", i))}
		} else {
			frames = [][]byte{[]byte(fmt.Sprintf("m-%d", i))}
		}
		want = append(want, frames...)
		appendAll(t, s, &batch, frames)
	}
	require.Len(t, batch.buffers, 1, "small frames did not coalesce into one run")
	flags, bodies := decodeFake(t, flatten(batch.buffers))
	require.Len(t, bodies, len(want))
	for i := range want {
		require.Equal(t, string(want[i]), string(bodies[i]), "frame %d", i)
	}
	require.Equal(t, uint8(0x01), flags[0]&0x01, "MORE flag lost on first frame of a multipart message")
	require.Equal(t, len(batch.scratch), batch.copied, "identity run is all copied bytes")
}

// TestWriterBatch_LargeBodyByReference: a body over coalesceBodyMax is
// handed to the wire by reference between two runs.
func TestWriterBatch_LargeBodyByReference(t *testing.T) {
	t.Parallel()
	s := identitySession(t)
	var batch writerBatch
	large := bytes.Repeat([]byte{'L'}, coalesceBodyMax+1)
	appendAll(t, s, &batch, [][]byte{[]byte("small-1")}, [][]byte{large}, [][]byte{[]byte("small-2")})
	bufs := batch.buffers
	// small-1 + large's header, then large by reference, then small-2.
	require.Len(t, bufs, 3)
	require.Same(t, &large[0], &bufs[1][0], "large body was copied, not sent by reference")
	require.Equal(t, large, bufs[1])
	_, bodies := decodeFake(t, flatten(bufs))
	require.Len(t, bodies, 3)
	require.Equal(t, "small-1", string(bodies[0]))
	require.Equal(t, large, bodies[1])
	require.Equal(t, "small-2", string(bodies[2]))
}

// TestWriterBatch_ScratchRelocation: appends that outgrow scratch relocate
// it; earlier entries keep pointing at the old array, new bytes start a
// new entry, and the flattened stream is still exact.
func TestWriterBatch_ScratchRelocation(t *testing.T) {
	t.Parallel()
	s := identitySession(t)
	var batch writerBatch
	batch.scratch = make([]byte, 0, 8)
	var msgs [][][]byte
	for i := 0; i < 20; i++ {
		msgs = append(msgs, [][]byte{[]byte(fmt.Sprintf("reloc-%02d", i))})
	}
	appendAll(t, s, &batch, msgs...)
	require.Greater(t, len(batch.buffers), 1, "scratch never relocated; test setup failure")
	_, bodies := decodeFake(t, flatten(batch.buffers))
	require.Len(t, bodies, len(msgs))
	for i, m := range msgs {
		require.Equal(t, string(m[0]), string(bodies[i]), "frame %d", i)
	}
	// Entries stay capped so nothing can append through them.
	for i, b := range batch.buffers {
		require.Equal(t, len(b), cap(b), "buffer %d not capacity-capped", i)
	}
}

// TestWriterBatch_DefaultPathByReference: a mechanism that returns fresh
// wire memory keeps header-in-scratch, body-by-reference.
func TestWriterBatch_DefaultPathByReference(t *testing.T) {
	t.Parallel()
	s := newBatchSession(t, xorMech{newFakeMech("NULL", false, nil)})
	var batch writerBatch
	appendAll(t, s, &batch, [][]byte{[]byte("one")}, [][]byte{[]byte("two")}, [][]byte{nil})
	// hdr, body, hdr, body, hdr(empty body omitted)
	require.Len(t, batch.buffers, 5)
	_, bodies := decodeFake(t, flatten(batch.buffers))
	require.Len(t, bodies, 3)
	require.Equal(t, xorBytes([]byte("one")), bodies[0])
	require.Equal(t, xorBytes([]byte("two")), bodies[1])
	require.Empty(t, bodies[2])
}

// TestWriterBatch_Caps: the pull loop honours maxBatchMessages and
// maxBatchIovecs. Drives writeBatchFrom against a conn that records each
// flush.
func TestWriterBatch_Caps(t *testing.T) {
	t.Parallel()
	run := func(t *testing.T, mech security.Mech, n, size int) (flushes []recordedFlush) {
		t.Helper()
		connA, _ := newFakeConnPair()
		t.Cleanup(func() { _ = connA.Close() })
		rc := &recordingBufConn{fakeConn: connA}
		s := New(Config{Conn: rc, Mech: mech, Codec: fakeCodec{}})
		sp, ep := pipe.NewPair(n+1, n+1, pipe.PeerInfo{}, pipe.PeerInfo{})
		s.sessionPipe = sp
		body := bytes.Repeat([]byte{'x'}, size)
		for i := 0; i < n; i++ {
			require.True(t, ep.TrySend(msg.FromBytes(body)))
		}
		var batch writerBatch
		for {
			m, ok := sp.TryRecv()
			if !ok {
				break
			}
			require.NoError(t, s.writeBatchFrom(&batch, m))
		}
		return rc.flushes
	}

	t.Run("messages", func(t *testing.T) {
		t.Parallel()
		// 2-byte bodies coalesce into one iovec, so the message cap binds.
		fl := run(t, identityFramingMech{newFakeMech("NULL", false, nil)}, 5000, 2)
		require.GreaterOrEqual(t, len(fl), 2)
		var total int
		for i, f := range fl {
			total += f.bytes / 4 // flags + len + 2 body bytes
			require.LessOrEqual(t, f.bytes/4, maxBatchMessages, "flush %d", i)
		}
		require.Equal(t, 5000, total)
	})
	t.Run("iovecs", func(t *testing.T) {
		t.Parallel()
		// 2 KiB bodies go by reference: two iovecs per message.
		fl := run(t, identityFramingMech{newFakeMech("NULL", false, nil)}, 200*10, 2<<10)
		require.GreaterOrEqual(t, len(fl), 2)
		for i, f := range fl {
			require.LessOrEqual(t, f.bufs, maxBatchIovecs+2, "flush %d", i)
		}
	})
}

type recordedFlush struct{ bufs, bytes int }

// recordingBufConn records every WriteBuffers call and consumes the batch.
type recordingBufConn struct {
	*fakeConn
	flushes []recordedFlush
}

func (c *recordingBufConn) WriteBuffers(bufs *net.Buffers) (int64, error) {
	var n int
	for _, b := range *bufs {
		n += len(b)
	}
	c.flushes = append(c.flushes, recordedFlush{len(*bufs), n})
	*bufs = (*bufs)[len(*bufs):]
	return int64(n), nil
}

// TestWriterBatch_AppendMechRotates: a FrameAppender mechanism's small
// frames are rotated into wire order (header first) and fold into one run;
// a large sealed body stays in place and goes out as header + body entries.
func TestWriterBatch_AppendMechRotates(t *testing.T) {
	t.Parallel()
	s := newBatchSession(t, appendMech{newFakeMech("NULL", false, nil)})
	var batch writerBatch
	batch.scratch = make([]byte, 0, 1<<16) // warm: no relocation splits

	var msgs [][][]byte
	for i := 0; i < 30; i++ {
		msgs = append(msgs, [][]byte{[]byte(fmt.Sprintf("sealed-%02d", i))})
	}
	appendAll(t, s, &batch, msgs...)
	require.Len(t, batch.buffers, 1, "small sealed frames did not fold into one run")
	flags, bodies := decodeFake(t, flatten(batch.buffers))
	require.Len(t, bodies, len(msgs), "header-first order broken")
	for i, m := range msgs {
		require.Equal(t, string(m[0]), string(bodies[i]), "frame %d", i)
		require.Zero(t, flags[i])
	}
	batch.release()

	large := bytes.Repeat([]byte{'S'}, coalesceBodyMax+1)
	appendAll(t, s, &batch, [][]byte{[]byte("small-1")}, [][]byte{large}, [][]byte{[]byte("small-2")})
	// small-1 | large header | large body | small-2
	require.Len(t, batch.buffers, 4)
	require.Equal(t, large, batch.buffers[2])
	require.LessOrEqual(t, len(batch.buffers[1]), 11, "second entry should be the large frame's header")
	_, bodies = decodeFake(t, flatten(batch.buffers))
	require.Len(t, bodies, 3)
	require.Equal(t, "small-1", string(bodies[0]))
	require.Equal(t, large, bodies[1])
	require.Equal(t, "small-2", string(bodies[2]))
}
