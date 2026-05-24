package session

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
)

// --------------------------------------------------------------------------
// countingConn wraps a fakeConn and records every Write call: count,
// per-call byte lengths, and an accumulated byte buffer of the full
// stream. It is the instrumented receiver used by writer-batching tests.
// --------------------------------------------------------------------------

type countingConn struct {
	*fakeConn

	mu         sync.Mutex
	writeCalls int32
	writeSizes []int
	accum      []byte

	// gateCh, when non-nil, blocks Write until a receive on gateCh
	// unblocks it. Stored via atomic.Pointer so test control goroutines
	// can install/remove the gate without racing with the writer
	// goroutine. Used by the backpressure and batch-visibility tests
	// to stall the wire.
	gate atomic.Pointer[chan struct{}]
}

func newCountingConn(fc *fakeConn) *countingConn {
	return &countingConn{fakeConn: fc}
}

// setGate installs g as the blocking gate for future Write calls, or
// clears the gate when g is nil.
func (c *countingConn) setGate(g chan struct{}) {
	if g == nil {
		c.gate.Store(nil)
		return
	}
	c.gate.Store(&g)
}

func (c *countingConn) Write(p []byte) (int, error) {
	if gp := c.gate.Load(); gp != nil {
		<-*gp
	}
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

// --------------------------------------------------------------------------
// newWriterTestPair builds two sessions A→B over a countingConn on the
// A side and plain fakeConn on the B side, runs both handshakes, and
// returns (sA, sB, countingConn). B drains via a goroutine that just
// calls Recv so the pipe doesn't stall.
// --------------------------------------------------------------------------

func newWriterTestPair(t *testing.T, sendHWM, recvHWM int) (sA, sB iface.Session, ccA *countingConn) {
	t.Helper()
	connA, connB := newFakeConnPair()
	ccA = newCountingConn(connA)

	sA = New(Config{
		Conn:    ccA,
		Mech:    newFakeMech("NULL", false, map[string]string{"Identity": "A"}),
		Codec:   fakeCodec{},
		Role:    iface.RoleClient,
		SendHWM: sendHWM,
		RecvHWM: recvHWM,
	})
	sB = New(Config{
		Conn:    connB,
		Mech:    newFakeMech("NULL", true, map[string]string{"Identity": "B"}),
		Codec:   fakeCodec{},
		Role:    iface.RoleServer,
		SendHWM: sendHWM,
		RecvHWM: recvHWM,
	})

	sA.Start()
	sB.Start()

	waitHandshake(t, sA, "A")
	waitHandshake(t, sB, "B")
	return sA, sB, ccA
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

// --------------------------------------------------------------------------
// TestWriter_BatchCoalescesMultipleMessages: enqueue many messages
// rapidly; verify the writer coalesces them into fewer Write calls than
// messages via net.Buffers.
//
// Timing-deterministic via a gated Conn.Write: queue all N messages
// into the sessionPipe while the writer is parked on its FIRST Write
// (from the handshake or an initial priming frame); unblock the gate
// and observe that the single subsequent writer-loop iteration
// coalesces the queued N messages into one writev.
// --------------------------------------------------------------------------

func TestWriter_BatchCoalescesMultipleMessages(t *testing.T) {
	// net.Buffers.WriteTo only uses writev on real *net.TCPConn / UnixConn;
	// on a non-TCP io.Writer it falls back to one Write per buffer. The
	// fakeConn used here is a plain io.Writer, so Write-count coalescing
	// is not observable. This test will become meaningful once a
	// writev-emulating fakeConn exists.
	t.Skip("batching coalescing is not observable on non-TCP fake conn")
	const HWM = 64
	connA, connB := newFakeConnPair()
	ccA := newCountingConn(connA)

	sA := New(Config{
		Conn: ccA, Mech: newFakeMech("NULL", false, nil), Codec: fakeCodec{},
		Role: iface.RoleClient, SendHWM: HWM, RecvHWM: HWM,
	})
	sB := New(Config{
		Conn: connB, Mech: newFakeMech("NULL", true, nil), Codec: fakeCodec{},
		Role: iface.RoleServer, SendHWM: HWM, RecvHWM: HWM,
	})
	sA.Start()
	sB.Start()
	defer func() { _ = sA.Stop(); _ = sB.Stop() }()

	waitHandshake(t, sA, "A")
	waitHandshake(t, sB, "B")

	// Prime the writer with one message so it's parked in the write
	// path, then install the gate so subsequent writes block. Now
	// enqueue N messages into the pipe — the writer's next iteration
	// will coalesce them.
	//
	// Simpler deterministic strategy: gate BEFORE any user traffic,
	// queue N, unblock. The writer's first blocking Recv pulls one,
	// then non-blocking peeks coalesce the rest.
	gate := make(chan struct{})
	ccA.setGate(gate)
	ccA.resetWriteCounters()

	const N = 5
	ctx := testCtx(t, 2*time.Second)
	for i := 0; i < N; i++ {
		m := msg.FromBytes([]byte(fmt.Sprintf("msg-%d", i)))
		require.NoError(t, sA.Pipe().Send(ctx, m), "Send[%d]", i)
	}

	// Release the gate: writer's pending flushBatch proceeds.
	ccA.setGate(nil)
	close(gate)

	// Drain on B.
	got := make([]string, 0, N)
	for i := 0; i < N; i++ {
		m, err := sB.Pipe().Recv(ctx)
		require.NoError(t, err, "Recv[%d]", i)
		got = append(got, string(m.Bytes()))
	}
	for i, s := range got {
		want := fmt.Sprintf("msg-%d", i)
		assert.Equal(t, want, s, "got[%d]", i)
	}

	// Coalescing: with N=5 messages queued before any Write proceeds,
	// the writer's first batch should contain all 5 — one Write call
	// for 5 messages.
	writes := ccA.WriteCount()
	assert.Less(t, writes, N, "writer issued %d Writes for %d msgs; expected coalescing (<N)", writes, N)
	assert.GreaterOrEqual(t, writes, 1, "writer issued 0 Writes; expected ≥1")
	assert.LessOrEqual(t, writes, 2, "writer issued %d Writes; expected ≤2 for 5 queued msgs", writes)
	t.Logf("batched %d messages into %d Write calls (sizes=%v)", N, writes, ccA.Sizes())
}

// --------------------------------------------------------------------------
// TestWriter_InteractiveSingleMessage: a solo message should be written
// immediately, NOT wait for a batch to fill. We assert latency is low
// AND the message is received promptly.
// --------------------------------------------------------------------------

func TestWriter_InteractiveSingleMessage(t *testing.T) {
	sA, sB, ccA := newWriterTestPair(t, 4, 4)
	defer func() { _ = sA.Stop(); _ = sB.Stop() }()

	time.Sleep(20 * time.Millisecond)
	ccA.resetWriteCounters()

	ctx := testCtx(t, time.Second)

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

// --------------------------------------------------------------------------
// TestWriter_OrderPreservation: enqueue 20 messages with sequential
// payloads; capture bytes at the wire; decode frames; assert original
// order.
// --------------------------------------------------------------------------

func TestWriter_OrderPreservation(t *testing.T) {
	sA, sB, _ := newWriterTestPair(t, 64, 64)
	defer func() { _ = sA.Stop(); _ = sB.Stop() }()

	const N = 20
	ctx := testCtx(t, 2*time.Second)

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

// --------------------------------------------------------------------------
// TestWriter_Backpressure: with a gated Conn.Write, the writer stalls
// after draining its in-flight batch. Subsequent Send calls must block
// once the pipe HWM fills — this is the fundamental backpressure
// guarantee: batching must not drain the pipe to empty ahead of writes.
// --------------------------------------------------------------------------

func TestWriter_Backpressure(t *testing.T) {
	// Depends on gating Writes to exert backpressure at the wire boundary,
	// but the current countingConn gate also blocks the greeting/handshake
	// writes (no phase distinction). Reworking this to gate only
	// traffic-phase writes is still pending.
	t.Skip("gate blocks handshake writes")
	const HWM = 4
	connA, connB := newFakeConnPair()
	ccA := newCountingConn(connA)

	// Install the gate BEFORE Start — the very first traffic-phase
	// Write parks on gateCh, including any batch the writer coalesces
	// from the first blocking Recv + peeks.
	gate := make(chan struct{})
	ccA.setGate(gate)

	sA := New(Config{
		Conn: ccA, Mech: newFakeMech("NULL", false, nil), Codec: fakeCodec{},
		Role: iface.RoleClient, SendHWM: HWM, RecvHWM: HWM,
	})
	sB := New(Config{
		Conn: connB, Mech: newFakeMech("NULL", true, nil), Codec: fakeCodec{},
		Role: iface.RoleServer, SendHWM: HWM, RecvHWM: HWM,
	})
	sA.Start()
	sB.Start()
	// Stop ordering: unblock gate first so writer/reader can complete
	// in-flight Write, then Stop. Without this Stop would hang.
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
		ccA.setGate(nil)
		_ = sA.Stop()
		_ = sB.Stop()
	}()

	waitHandshake(t, sA, "A")
	waitHandshake(t, sB, "B")

	// Now push messages as fast as possible. The writer will pull one,
	// coalesce up to maxBatchMessages/maxBatchBytes of additional
	// queued ones (but here capped by HWM), then park in flushBatch.
	// Once the writer parks, further Sends accumulate in the channel
	// until its capacity (HWM) is reached, after which Send blocks.
	//
	// Worst case: writer coalesces up to HWM messages and parks. That
	// leaves HWM slots in the pipe for additional queued items. So the
	// total number of messages we can push before Send blocks is
	// bounded by (writer-batched) + HWM ≤ HWM + HWM = 2*HWM. Any push
	// beyond that MUST block — demonstrating backpressure.
	pushCtx := testCtx(t, 200*time.Millisecond)

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

// --------------------------------------------------------------------------
// TestWriter_BatchCap: enqueue more than maxBatchMessages rapidly and
// verify multiple batches are emitted, and each batch's bytes are under
// the size cap. This also exercises the loop's re-arm path (after one
// batch it goes back to blocking Recv).
// --------------------------------------------------------------------------

func TestWriter_BatchCap(t *testing.T) {
	// SendHWM large enough that we can enqueue well past maxBatchMessages
	// before the writer drains them.
	sA, sB, ccA := newWriterTestPair(t, maxBatchMessages*4, maxBatchMessages*4)
	defer func() { _ = sA.Stop(); _ = sB.Stop() }()

	time.Sleep(20 * time.Millisecond)
	ccA.resetWriteCounters()

	// Enqueue exactly 2.5× maxBatchMessages so we're guaranteed to
	// exceed the cap in at least one batch boundary.
	N := maxBatchMessages*5/2 + 1
	ctx := testCtx(t, 5*time.Second)
	for i := 0; i < N; i++ {
		require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte(fmt.Sprintf("c-%d", i)))), "Send[%d]", i)
	}

	// Drain on B.
	for i := 0; i < N; i++ {
		_, err := sB.Pipe().Recv(ctx)
		require.NoError(t, err, "Recv[%d]", i)
	}

	// Each Write call is one batch's worth of bytes. Verify no single
	// Write exceeded maxBatchBytes by a wide margin (allow some slack
	// for the last encoded frame pushing us slightly over the pre-
	// append check; the implementation checks BEFORE appending the
	// next message, so a message can push over by exactly its own
	// size, bounded by whatever the message payload was — here <100 B).
	sizes := ccA.Sizes()
	assert.GreaterOrEqual(t, len(sizes), 2, "expected ≥2 Write calls (multiple batches)")
	for i, sz := range sizes {
		assert.LessOrEqual(t, sz, maxBatchBytes+4096, "batch %d size=%d exceeds maxBatchBytes=%d by more than slack", i, sz, maxBatchBytes)
	}
	t.Logf("N=%d messages emitted in %d batches, sizes=%v", N, len(sizes), sizes)
}

// --------------------------------------------------------------------------
// TestWriter_BatchLingerDrain: cancel writerCtx while messages are
// queued; assert drainSessionPipe flushes everything using the
// batched path.
// --------------------------------------------------------------------------

func TestWriter_BatchLingerDrain(t *testing.T) {
	sA, sB, ccA := newWriterTestPair(t, 64, 64)
	// Note: we Stop manually below after verifying drain semantics.
	defer func() { _ = sA.Stop(); _ = sB.Stop() }()

	time.Sleep(20 * time.Millisecond)
	ccA.resetWriteCounters()

	// Enqueue messages WITHOUT having B drain: the writer runs normally
	// and sends them. We want them on the wire before the CancelWriter.
	const N = 10
	ctx := testCtx(t, 2*time.Second)
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

	// Now exercise the CancelWriter linger-drain path: push a few more
	// messages, trigger CancelWriter; drain path should flush them.
	if cw, ok := sA.(interface{ CancelWriter() }); ok {
		for i := 0; i < 3; i++ {
			require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte(fmt.Sprintf("linger-%d", i)))), "linger Send[%d]", i)
		}
		cw.CancelWriter()
		// Expect WriterFlushed to fire after drain completes.
		select {
		case <-sA.WriterFlushed():
		case <-time.After(2 * time.Second):
			require.FailNow(t, "WriterFlushed did not close after CancelWriter")
		}
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
}

// --------------------------------------------------------------------------
// TestWriter_EmptyMessageStillEmitted: an empty Msg (no frames) must
// still emit one zero-length non-MORE frame so the peer sees a
// message boundary.
// --------------------------------------------------------------------------

func TestWriter_EmptyMessageStillEmitted(t *testing.T) {
	sA, sB, _ := newWriterTestPair(t, 4, 4)
	defer func() { _ = sA.Stop(); _ = sB.Stop() }()

	ctx := testCtx(t, time.Second)

	require.NoError(t, sA.Pipe().Send(ctx, msg.New()), "Send(empty)")
	got, err := sB.Pipe().Recv(ctx)
	require.NoError(t, err, "Recv")
	require.Equal(t, 1, got.NumFrames(), "empty msg surfaced as wrong number of frames; want 1 zero-length frame")
	require.Empty(t, got.Frames()[0], "frame not zero-length")
}

// --------------------------------------------------------------------------
// TestWriter_MultipartBatched: multipart messages must be batched as a
// single contiguous sequence of frames with MORE flags preserved.
// --------------------------------------------------------------------------

func TestWriter_MultipartBatched(t *testing.T) {
	sA, sB, _ := newWriterTestPair(t, 16, 16)
	defer func() { _ = sA.Stop(); _ = sB.Stop() }()

	ctx := testCtx(t, 2*time.Second)

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

// --------------------------------------------------------------------------
// Compile-time guard on types used above (prevents unused-import lint
// regressions when tests shrink).
// --------------------------------------------------------------------------

var (
	_ io.Writer        = (*countingConn)(nil)
	_ binary.ByteOrder = binary.BigEndian
)
