package session

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/msg"
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

// --------------------------------------------------------------------------
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
// --------------------------------------------------------------------------

type writevConn struct {
	*countingConn

	gate atomic.Pointer[chan struct{}]
	// gateWaiting counts WriteBuffers calls currently parked on the
	// gate, so tests can wait until the writer is provably stalled.
	gateWaiting atomic.Int32
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
	var all []byte
	for _, b := range *bufs {
		all = append(all, b...)
	}
	*bufs = (*bufs)[:0] // consumed, mirroring net.Buffers.WriteTo
	n, err := c.Write(all)
	return int64(n), err
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
	sA, sB = startPair(t,
		Config{Conn: ccA, Mech: newFakeMech("NULL", false, map[string]string{"Identity": "A"}), SendHWM: sendHWM, RecvHWM: recvHWM},
		Config{Conn: connB, Mech: newFakeMech("NULL", true, map[string]string{"Identity": "B"}), SendHWM: sendHWM, RecvHWM: recvHWM},
	)
	return sA, sB, ccA
}

// newWritevTestPair is newWriterTestPair with a writev-emulating conn on
// the A side, for tests that observe batch coalescing or gate the wire.
func newWritevTestPair(t *testing.T, sendHWM, recvHWM int) (sA, sB iface.Session, wcA *writevConn) {
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

// --------------------------------------------------------------------------
// TestWriter_BatchRetainsBufferCapacityAcrossFlushes: net.Buffers.WriteTo
// and the tcpConn WriteBuffers fast path CONSUME the slice header they
// are handed (*v = (*v)[i:]), so flushBatch must be given a COPY of the
// batch's buffers header. Regression check: the batch's backing array
// must survive flushes — before the fix, release() truncated the
// consumed (empty) tail and every batch re-grew from zero capacity.
// --------------------------------------------------------------------------

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
	connA, _ := newFakeConnPair()
	s := New(Config{
		Conn:  &consumeWritevConn{fakeConn: connA},
		Mech:  newFakeMech("NULL", false, nil),
		Codec: fakeCodec{},
	}).(*sessionImpl)
	t.Cleanup(func() { _ = s.Stop() })
	sp, ep := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	s.sessionPipe = asSessionChanPipe(sp)
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
}

// --------------------------------------------------------------------------
// TestWriter_BatchCoalescesMultipleMessages: enqueue many messages
// rapidly; verify the writer coalesces them into fewer writev calls than
// messages via net.Buffers.
//
// Timing-deterministic via the gated writev conn: stall the writer's
// first traffic-phase flush, queue all N messages into the sessionPipe
// while it is parked, then unblock the gate — at most one more
// writer-loop iteration coalesces everything still queued.
// --------------------------------------------------------------------------

func TestWriter_BatchCoalescesMultipleMessages(t *testing.T) {
	const HWM = 64
	sA, sB, wcA := newWritevTestPair(t, HWM, HWM)

	gate := make(chan struct{})
	wcA.setGate(gate)
	wcA.resetWriteCounters()

	const N = 5
	ctx := testCtx(t, 2*time.Second)
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
	primeCtx := testCtx(t, 2*time.Second)
	require.NoError(t, sA.Pipe().Send(primeCtx, msg.FromBytes([]byte("bp-prime"))), "Send(prime)")
	require.Eventually(t, func() bool { return wcA.gateWaiting.Load() == 1 },
		2*time.Second, time.Millisecond, "writer did not park in the gated writev")

	// With the writer parked, further Sends accumulate in the pipe until
	// its capacity (HWM) is reached, after which Send MUST block —
	// batching must not drain the pipe to empty ahead of the wire.
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
		waitClosed(t, sA.WriterFlushed(), 2*time.Second, "WriterFlushed did not close after CancelWriter")
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
// TestReader_HandlesPingWhileWriterBlocked: regression for the
// reader-writes-PONG-inline deadlock. With the writer goroutine wedged
// in a conn write (peer not draining) and holding writeMu, an incoming
// PING must NOT stall the reader — the PONG is routed to the writer via
// the command channel and the reader keeps reading.
// --------------------------------------------------------------------------

func TestReader_HandlesPingWhileWriterBlocked(t *testing.T) {
	connA, connB := newFakeConnPair()
	wcA := newWritevConn(connA)
	sA := New(Config{
		Conn: wcA, Mech: newFakeMech("NULL", false, nil), Codec: fakeCodec{},
		SendHWM: 8, RecvHWM: 8,
	})
	sA.Start()
	t.Cleanup(func() { _ = sA.Stop() })
	peer := newManualPeer(t, connB, nil)
	waitHandshake(t, sA, "A")
	frames := peer.startReadLoop()

	// Wedge the writer: gate traffic-phase writevs, then send a user
	// message so the writer parks inside WriteBuffers holding writeMu.
	gate := make(chan struct{})
	wcA.setGate(gate)
	ctx := testCtx(t, 2*time.Second)
	require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte("stall"))), "Send(stall)")
	require.Eventually(t, func() bool { return wcA.gateWaiting.Load() == 1 },
		2*time.Second, time.Millisecond, "writer did not park in the gated writev")

	// A PING arriving now must not block the reader (pre-fix, the inline
	// PONG write would deadlock on writeMu) ...
	peer.sendPing(0, []byte("ctx1"))
	// ... proven by data sent AFTER the PING still flowing to the engine.
	peer.sendData([]byte("after-ping"))
	got, err := sA.Pipe().Recv(testCtx(t, 2*time.Second))
	require.NoError(t, err, "reader blocked behind the wedged writer")
	require.Equal(t, "after-ping", string(got.Bytes()))

	// Release the writer: the stalled flush and the queued PONG drain.
	wcA.setGate(nil)
	close(gate)
	require.Equal(t, []byte("ctx1"), waitCommand(t, frames, "PONG", 2*time.Second),
		"PONG must echo the PING context")
}

// --------------------------------------------------------------------------
// TestWriter_LingerDrainUnboundedQueue: with SendHWM 0 the outbound
// direction is an unbounded relay that refills the pipe's receive channel
// one message at a time, so a TryRecv-until-empty drain stops early. The
// linger drain must flush every queued message.
// --------------------------------------------------------------------------

func TestWriter_LingerDrainUnboundedQueue(t *testing.T) {
	for _, hwm := range []int{0, 64} {
		t.Run(fmt.Sprintf("sendHWM=%d", hwm), func(t *testing.T) {
			sA, sB, _ := newWriterTestPair(t, hwm, 0)
			const N = 5000
			ctx := testCtx(t, 5*time.Second)
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
			sA.CancelWriter()
			waitClosed(t, sA.WriterFlushed(), 5*time.Second, "WriterFlushed did not close after CancelWriter")
			require.Equal(t, N, <-got, "messages delivered by the linger drain")
		})
	}
}
