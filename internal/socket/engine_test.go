package socket_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// helper: build N pipe pairs with given HWMs. Returns engine ends and
// peer ends. The caller is responsible for Close-ing both ends.
func makePairs(n, sendHWM, recvHWM int) (engineEnds, peerEnds []*pipe.End) {
	engineEnds = make([]*pipe.End, n)
	peerEnds = make([]*pipe.End, n)
	for i := 0; i < n; i++ {
		a, b := pipe.NewPair(sendHWM, recvHWM, nil, nil)
		engineEnds[i] = a
		peerEnds[i] = b
	}
	return
}

func closePipes(ps ...*pipe.End) {
	for _, p := range ps {
		_ = p.Close()
	}
}

// ---------------------- LoadBalancer ----------------------

func TestLoadBalancer_RoundRobin(t *testing.T) {
	t.Parallel()
	lb := newLB()
	defer lb.Close()

	const n = 3
	const send = 9
	engineEnds, peerEnds := makePairs(n, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)

	for _, p := range engineEnds {
		require.NoError(t, lb.Attach(p), "attach")
	}

	ctx := testutil.Ctx(t)
	for i := 0; i < send; i++ {
		m := msg.FromBytes([]byte{byte(i)})
		require.NoError(t, lb.Send(ctx, m), "send %d", i)
	}

	// Drain each peer end and assert each got exactly send/n messages.
	counts := make([]int, n)
	for i := 0; i < n; i++ {
		drainCtx, c := context.WithTimeout(context.Background(), time.Second)
		for {
			m, err := peerEnds[i].RecvOr(drainCtx, nil)
			if err != nil {
				break
			}
			_ = m
			counts[i]++
			if counts[i] >= send/n {
				break
			}
		}
		c()
	}
	for i, c := range counts {
		require.Equal(t, send/n, c, "peer %d got %d messages, expected %d", i, c, send/n)
	}
}

func TestLoadBalancer_BlocksOnHWMUnblocksOnDrain(t *testing.T) {
	t.Parallel()
	lb := newLB()
	defer lb.Close()

	// Single peer, HWM=1. First send fits in the channel; second send
	// blocks until the peer drains.
	a, b := pipe.NewPair(1, 1, nil, nil)
	defer closePipes(a, b)
	require.NoError(t, lb.Attach(a), "attach")

	ctx := testutil.Ctx(t)
	require.NoError(t, lb.Send(ctx, msg.FromBytes([]byte("one"))), "first send")

	// Second send must block until the peer drains "one"; then we drain "two".
	drainCtx, dc := context.WithTimeout(context.Background(), time.Second)
	defer dc()
	err := testutil.RequireUnblocks(t,
		func() error { return lb.Send(ctx, msg.FromBytes([]byte("two"))) },
		func() {
			_, err := b.RecvOr(drainCtx, nil)
			require.NoError(t, err, "drain one")
		})
	require.NoError(t, err, "second send")
	_, err = b.RecvOr(drainCtx, nil)
	require.NoError(t, err, "drain two")
}

func TestLoadBalancer_CtxCancelUnblocksSend(t *testing.T) {
	t.Parallel()
	lb := newLB()
	defer lb.Close()

	a, b := pipe.NewPair(1, 1, nil, nil)
	defer closePipes(a, b)
	require.NoError(t, lb.Attach(a), "attach")
	require.NoError(t, a.Send(context.Background(), msg.FromBytes([]byte("fill"))))

	ctx, cancel := context.WithCancel(context.Background())
	err := testutil.RequireUnblocks(t,
		func() error { return lb.Send(ctx, msg.FromBytes([]byte("x"))) },
		cancel)
	require.ErrorIs(t, err, context.Canceled)
}

// ---------------------- FairQueue ----------------------

func TestFairQueue_FairAcrossPipes(t *testing.T) {
	t.Parallel()
	fq := newFQ()
	defer fq.Close()

	const n = 3
	engineEnds, peerEnds := makePairs(n, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)

	for _, p := range engineEnds {
		require.NoError(t, fq.Attach(p), "attach")
	}

	// Pre-load each peer end with one distinct message.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	want := make(map[byte]bool)
	for i := 0; i < n; i++ {
		b := byte('a' + i)
		want[b] = true
		require.NoError(t, peerEnds[i].Send(ctx, msg.FromBytes([]byte{b})), "peer %d send", i)
	}

	got := make(map[byte]bool)
	for i := 0; i < n; i++ {
		m, err := fq.Recv(ctx)
		require.NoError(t, err, "recv %d", i)
		require.Equal(t, 1, m.NumFrames(), "recv %d: frames", i)
		got[m.Bytes()[0]] = true
	}
	for b := range want {
		require.True(t, got[b], "missing message %c (got %v)", b, got)
	}
}

func TestFairQueue_RecvWakesOnAttach(t *testing.T) {
	t.Parallel()
	fq := newFQ()
	defer fq.Close()

	ctx := testutil.Ctx(t)

	type result struct {
		m   *msg.Msg
		err error
	}
	a, b := pipe.NewPair(1, 1, nil, nil)
	defer closePipes(a, b)

	// Recv must block until we attach a pipe with a message available.
	r := testutil.RequireUnblocks(t, func() result {
		m, err := fq.Recv(ctx)
		return result{m: m, err: err}
	}, func() {
		require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("hello"))), "peer send")
		require.NoError(t, fq.Attach(a), "attach")
	})
	require.NoError(t, r.err, "recv")
	require.Equal(t, "hello", string(r.m.Bytes()))
}

// TestFairQueue_DetachThenClose: Close returns promptly after a Detach.
func TestFairQueue_DetachThenClose(t *testing.T) {
	t.Parallel()
	fq := newFQ()

	a, b := pipe.NewPair(1, 1, nil, nil)
	defer closePipes(a, b)

	require.NoError(t, fq.Attach(a), "attach")
	require.NoError(t, fq.Detach(a), "detach")

	done := testutil.Async(fq.Close)
	require.NoError(t, testutil.RequireRecv(t, done, time.Second, "Close did not return after Detach"), "close")
}

// TestFairQueue_CloseWithAttachedPipes: Close with pipes still attached
// leaves nothing running (goleak) and the pipes usable by their owners.
func TestFairQueue_CloseWithAttachedPipes(t *testing.T) {
	t.Parallel()
	fq := newFQ()

	const n = 4
	engineEnds, peerEnds := makePairs(n, 1, 1)
	defer closePipes(peerEnds...) // engine ends will be reaped via fq.Close

	for _, p := range engineEnds {
		require.NoError(t, fq.Attach(p), "attach")
	}

	require.NoError(t, fq.Close(), "close")
	// A send after Close must not reach back into the closed queue.
	require.NoError(t, peerEnds[0].Send(testutil.Ctx(t), msg.FromBytes([]byte("x"))))
	closePipes(engineEnds...)
}

// TestFairQueue_DetachedPipeNoLongerNotifies: a pipe detached while idle
// no longer reaches the queue: its traffic neither wakes a parked Recv nor
// is received.
func TestFairQueue_DetachedPipeNoLongerNotifies(t *testing.T) {
	t.Parallel()
	fq := newFQ()
	defer fq.Close()

	engineEnds, peerEnds := makePairs(2, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)
	for _, p := range engineEnds {
		require.NoError(t, fq.Attach(p), "attach")
	}
	require.False(t, fq.CanRecv(), "both pipes idle")
	require.NoError(t, fq.Detach(engineEnds[0]), "detach")
	require.NoError(t, peerEnds[0].Send(testutil.Ctx(t), msg.FromBytes([]byte("gone"))))
	require.False(t, fq.Pending(), "a detached pipe must not activate")

	require.NoError(t, peerEnds[1].Send(testutil.Ctx(t), msg.FromBytes([]byte("kept"))))
	m, err := fq.Recv(testutil.Ctx(t))
	require.NoError(t, err)
	require.Equal(t, "kept", string(m.Bytes()))
}

// TestFairQueue_DetachClosedDirectPipeDrains: detaching the sole pipe
// (single-pipe path) after its peer closed keeps the messages the peer already
// delivered receivable, in order, even when another pipe attaches while
// they are still buffered.
func TestFairQueue_DetachClosedDirectPipeDrains(t *testing.T) {
	t.Parallel()
	fq := newFQ()
	defer fq.Close()
	ctx := testutil.Ctx(t)

	a, b := pipe.NewPair(8, 8, nil, nil)
	defer closePipes(a, b)
	require.NoError(t, fq.Attach(a), "attach")
	const n = 5
	for i := range n {
		require.NoError(t, b.Send(ctx, msg.FromBytes([]byte{byte(i)})), "peer send %d", i)
	}
	require.NoError(t, b.Close())
	require.NoError(t, fq.Detach(a), "detach")
	require.NoError(t, a.Close()) // the owner releases its end after detach

	c, d := pipe.NewPair(8, 8, nil, nil)
	defer closePipes(c, d)
	require.NoError(t, fq.Attach(c), "attach replacement")
	require.NoError(t, d.Send(ctx, msg.FromBytes([]byte{0xff})), "replacement send")

	var fromA []byte
	gotC := false
	for range n + 1 {
		m, src, err := fq.RecvWithSource(ctx)
		require.NoError(t, err)
		if src == c {
			gotC = true
			continue
		}
		fromA = append(fromA, m.Frames()[0][0])
	}
	require.True(t, gotC, "replacement pipe message")
	require.Equal(t, []byte{0, 1, 2, 3, 4}, fromA, "drained messages in order")
}

// TestFairQueue_SingleRecvWaitsOutClosedPipe: a receiver that drains the
// sole pipe after its peer closed stays parked (no error, no spurious
// message) until a replacement attaches, then receives from it.
func TestFairQueue_SingleRecvWaitsOutClosedPipe(t *testing.T) {
	t.Parallel()
	fq := newFQ()
	defer fq.Close()
	ctx := testutil.Ctx(t)

	a, b := pipe.NewPair(8, 8, nil, nil)
	defer closePipes(a, b)
	require.NoError(t, fq.Attach(a), "attach")
	require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("last"))))
	require.NoError(t, b.Close())

	m, err := fq.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, "last", string(m.Bytes()), "backlog survives the close")

	c, d := pipe.NewPair(8, 8, nil, nil)
	defer closePipes(c, d)
	// Recv stays parked on the closed, empty, still-attached pipe until a
	// replacement is attached and delivers.
	got := testutil.RequireUnblocks(t, func() string {
		m, err := fq.Recv(ctx)
		if err != nil {
			return "error: " + err.Error()
		}
		return string(m.Bytes())
	}, func() {
		require.NoError(t, fq.Detach(a), "detach")
		require.NoError(t, fq.Attach(c), "attach replacement")
		require.NoError(t, d.Send(ctx, msg.FromBytes([]byte("next"))))
	})
	require.Equal(t, "next", got)
}

// ---------------------- shared sanity ----------------------

func TestLoadBalancer_DetachAdvancesCursor(t *testing.T) {
	t.Parallel()
	lb := newLB()
	defer lb.Close()

	const n = 3
	engineEnds, peerEnds := makePairs(n, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)
	for _, p := range engineEnds {
		_ = lb.Attach(p)
	}

	ctx := testutil.Ctx(t)

	// First send should hit peer 0.
	require.NoError(t, lb.Send(ctx, msg.FromBytes([]byte{0})), "send")
	// Detach peer 0 (cursor should now point at what was index 1).
	_ = lb.Detach(engineEnds[0])

	// Drain the message peer 0 already received.
	_, err := peerEnds[0].RecvOr(ctx, nil)
	require.NoError(t, err, "drain peer 0")

	// Two more sends should land on the surviving peers, one each.
	for i := 0; i < 2; i++ {
		require.NoError(t, lb.Send(ctx, msg.FromBytes([]byte{byte(i + 1)})), "send %d", i)
	}
	for i := 1; i < n; i++ {
		_, err := peerEnds[i].RecvOr(ctx, nil)
		require.NoError(t, err, "peer %d recv", i)
	}
}

func TestLoadBalancer_AttachAfterClose(t *testing.T) {
	t.Parallel()
	lb := newLB()
	require.NoError(t, lb.Close(), "close")
	a, b := pipe.NewPair(1, 1, nil, nil)
	defer closePipes(a, b)
	assert.ErrorIs(t, lb.Attach(a), zmqerr.ErrSocketClosed)
}

func TestFairQueue_AttachAfterClose(t *testing.T) {
	t.Parallel()
	fq := newFQ()
	require.NoError(t, fq.Close(), "close")
	a, b := pipe.NewPair(1, 1, nil, nil)
	defer closePipes(a, b)
	assert.ErrorIs(t, fq.Attach(a), zmqerr.ErrSocketClosed)
}

// TestLoadBalancer_TwoParkedSendersWakeOnAttach is the lost-wakeup
// regression for the old cap-1 notify token: with two goroutines parked
// in Send (no peers), a single Attach must wake BOTH (the notify channel
// is closed — broadcast — not a token one sender can swallow).
func TestLoadBalancer_TwoParkedSendersWakeOnAttach(t *testing.T) {
	t.Parallel()
	lb := newLB()
	defer lb.Close()

	ctx := testutil.Ctx(t)
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { done <- lb.Send(ctx, msg.FromBytes([]byte{byte(i)})) }()
	}
	time.Sleep(50 * time.Millisecond) // both park on the no-peer wait

	a, b := pipe.NewPair(2, 2, nil, nil)
	defer closePipes(a, b)
	require.NoError(t, lb.Attach(a), "attach")

	for i := 0; i < 2; i++ {
		err := testutil.RequireRecv(t, done, time.Second, "a parked sender never woke after Attach, sender %d", i)
		require.NoError(t, err, "parked send %d", i)
	}
}

// TestLoadBalancer_RotatesPastFullPeer pins libzmq lb_t parity: a peer at
// HWM is skipped, not head-of-line blocked on — the send proceeds to the
// next free peer immediately.
func TestLoadBalancer_RotatesPastFullPeer(t *testing.T) {
	t.Parallel()
	lb := newLB()
	defer lb.Close()

	full, fullPeer := pipe.NewPair(1, 1, nil, nil)
	free, freePeer := pipe.NewPair(4, 4, nil, nil)
	defer closePipes(full, fullPeer, free, freePeer)
	require.NoError(t, lb.Attach(full), "attach full")
	require.NoError(t, lb.Attach(free), "attach free")

	ctx := testutil.Ctx(t)
	// Saturate the first peer (the cursor starts there).
	require.NoError(t, full.Send(ctx, msg.FromBytes([]byte("fill"))), "fill")

	start := time.Now()
	require.NoError(t, lb.Send(ctx, msg.FromBytes([]byte("x"))), "send")
	require.Less(t, time.Since(start), 500*time.Millisecond, "send head-of-line blocked on the full peer")

	got, err := freePeer.RecvOr(ctx, nil)
	require.NoError(t, err, "free peer recv")
	require.Equal(t, "x", string(got.Bytes()), "message must have rotated to the free peer")
}

// TestFairQueue_PromotionPreservesPerPeerFIFO stresses the 1→2 transition:
// messages from the original pipe must stay in order even while a second
// pipe attaches mid-stream and receivers move from the single-pipe fast
// path to the multi-pipe walk.
func TestFairQueue_PromotionPreservesPerPeerFIFO(t *testing.T) {
	t.Parallel()
	fq := newFQ()
	defer fq.Close()

	a, aPeer := pipe.NewPair(8, 8, nil, nil)
	c, cPeer := pipe.NewPair(8, 8, nil, nil)
	defer closePipes(a, aPeer, c, cPeer)
	require.NoError(t, fq.Attach(a), "attach a (single)")

	ctx := testutil.Ctx(t)
	const n = 500
	go func() {
		for i := 0; i < n; i++ {
			if err := aPeer.Send(ctx, msg.FromBytes([]byte{byte(i >> 8), byte(i)})); err != nil {
				return
			}
		}
	}()
	go func() {
		time.Sleep(time.Millisecond) // land mid-stream
		_ = fq.Attach(c)             // 1→2 while recvs are in flight
	}()

	next := 0
	for next < n {
		m, err := fq.Recv(ctx)
		require.NoError(t, err, "recv %d", next)
		got := int(m.Bytes()[0])<<8 | int(m.Bytes()[1])
		require.Equal(t, next, got, "per-peer FIFO inverted across promotion")
		next++
	}
}
