package socket_test

import (
	"context"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/pipe"
	"github.com/rkm/gozmq/internal/socket"
	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helper: build N pipe pairs with given HWMs. Returns engine ends and
// peer ends. The caller is responsible for Close-ing both ends.
func makePairs(n, sendHWM, recvHWM int) (engineEnds, peerEnds []iface.Pipe) {
	engineEnds = make([]iface.Pipe, n)
	peerEnds = make([]iface.Pipe, n)
	for i := 0; i < n; i++ {
		a, b := pipe.NewPair(sendHWM, recvHWM, pipe.PeerInfo{}, pipe.PeerInfo{})
		engineEnds[i] = a
		peerEnds[i] = b
	}
	return
}

func closePipes(ps ...iface.Pipe) {
	for _, p := range ps {
		_ = p.Close()
	}
}

// ---------------------- LoadBalancer ----------------------

func TestLoadBalancer_RoundRobin(t *testing.T) {
	t.Parallel()
	lb := socket.NewLoadBalancer()
	defer lb.Close()

	const n = 3
	const send = 9
	engineEnds, peerEnds := makePairs(n, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)

	for _, p := range engineEnds {
		require.NoError(t, lb.Attach(p), "attach")
	}

	ctx := testCtx(t)
	for i := 0; i < send; i++ {
		m := msg.FromBytes([]byte{byte(i)})
		require.NoError(t, lb.Send(ctx, m), "send %d", i)
	}

	// Drain each peer end and assert each got exactly send/n messages.
	counts := make([]int, n)
	for i := 0; i < n; i++ {
		drainCtx, c := context.WithTimeout(context.Background(), time.Second)
		for {
			m, err := peerEnds[i].Recv(drainCtx)
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
	lb := socket.NewLoadBalancer()
	defer lb.Close()

	// Single peer, HWM=1. First send fits in the channel; second send
	// blocks until the peer drains.
	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	require.NoError(t, lb.Attach(a), "attach")

	ctx := testCtx(t)
	require.NoError(t, lb.Send(ctx, msg.FromBytes([]byte("one"))), "first send")

	// Second send must block.
	sendDone := make(chan error, 1)
	go func() { sendDone <- lb.Send(ctx, msg.FromBytes([]byte("two"))) }()
	select {
	case err := <-sendDone:
		require.Fail(t, "second send completed early", "%v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Drain peer. Drains "one"; then second send completes; then we drain "two".
	drainCtx, dc := context.WithTimeout(context.Background(), time.Second)
	defer dc()
	_, err := b.Recv(drainCtx)
	require.NoError(t, err, "drain one")
	select {
	case err := <-sendDone:
		require.NoError(t, err, "second send")
	case <-time.After(time.Second):
		require.Fail(t, "second send did not unblock after peer drained")
	}
	_, err = b.Recv(drainCtx)
	require.NoError(t, err, "drain two")
}

func TestLoadBalancer_CloseUnblocksSend(t *testing.T) {
	t.Parallel()
	lb := socket.NewLoadBalancer()

	a, b := pipe.NewPair(0, 0, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	require.NoError(t, lb.Attach(a), "attach")

	// HWM=0 → unbuffered; Send blocks until a Recv on the other side.
	ctx := context.Background()
	sendErr := make(chan error, 1)
	go func() { sendErr <- lb.Send(ctx, msg.FromBytes([]byte("x"))) }()

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, lb.Close(), "close")
	select {
	case err := <-sendErr:
		require.ErrorIs(t, err, zmqerr.ErrSocketClosed)
	case <-time.After(time.Second):
		require.Fail(t, "send did not unblock on close")
	}
}

func TestLoadBalancer_CloseUnblocksWaitForPeer(t *testing.T) {
	t.Parallel()
	lb := socket.NewLoadBalancer()

	// No peers attached — Send should block waiting for an Attach.
	ctx := context.Background()
	sendErr := make(chan error, 1)
	go func() { sendErr <- lb.Send(ctx, msg.FromBytes([]byte("x"))) }()

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, lb.Close(), "close")
	select {
	case err := <-sendErr:
		require.ErrorIs(t, err, zmqerr.ErrSocketClosed)
	case <-time.After(time.Second):
		require.Fail(t, "send did not unblock on close")
	}
}

func TestLoadBalancer_CtxCancelUnblocksSend(t *testing.T) {
	t.Parallel()
	lb := socket.NewLoadBalancer()
	defer lb.Close()

	a, b := pipe.NewPair(0, 0, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	require.NoError(t, lb.Attach(a), "attach")

	ctx, cancel := context.WithCancel(context.Background())
	sendErr := make(chan error, 1)
	go func() { sendErr <- lb.Send(ctx, msg.FromBytes([]byte("x"))) }()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-sendErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		require.Fail(t, "send did not unblock on ctx cancel")
	}
}

// ---------------------- FairQueue ----------------------

func TestFairQueue_FairAcrossPipes(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()
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
	fq := socket.NewFairQueue()
	defer fq.Close()

	ctx := testCtx(t)

	type result struct {
		m   *msg.Msg
		err error
	}
	done := make(chan result, 1)
	go func() {
		m, err := fq.Recv(ctx)
		done <- result{m: m, err: err}
	}()

	// Recv must block until we attach a pipe with a message available.
	select {
	case <-done:
		require.Fail(t, "recv returned before any pipe attached")
	case <-time.After(50 * time.Millisecond):
	}

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("hello"))), "peer send")
	require.NoError(t, fq.Attach(a), "attach")

	select {
	case r := <-done:
		require.NoError(t, r.err, "recv")
		require.Equal(t, "hello", string(r.m.Bytes()))
	case <-time.After(time.Second):
		require.Fail(t, "recv did not wake on attach")
	}
}

func TestFairQueue_DetachCancelsBlockedForwarder(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)

	require.NoError(t, fq.Attach(a), "attach")
	require.NoError(t, fq.Detach(a), "detach")

	done := make(chan error, 1)
	go func() { done <- fq.Close() }()

	select {
	case err := <-done:
		require.NoError(t, err, "close")
	case <-time.After(time.Second):
		require.Fail(t, "Close did not return after Detach canceled forwarder")
	}
}

func TestFairQueue_CloseUnblocksRecv(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()

	ctx := context.Background()
	done := make(chan error, 1)
	go func() {
		_, err := fq.Recv(ctx)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, fq.Close(), "close")
	select {
	case err := <-done:
		require.ErrorIs(t, err, zmqerr.ErrSocketClosed)
	case <-time.After(time.Second):
		require.Fail(t, "recv did not unblock on close")
	}
}

func TestFairQueue_CloseStopsForwarders(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()

	const n = 4
	engineEnds, peerEnds := makePairs(n, 1, 1)
	defer closePipes(peerEnds...) // engine ends will be reaped via fq.Close

	for _, p := range engineEnds {
		require.NoError(t, fq.Attach(p), "attach")
	}

	require.NoError(t, fq.Close(), "close")
	// Now close the engine ends explicitly (no-op cleanup; forwarders
	// already exited in fq.Close).
	closePipes(engineEnds...)
}

func TestFairQueue_DetachStopsForwarderOnPipeClose(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()
	defer fq.Close()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	require.NoError(t, fq.Attach(a), "attach")
	require.NoError(t, fq.Detach(a), "detach")
	// Close the pipe so the forwarder (which may still be in Pipe.Recv)
	// exits via EOF on the next iteration. fq.Close in the deferred call
	// also drives this, but doing it here exercises the "pipe close
	// frees a parked forwarder" path.
	closePipes(a, b)
}

// ---------------------- shared sanity ----------------------

func TestLoadBalancer_DetachAdvancesCursor(t *testing.T) {
	t.Parallel()
	lb := socket.NewLoadBalancer()
	defer lb.Close()

	const n = 3
	engineEnds, peerEnds := makePairs(n, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)
	for _, p := range engineEnds {
		_ = lb.Attach(p)
	}

	ctx := testCtx(t)

	// First send should hit peer 0.
	require.NoError(t, lb.Send(ctx, msg.FromBytes([]byte{0})), "send")
	// Detach peer 0 (cursor should now point at what was index 1).
	_ = lb.Detach(engineEnds[0])

	// Drain the message peer 0 already received so its forwarder doesn't leak it.
	_, err := peerEnds[0].Recv(ctx)
	require.NoError(t, err, "drain peer 0")

	// Two more sends should land on the surviving peers, one each.
	for i := 0; i < 2; i++ {
		require.NoError(t, lb.Send(ctx, msg.FromBytes([]byte{byte(i + 1)})), "send %d", i)
	}
	for i := 1; i < n; i++ {
		_, err := peerEnds[i].Recv(ctx)
		require.NoError(t, err, "peer %d recv", i)
	}
}

func TestLoadBalancer_AttachAfterClose(t *testing.T) {
	t.Parallel()
	lb := socket.NewLoadBalancer()
	require.NoError(t, lb.Close(), "close")
	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	assert.ErrorIs(t, lb.Attach(a), zmqerr.ErrSocketClosed)
}

func TestFairQueue_AttachAfterClose(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()
	require.NoError(t, fq.Close(), "close")
	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	assert.ErrorIs(t, fq.Attach(a), zmqerr.ErrSocketClosed)
}
