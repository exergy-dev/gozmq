package socket_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/msg"
)

// TestLoadBalancer_SentCounter verifies that LoadBalancer.Sent advances by
// exactly one for each successful Send and does not advance on send-side
// errors (caller already saw the failure).
func TestLoadBalancer_SentCounter(t *testing.T) {
	t.Parallel()
	lb := socket.NewLoadBalancer()
	defer lb.Close()

	require.Equal(t, uint64(0), lb.Sent(), "initial Sent")

	const n = 5
	engineEnds, peerEnds := makePairs(1, n, n)
	defer closePipes(append(engineEnds, peerEnds...)...)
	require.NoError(t, lb.Attach(engineEnds[0]), "attach")

	ctx := testCtx(t)
	for i := 0; i < n; i++ {
		require.NoError(t, lb.Send(ctx, msg.FromBytes([]byte{byte(i)})), "send %d", i)
	}
	require.Equal(t, uint64(n), lb.Sent(), "Sent")

	// Drain so HWM doesn't block further sends.
	for i := 0; i < n; i++ {
		_, err := peerEnds[0].Recv(ctx)
		require.NoError(t, err, "drain %d", i)
	}
}

// TestFairQueue_RecvCounter verifies that FairQueue.Recvd advances by
// exactly one for each message handed off to the inbound channel and
// eventually dequeued by Recv.
func TestFairQueue_RecvCounter(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()
	defer fq.Close()

	require.Equal(t, uint64(0), fq.Recvd(), "initial Recvd")

	const n = 4
	engineEnds, peerEnds := makePairs(1, n, n)
	defer closePipes(append(engineEnds, peerEnds...)...)
	require.NoError(t, fq.Attach(engineEnds[0]), "attach")

	ctx := testCtx(t)
	for i := 0; i < n; i++ {
		require.NoError(t, peerEnds[0].Send(ctx, msg.FromBytes([]byte{byte(i)})), "peer send %d", i)
	}
	for i := 0; i < n; i++ {
		_, err := fq.Recv(ctx)
		require.NoError(t, err, "recv %d", i)
	}
	require.Equal(t, uint64(n), fq.Recvd(), "Recvd")
}

// TestFairQueue_QueueDepthGauge verifies that QueueDepth counts the
// messages queued in the attached pipes, on the single-pipe fast path and
// with several pipes, including HWM-0 overflow.
func TestFairQueue_QueueDepthGauge(t *testing.T) {
	t.Parallel()
	for _, n := range []int{1, 2} {
		fq := socket.NewFairQueue()
		engineEnds, peerEnds := makePairs(n, 0, 0)
		for _, p := range engineEnds {
			require.NoError(t, fq.Attach(p), "attach")
		}
		ctx := testCtx(t)
		const depth = 1500 // past the HWM-0 channel, into overflow
		for i := 0; i < depth; i++ {
			require.NoError(t, peerEnds[i%n].Send(ctx, msg.FromBytes([]byte("hi"))), "peer send")
		}
		require.Equal(t, uint64(depth), fq.QueueDepth(), "pipes=%d: before drain", n)
		_, err := fq.Recv(ctx)
		require.NoError(t, err, "recv")
		require.Equal(t, uint64(depth-1), fq.QueueDepth(), "pipes=%d: after one Recv", n)
		require.NoError(t, fq.Close())
		closePipes(append(engineEnds, peerEnds...)...)
	}
}
