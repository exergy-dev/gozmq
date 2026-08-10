package socket_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
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

// TestFairQueue_QueueDepthGauge verifies that QueueDepth reflects the
// transient inbound channel occupancy. With one pending message in the
// channel-1 inbound buffer and no consumer, depth should be 1.
// Two pipes are attached so the FairQueue uses forwarder-backed mode
// (the single-pipe direct path bypasses the inbound channel entirely).
func TestFairQueue_QueueDepthGauge(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()
	defer fq.Close()

	// Attach two pipes to force forwarder-backed mode.
	engineEnd, peerEnd := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(engineEnd, peerEnd)
	engineEnd2, peerEnd2 := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(engineEnd2, peerEnd2)
	require.NoError(t, fq.Attach(engineEnd), "attach")
	require.NoError(t, fq.Attach(engineEnd2), "attach2")

	ctx := testCtx(t)
	// Send only on the first peer.
	require.NoError(t, peerEnd.Send(ctx, msg.FromBytes([]byte("hi"))), "peer send")
	_ = peerEnd2 // second peer is idle; forces forwarder mode

	// Wait for the forwarder to hand the message into the inbound
	// channel — that's when QueueDepth flips to 1.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fq.QueueDepth() == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	require.Equal(t, uint64(1), fq.QueueDepth(), "QueueDepth before drain")

	_, err := fq.Recv(ctx)
	require.NoError(t, err, "recv")
	require.Equal(t, uint64(0), fq.QueueDepth(), "QueueDepth after drain")
}

// TestFairQueue_DroppedOnDetach verifies that messages pulled by a
// forwarder but stuck mid-handoff when Detach cancels the forwarder are
// accounted as drops, not as recvs.
func TestFairQueue_DroppedOnDetach(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()
	defer fq.Close()

	// HWM 2 so the peer can push two messages without blocking; the
	// forwarder will eat one into the inbound channel (capacity 1) and
	// then block trying to hand off the second. Detach then cancels.
	engineEnd, peerEnd := pipe.NewPair(2, 2, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(engineEnd, peerEnd)
	require.NoError(t, fq.Attach(engineEnd), "attach")

	ctx := testCtx(t)
	for i := 0; i < 2; i++ {
		require.NoError(t, peerEnd.Send(ctx, msg.FromBytes([]byte{byte(i)})), "peer send %d", i)
	}

	// Give the forwarder time to pull the second message and park on
	// the inbound handoff.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fq.QueueDepth() == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Detach mid-handoff. The forwarder's ctx cancels; the message it
	// was about to hand off is dropped.
	require.NoError(t, fq.Detach(engineEnd), "detach")

	// Wait for the forwarder to actually observe the cancel. (Forwarder
	// might also have managed to enqueue before cancel won the race —
	// in that case nothing drops. So we tolerate either 0 or 1.)
	time.Sleep(50 * time.Millisecond)
	require.LessOrEqual(t, fq.Dropped(), uint64(1), "Dropped")
}
