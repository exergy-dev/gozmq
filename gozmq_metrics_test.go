package gozmq_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
)

// TestSocketMetrics_PushPullCounters drives a PUSH→PULL pair and checks
// that Sent on the pusher and Recv on the puller advance by the message
// count. Exercises the LoadBalancer.Sent / FairQueue.Recvd plumbing
// through the public Socket.Metrics surface.
func TestSocketMetrics_PushPullCounters(t *testing.T) {
	t.Parallel()
	_, puller, pusher := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "inproc://metrics-pushpull", nil, nil)

	callCtx := testutil.Ctx(t)

	const n = 8
	for i := 0; i < n; i++ {
		require.NoError(t, pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{byte(i)})), "send %d", i)
	}
	for i := 0; i < n; i++ {
		m, err := puller.RecvMsg(callCtx)
		require.NoError(t, err, "recv %d", i)
		m.Release()
	}

	require.EqualValues(t, n, pusher.Metrics().Sent, "pusher Sent")
	require.EqualValues(t, n, puller.Metrics().Recv, "puller Recv")
	require.Zero(t, pusher.Metrics().Recv, "pusher Recv (send-only)")
}

// TestSocketMetrics_PubDropsOnSlowSubscriber confirms that a PUB whose
// subscriber never drains accumulates Dropped > 0 once the peer's HWM
// is full. Verifies the per-(message, peer) drop accounting at the PUB
// fanout site.
func TestSocketMetrics_PubDropsOnSlowSubscriber(t *testing.T) {
	t.Parallel()
	// Small HWMs so a handful of sends are enough to overflow the
	// peer's inbound buffer. newPubSubPair waits for the SUBSCRIBE to
	// reach the PUB engine before returning; otherwise the early sends
	// would be filtered out (a different path that doesn't increment
	// Dropped).
	_, pub, _ := newPubSubPair(t, "inproc://metrics-pubdrop", [][]byte{nil}, // match everything
		[]gozmq.Option{gozmq.WithSendHWM(2)},
		[]gozmq.Option{gozmq.WithRecvHWM(2)},
	)

	// The subscription sync probes count toward Sent — measure the delta.
	base := pub.Metrics().Sent

	callCtx := testutil.Ctx(t)
	const n = 200
	for i := 0; i < n; i++ {
		// Never drain on the SUB side. Once HWM is full the per-peer
		// try-send hits the pubTryTimeout deadline and the engine
		// increments Dropped.
		require.NoError(t, pub.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{byte(i)})), "send %d", i)
	}

	m := pub.Metrics()
	require.Equal(t, base+n, m.Sent, "pub Sent")
	require.Positive(t, m.Dropped, "pub Dropped: slow subscriber should overflow HWM=%d", 2)
}

// TestSocketMetrics_QueueDepthGauge confirms that QueueDepth flips to 1
// once a message is queued in one of the puller's inbound pipes and
// returns to 0 once Recv drains it, with two PUSH peers connected.
func TestSocketMetrics_QueueDepthGauge(t *testing.T) {
	t.Parallel()
	ctxZmq, puller, pusher := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "inproc://metrics-depth", nil, nil)

	// Attach a second pusher: the FairQueue walks several pipes.
	pusher2, err := ctxZmq.NewSocket(gozmq.Push)
	require.NoError(t, err, "new push2")
	defer pusher2.Close()
	require.NoError(t, pusher2.Connect(puller.LastEndpoint()), "connect2")

	callCtx := testutil.Ctx(t)
	require.NoError(t, pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("x"))), "send")

	// Wait for the message to reach the puller's pipe.
	require.Eventually(t, func() bool { return puller.Metrics().QueueDepth == 1 },
		2*time.Second, 2*time.Millisecond, "QueueDepth before drain never reached 1")

	m, err := puller.RecvMsg(callCtx)
	require.NoError(t, err, "recv")
	m.Release()
	require.Zero(t, puller.Metrics().QueueDepth, "QueueDepth after drain")
}

// TestSocketMetrics_ClosedReturnsZero asserts that Metrics on a closed
// Socket returns the zero value rather than panicking.
func TestSocketMetrics_ClosedReturnsZero(t *testing.T) {
	t.Parallel()
	_, s := newTestSocket(t, gozmq.Push)
	require.NoError(t, s.Close(), "close")
	require.Zero(t, s.Metrics(), "Metrics on closed socket")
}
