package gozmq_test

import (
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
)

// TestSocketMetrics_PushPullCounters drives a PUSH→PULL pair and checks
// that Sent on the pusher and Recv on the puller advance by the message
// count. Exercises the LoadBalancer.Sent / FairQueue.Recvd plumbing
// through the public Socket.Metrics surface.
func TestSocketMetrics_PushPullCounters(t *testing.T) {
	_, puller, pusher := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "inproc://metrics-pushpull", nil, nil)

	callCtx := testCtx(t)

	const n = 8
	for i := 0; i < n; i++ {
		if err := pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{byte(i)})); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		m, err := puller.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		m.Release()
	}

	if got := pusher.Metrics().Sent; got != n {
		t.Fatalf("pusher Sent = %d, want %d", got, n)
	}
	if got := puller.Metrics().Recv; got != n {
		t.Fatalf("puller Recv = %d, want %d", got, n)
	}
	if got := pusher.Metrics().Recv; got != 0 {
		t.Fatalf("pusher Recv = %d, want 0 (send-only)", got)
	}
}

// TestSocketMetrics_PubDropsOnSlowSubscriber confirms that a PUB whose
// subscriber never drains accumulates Dropped > 0 once the peer's HWM
// is full. Verifies the per-(message, peer) drop accounting at the PUB
// fanout site.
func TestSocketMetrics_PubDropsOnSlowSubscriber(t *testing.T) {
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

	callCtx := testCtx(t)
	const n = 200
	for i := 0; i < n; i++ {
		// Never drain on the SUB side. Once HWM is full the per-peer
		// try-send hits the pubTryTimeout deadline and the engine
		// increments Dropped.
		if err := pub.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{byte(i)})); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	m := pub.Metrics()
	if m.Sent != base+n {
		t.Fatalf("pub Sent = %d, want %d", m.Sent, base+n)
	}
	if m.Dropped == 0 {
		t.Fatalf("pub Dropped = 0, want > 0 (slow subscriber should overflow HWM=%d)", 2)
	}
}

// TestSocketMetrics_QueueDepthGauge confirms that QueueDepth flips to 1
// once a message is queued in one of the puller's inbound pipes and
// returns to 0 once Recv drains it, with two PUSH peers connected.
func TestSocketMetrics_QueueDepthGauge(t *testing.T) {
	ctxZmq, puller, pusher := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "inproc://metrics-depth", nil, nil)

	// Attach a second pusher: the FairQueue walks several pipes.
	pusher2, err := ctxZmq.NewSocket(gozmq.Push)
	if err != nil {
		t.Fatalf("new push2: %v", err)
	}
	defer pusher2.Close()
	if err := pusher2.Connect(puller.LastEndpoint()); err != nil {
		t.Fatalf("connect2: %v", err)
	}

	callCtx := testCtx(t)
	if err := pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("x"))); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Wait for the message to reach the puller's pipe.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if puller.Metrics().QueueDepth == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := puller.Metrics().QueueDepth; got != 1 {
		t.Fatalf("QueueDepth before drain = %d, want 1", got)
	}

	if m, err := puller.RecvMsg(callCtx); err != nil {
		t.Fatalf("recv: %v", err)
	} else {
		m.Release()
	}
	if got := puller.Metrics().QueueDepth; got != 0 {
		t.Fatalf("QueueDepth after drain = %d, want 0", got)
	}
}

// TestSocketMetrics_ClosedReturnsZero asserts that Metrics on a closed
// Socket returns the zero value rather than panicking.
func TestSocketMetrics_ClosedReturnsZero(t *testing.T) {
	_, s := newTestSocket(t, gozmq.Push)
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	m := s.Metrics()
	if m != (gozmq.SocketMetrics{}) {
		t.Fatalf("Metrics on closed socket = %+v, want zero", m)
	}
}
