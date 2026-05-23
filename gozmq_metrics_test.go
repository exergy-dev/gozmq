package gozmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/rkm/gozmq"
)

// TestSocketMetrics_PushPullCounters drives a PUSH→PULL pair and checks
// that Sent on the pusher and Recv on the puller advance by the message
// count. Exercises the LoadBalancer.Sent / FairQueue.Recvd plumbing
// through the public Socket.Metrics surface.
func TestSocketMetrics_PushPullCounters(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	puller, err := ctxZmq.NewSocket(gozmq.Pull)
	if err != nil {
		t.Fatalf("new pull: %v", err)
	}
	defer puller.Close()
	if err := puller.Bind("inproc://metrics-pushpull"); err != nil {
		t.Fatalf("bind: %v", err)
	}

	pusher, err := ctxZmq.NewSocket(gozmq.Push)
	if err != nil {
		t.Fatalf("new push: %v", err)
	}
	defer pusher.Close()
	if err := pusher.Connect("inproc://metrics-pushpull"); err != nil {
		t.Fatalf("connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const n = 8
	for i := 0; i < n; i++ {
		if err := pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{byte(i)})); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		if _, err := puller.RecvMsg(callCtx); err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
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
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	// Small HWMs so a handful of sends are enough to overflow the
	// peer's inbound buffer.
	pub, err := ctxZmq.NewSocket(gozmq.Pub, gozmq.WithSendHWM(2))
	if err != nil {
		t.Fatalf("new pub: %v", err)
	}
	defer pub.Close()
	if err := pub.Bind("inproc://metrics-pubdrop"); err != nil {
		t.Fatalf("bind: %v", err)
	}

	sub, err := ctxZmq.NewSocket(gozmq.Sub, gozmq.WithRecvHWM(2))
	if err != nil {
		t.Fatalf("new sub: %v", err)
	}
	defer sub.Close()
	if err := sub.Subscribe(nil); err != nil { // match everything
		t.Fatalf("subscribe: %v", err)
	}
	if err := sub.Connect("inproc://metrics-pubdrop"); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Let the SUBSCRIBE propagate to the PUB engine before we publish;
	// otherwise the early sends are filtered out (which is a different
	// path that doesn't increment Dropped).
	time.Sleep(150 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
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
	if m.Sent != n {
		t.Fatalf("pub Sent = %d, want %d", m.Sent, n)
	}
	if m.Dropped == 0 {
		t.Fatalf("pub Dropped = 0, want > 0 (slow subscriber should overflow HWM=%d)", 2)
	}
}

// TestSocketMetrics_QueueDepthGauge confirms that QueueDepth flips to 1
// once a message is sitting in the puller's FairQueue inbound channel
// (capacity 1) and returns to 0 once Recv drains it.
func TestSocketMetrics_QueueDepthGauge(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	puller, err := ctxZmq.NewSocket(gozmq.Pull)
	if err != nil {
		t.Fatalf("new pull: %v", err)
	}
	defer puller.Close()
	if err := puller.Bind("inproc://metrics-depth"); err != nil {
		t.Fatalf("bind: %v", err)
	}

	pusher, err := ctxZmq.NewSocket(gozmq.Push)
	if err != nil {
		t.Fatalf("new push: %v", err)
	}
	defer pusher.Close()
	if err := pusher.Connect("inproc://metrics-depth"); err != nil {
		t.Fatalf("connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("x"))); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Wait for the puller's forwarder to land the message into the
	// inbound channel.
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

	if _, err := puller.RecvMsg(callCtx); err != nil {
		t.Fatalf("recv: %v", err)
	}
	if got := puller.Metrics().QueueDepth; got != 0 {
		t.Fatalf("QueueDepth after drain = %d, want 0", got)
	}
}

// TestSocketMetrics_ClosedReturnsZero asserts that Metrics on a closed
// Socket returns the zero value rather than panicking.
func TestSocketMetrics_ClosedReturnsZero(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	s, err := ctxZmq.NewSocket(gozmq.Push)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	m := s.Metrics()
	if m != (gozmq.SocketMetrics{}) {
		t.Fatalf("Metrics on closed socket = %+v, want zero", m)
	}
}
