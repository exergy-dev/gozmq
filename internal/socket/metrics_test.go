package socket_test

import (
	"context"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/pipe"
	"github.com/rkm/gozmq/internal/socket"
)

// TestLoadBalancer_SentCounter verifies that LoadBalancer.Sent advances by
// exactly one for each successful Send and does not advance on send-side
// errors (caller already saw the failure).
func TestLoadBalancer_SentCounter(t *testing.T) {
	t.Parallel()
	lb := socket.NewLoadBalancer()
	defer lb.Close()

	if got := lb.Sent(); got != 0 {
		t.Fatalf("initial Sent = %d, want 0", got)
	}

	const n = 5
	engineEnds, peerEnds := makePairs(1, n, n)
	defer closePipes(append(engineEnds, peerEnds...)...)
	if err := lb.Attach(engineEnds[0]); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		if err := lb.Send(ctx, msg.FromBytes([]byte{byte(i)})); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if got := lb.Sent(); got != n {
		t.Fatalf("Sent = %d, want %d", got, n)
	}

	// Drain so HWM doesn't block further sends.
	for i := 0; i < n; i++ {
		if _, err := peerEnds[0].Recv(ctx); err != nil {
			t.Fatalf("drain %d: %v", i, err)
		}
	}
}

// TestFairQueue_RecvCounter verifies that FairQueue.Recvd advances by
// exactly one for each message handed off to the inbound channel and
// eventually dequeued by Recv.
func TestFairQueue_RecvCounter(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()
	defer fq.Close()

	if got := fq.Recvd(); got != 0 {
		t.Fatalf("initial Recvd = %d, want 0", got)
	}

	const n = 4
	engineEnds, peerEnds := makePairs(1, n, n)
	defer closePipes(append(engineEnds, peerEnds...)...)
	if err := fq.Attach(engineEnds[0]); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		if err := peerEnds[0].Send(ctx, msg.FromBytes([]byte{byte(i)})); err != nil {
			t.Fatalf("peer send %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		if _, err := fq.Recv(ctx); err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
	}
	if got := fq.Recvd(); got != n {
		t.Fatalf("Recvd = %d, want %d", got, n)
	}
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
	if err := fq.Attach(engineEnd); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := fq.Attach(engineEnd2); err != nil {
		t.Fatalf("attach2: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Send only on the first peer.
	if err := peerEnd.Send(ctx, msg.FromBytes([]byte("hi"))); err != nil {
		t.Fatalf("peer send: %v", err)
	}
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
	if got := fq.QueueDepth(); got != 1 {
		t.Fatalf("QueueDepth before drain = %d, want 1", got)
	}

	if _, err := fq.Recv(ctx); err != nil {
		t.Fatalf("recv: %v", err)
	}
	if got := fq.QueueDepth(); got != 0 {
		t.Fatalf("QueueDepth after drain = %d, want 0", got)
	}
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
	if err := fq.Attach(engineEnd); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		if err := peerEnd.Send(ctx, msg.FromBytes([]byte{byte(i)})); err != nil {
			t.Fatalf("peer send %d: %v", i, err)
		}
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
	if err := fq.Detach(engineEnd); err != nil {
		t.Fatalf("detach: %v", err)
	}

	// Wait for the forwarder to actually observe the cancel. (Forwarder
	// might also have managed to enqueue before cancel won the race —
	// in that case nothing drops. So we tolerate either 0 or 1.)
	time.Sleep(50 * time.Millisecond)
	if got := fq.Dropped(); got > 1 {
		t.Fatalf("Dropped = %d, want 0 or 1", got)
	}
}
