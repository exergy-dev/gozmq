package socket_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/pipe"
	"github.com/rkm/gozmq/internal/socket"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// subscribeMsg builds the single-frame synthetic subscribe/unsubscribe
// message that peers use to surface SUBSCRIBE/CANCEL to a PUB/XPUB
// engine. Mirrors socket.SubscribeByte / socket.UnsubscribeByte.
func subscribeMsg(subscribe bool, prefix []byte) *msg.Msg {
	buf := make([]byte, 1+len(prefix))
	if subscribe {
		buf[0] = socket.SubscribeByte
	} else {
		buf[0] = socket.UnsubscribeByte
	}
	copy(buf[1:], prefix)
	return msg.FromBytes(buf)
}

// drainPeer returns every non-subscription message available on p until
// ctx fires. Subscription frames (synthetic) are echoed back from the
// PUB's peer reader if we sent them; we want only the real data frames.
func drainPeer(ctx context.Context, p iface.Pipe) []*msg.Msg {
	var out []*msg.Msg
	for {
		recvCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		m, err := p.Recv(recvCtx)
		cancel()
		if err != nil {
			return out
		}
		out = append(out, m)
	}
}

// TestPub_FanoutByPrefix verifies that Send delivers a multi-frame
// message only to peers whose subscription prefix matches the first
// frame (the topic).
func TestPub_FanoutByPrefix(t *testing.T) {
	t.Parallel()
	eng := socket.NewPubEngine()
	defer eng.Close()

	const n = 3
	engineEnds, peerEnds := makePairs(n, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)

	for _, p := range engineEnds {
		if err := eng.AttachPipe(p); err != nil {
			t.Fatalf("attach: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Subscribe peer 0 to "A", peer 1 to "B", peer 2 to "" (everything).
	if err := peerEnds[0].Send(ctx, subscribeMsg(true, []byte("A"))); err != nil {
		t.Fatalf("peer0 subscribe: %v", err)
	}
	if err := peerEnds[1].Send(ctx, subscribeMsg(true, []byte("B"))); err != nil {
		t.Fatalf("peer1 subscribe: %v", err)
	}
	if err := peerEnds[2].Send(ctx, subscribeMsg(true, []byte{})); err != nil {
		t.Fatalf("peer2 subscribe: %v", err)
	}

	// Wait for each peer's subscription to land. A simple time-based
	// synchronization is flaky under load; instead we poll by sending
	// a probe message and draining every peer after each probe so no
	// peer's buffer fills with leftover probe traffic.
	drainAll := func() {
		for i := 0; i < n; i++ {
			for {
				rc, c := context.WithTimeout(ctx, 30*time.Millisecond)
				_, err := peerEnds[i].Recv(rc)
				c()
				if err != nil {
					break
				}
			}
		}
	}
	waitForSub := func(peerIdx int, topic string) {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if err := eng.Send(ctx, msg.FromFrames([]byte(topic), []byte("probe"))); err != nil {
				t.Fatalf("pub probe: %v", err)
			}
			rc, c := context.WithTimeout(ctx, 50*time.Millisecond)
			m, err := peerEnds[peerIdx].Recv(rc)
			c()
			if err == nil && m.NumFrames() == 2 && string(m.Frames()[0]) == topic {
				drainAll()
				return
			}
		}
		t.Fatalf("peer %d sub for %q never took effect", peerIdx, topic)
	}
	waitForSub(0, "A")
	waitForSub(1, "B")
	waitForSub(2, "Z") // peer 2 subscribed to ""; any topic works.

	// Now do the real assertions. Send topic "B" and topic "X". Only
	// peer 1 should receive the B message; only peer 2 should receive
	// the X message. Peer 2 additionally receives the B (it subscribed
	// to everything). Peer 0 receives neither.
	if err := eng.Send(ctx, msg.FromFrames([]byte("B"), []byte("bpayload"))); err != nil {
		t.Fatalf("pub send B: %v", err)
	}
	if err := eng.Send(ctx, msg.FromFrames([]byte("X"), []byte("xpayload"))); err != nil {
		t.Fatalf("pub send X: %v", err)
	}

	// Allow fanout to settle before we start draining.
	time.Sleep(50 * time.Millisecond)
	drainCtx, dc := context.WithTimeout(ctx, time.Second)
	defer dc()

	peer0 := drainPeer(drainCtx, peerEnds[0])
	peer1 := drainPeer(drainCtx, peerEnds[1])
	peer2 := drainPeer(drainCtx, peerEnds[2])

	// Peer 0 subscribed only to A; after the real-assertion sends
	// (topics B and X), peer 0 should receive nothing.
	if len(peer0) != 0 {
		t.Errorf("peer0 got %d unexpected messages (first topic=%q)", len(peer0), peer0[0].Frames()[0])
	}
	// Peer 1 should have exactly one "B" message and no "A"/"X".
	countByTopic := func(ms []*msg.Msg) map[string]int {
		counts := map[string]int{}
		for _, m := range ms {
			if m.NumFrames() == 0 {
				continue
			}
			counts[string(m.Frames()[0])]++
		}
		return counts
	}
	p1Counts := countByTopic(peer1)
	if p1Counts["B"] != 1 {
		t.Errorf("peer1 B count = %d, want 1 (all=%v)", p1Counts["B"], p1Counts)
	}
	if p1Counts["A"] != 0 || p1Counts["X"] != 0 {
		t.Errorf("peer1 got unsubscribed topics: %v", p1Counts)
	}
	p2Counts := countByTopic(peer2)
	// Peer 2 ("" subscription) should have received every topic the
	// pub sent while peer 2 was subscribed.
	if p2Counts["B"] != 1 {
		t.Errorf("peer2 B count = %d, want 1 (all=%v)", p2Counts["B"], p2Counts)
	}
	if p2Counts["X"] != 1 {
		t.Errorf("peer2 X count = %d, want 1 (all=%v)", p2Counts["X"], p2Counts)
	}
}

// TestPub_UnsubscribeStopsDelivery checks that a synthetic unsubscribe
// message removes the peer's filter and further sends skip that peer.
func TestPub_UnsubscribeStopsDelivery(t *testing.T) {
	t.Parallel()
	eng := socket.NewPubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Subscribe to A; poll until it lands.
	if err := b.Send(ctx, subscribeMsg(true, []byte("A"))); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	got := false
	for time.Now().Before(deadline) {
		if err := eng.Send(ctx, msg.FromFrames([]byte("A"), []byte("p"))); err != nil {
			t.Fatalf("pub send: %v", err)
		}
		rc, c := context.WithTimeout(ctx, 50*time.Millisecond)
		_, err := b.Recv(rc)
		c()
		if err == nil {
			got = true
			break
		}
	}
	if !got {
		t.Fatal("subscribe never took effect")
	}
	// Drain any extras from the polling loop.
	for {
		rc, c := context.WithTimeout(ctx, 50*time.Millisecond)
		_, err := b.Recv(rc)
		c()
		if err != nil {
			break
		}
	}

	// Unsubscribe.
	if err := b.Send(ctx, subscribeMsg(false, []byte("A"))); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	// Wait for the engine to process.
	time.Sleep(50 * time.Millisecond)
	// Send and confirm nothing arrives.
	if err := eng.Send(ctx, msg.FromFrames([]byte("A"), []byte("p"))); err != nil {
		t.Fatalf("pub send post-unsub: %v", err)
	}
	rc, c := context.WithTimeout(ctx, 100*time.Millisecond)
	defer c()
	if _, err := b.Recv(rc); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("peer received after unsubscribe: err=%v", err)
	}
}

// TestPub_RecvIsFSMViolation: PUB is send-only.
func TestPub_RecvIsFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewPubEngine()
	defer eng.Close()
	_, err := eng.Recv(context.Background())
	if !errors.Is(err, zmqerr.ErrFSMViolation) {
		t.Fatalf("recv err = %v, want ErrFSMViolation", err)
	}
}

// TestPub_HWMDropsRatherThanBlocks: a PUB send to a peer at HWM drops
// the message for that peer and returns without blocking.
func TestPub_HWMDropsRatherThanBlocks(t *testing.T) {
	t.Parallel()
	eng := socket.NewPubEngine()
	defer eng.Close()

	// HWM=1 on peer -> second send drops for that peer.
	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Send(ctx, subscribeMsg(true, []byte{})); err != nil {
		t.Fatalf("subscribe all: %v", err)
	}
	// Wait for subscription.
	time.Sleep(50 * time.Millisecond)

	// Rapid-fire 10 sends without draining. Send should return quickly
	// each time (no block) because of the non-blocking try-send.
	start := time.Now()
	for i := 0; i < 10; i++ {
		if err := eng.Send(ctx, msg.FromFrames([]byte("t"), []byte("x"))); err != nil {
			t.Fatalf("pub send %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("pub send appears to be blocking on HWM: took %v", elapsed)
	}
	// We should be able to drain at least one message (the buffered
	// one), but not necessarily all ten.
	drainCtx, dc := context.WithTimeout(ctx, time.Second)
	defer dc()
	_, err := b.Recv(drainCtx)
	if err != nil {
		t.Fatalf("drain one: %v", err)
	}
}

// TestPub_CloseReleasesPeerReaders ensures Close unblocks the per-peer
// reader goroutines and returns promptly.
func TestPub_CloseReleasesPeerReaders(t *testing.T) {
	t.Parallel()
	eng := socket.NewPubEngine()

	const n = 3
	engineEnds, peerEnds := makePairs(n, 2, 2)
	defer closePipes(append(engineEnds, peerEnds...)...)
	for _, p := range engineEnds {
		if err := eng.AttachPipe(p); err != nil {
			t.Fatalf("attach: %v", err)
		}
	}

	done := make(chan error, 1)
	go func() { done <- eng.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("close err: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2s")
	}
}

// TestPub_DetachReleasesPeerReader ensures DetachPipe drops the peer's
// reader goroutine promptly, independent of Close.
func TestPub_DetachReleasesPeerReader(t *testing.T) {
	t.Parallel()
	eng := socket.NewPubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(2, 2, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.DetachPipe(a) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("detach err: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Detach did not return within 2s")
	}
}
