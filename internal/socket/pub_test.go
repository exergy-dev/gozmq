package socket_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
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

// drainPeer returns every message that arrives on p until none has for
// 100ms or ctx fires.
func drainPeer(ctx context.Context, p *pipe.End) []*msg.Msg {
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

	mustAttach(t, eng, engineEnds...)

	ctx := testCtx(t)

	// Subscribe peer 0 to "A", peer 1 to "B", peer 2 to "" (everything).
	require.NoError(t, peerEnds[0].Send(ctx, subscribeMsg(true, []byte("A"))), "peer0 subscribe")
	require.NoError(t, peerEnds[1].Send(ctx, subscribeMsg(true, []byte("B"))), "peer1 subscribe")
	require.NoError(t, peerEnds[2].Send(ctx, subscribeMsg(true, []byte{})), "peer2 subscribe")

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
				require.NoError(t, err, "pub probe")
			}
			rc, c := context.WithTimeout(ctx, 50*time.Millisecond)
			m, err := peerEnds[peerIdx].Recv(rc)
			c()
			if err == nil && m.NumFrames() == 2 && string(m.Frames()[0]) == topic {
				drainAll()
				return
			}
		}
		require.Failf(t, "subscription timeout", "peer %d sub for %q never took effect", peerIdx, topic)
	}
	waitForSub(0, "A")
	waitForSub(1, "B")
	waitForSub(2, "Z") // peer 2 subscribed to ""; any topic works.

	// Now do the real assertions. Send topic "B" and topic "X". Only
	// peer 1 should receive the B message; only peer 2 should receive
	// the X message. Peer 2 additionally receives the B (it subscribed
	// to everything). Peer 0 receives neither.
	require.NoError(t, eng.Send(ctx, msg.FromFrames([]byte("B"), []byte("bpayload"))), "pub send B")
	require.NoError(t, eng.Send(ctx, msg.FromFrames([]byte("X"), []byte("xpayload"))), "pub send X")

	// Allow fanout to settle before we start draining.
	time.Sleep(50 * time.Millisecond)
	drainCtx, dc := context.WithTimeout(ctx, time.Second)
	defer dc()

	peer0 := drainPeer(drainCtx, peerEnds[0])
	peer1 := drainPeer(drainCtx, peerEnds[1])
	peer2 := drainPeer(drainCtx, peerEnds[2])

	// Peer 0 subscribed only to A; after the real-assertion sends
	// (topics B and X), peer 0 should receive nothing.
	assert.Empty(t, peer0, "peer0 got unexpected messages")
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
	assert.Equal(t, 1, p1Counts["B"], "peer1 B count (all=%v)", p1Counts)
	assert.Equal(t, 0, p1Counts["A"], "peer1 got unsubscribed topic A: %v", p1Counts)
	assert.Equal(t, 0, p1Counts["X"], "peer1 got unsubscribed topic X: %v", p1Counts)
	p2Counts := countByTopic(peer2)
	// Peer 2 ("" subscription) should have received every topic the
	// pub sent while peer 2 was subscribed.
	assert.Equal(t, 1, p2Counts["B"], "peer2 B count (all=%v)", p2Counts)
	assert.Equal(t, 1, p2Counts["X"], "peer2 X count (all=%v)", p2Counts)
}

// TestPub_UnsubscribeStopsDelivery checks that a synthetic unsubscribe
// message removes the peer's filter and further sends skip that peer.
func TestPub_UnsubscribeStopsDelivery(t *testing.T) {
	t.Parallel()
	eng := socket.NewPubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testCtx(t)

	// Subscribe to A; poll until it lands.
	require.NoError(t, b.Send(ctx, subscribeMsg(true, []byte("A"))), "subscribe")
	deadline := time.Now().Add(time.Second)
	got := false
	for time.Now().Before(deadline) {
		if err := eng.Send(ctx, msg.FromFrames([]byte("A"), []byte("p"))); err != nil {
			require.NoError(t, err, "pub send")
		}
		rc, c := context.WithTimeout(ctx, 50*time.Millisecond)
		_, err := b.Recv(rc)
		c()
		if err == nil {
			got = true
			break
		}
	}
	require.True(t, got, "subscribe never took effect")
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
	require.NoError(t, b.Send(ctx, subscribeMsg(false, []byte("A"))), "unsubscribe")
	// Send and confirm nothing arrives.
	require.NoError(t, eng.Send(ctx, msg.FromFrames([]byte("A"), []byte("p"))), "pub send post-unsub")
	rc, c := context.WithTimeout(ctx, 100*time.Millisecond)
	defer c()
	_, err := b.Recv(rc)
	require.ErrorIs(t, err, context.DeadlineExceeded, "peer received after unsubscribe")
}

// TestPub_RecvIsFSMViolation: PUB is send-only.
func TestPub_RecvIsFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewPubEngine()
	defer eng.Close()
	_, err := eng.Recv(context.Background())
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation)
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
	mustAttach(t, eng, a)

	ctx := testCtx(t)
	require.NoError(t, b.Send(ctx, subscribeMsg(true, []byte{})), "subscribe all")

	// Rapid-fire 10 sends without draining. Send should return quickly
	// each time (no block) because of the non-blocking try-send.
	start := time.Now()
	for i := 0; i < 10; i++ {
		require.NoError(t, eng.Send(ctx, msg.FromFrames([]byte("t"), []byte("x"))), "pub send %d", i)
	}
	elapsed := time.Since(start)
	require.Less(t, elapsed, 500*time.Millisecond, "pub send appears to be blocking on HWM")
	// We should be able to drain at least one message (the buffered
	// one), but not necessarily all ten.
	drainCtx, dc := context.WithTimeout(ctx, time.Second)
	defer dc()
	_, err := b.Recv(drainCtx)
	require.NoError(t, err, "drain one")
}

// TestPub_NoGoroutinesPerSubscriber: PUB and XPUB apply subscriptions on
// the application's own calls, so a full subscriber lifecycle across 64
// peers (attach, subscribe, publish, detach, close) starts no goroutine.
// Not parallel: it counts the process's goroutines.
func TestPub_NoGoroutinesPerSubscriber(t *testing.T) {
	for _, tc := range []struct {
		name string
		eng  func() socket.Engine
	}{
		{"pub", socket.NewPubEngine},
		{"xpub", func() socket.Engine { return socket.NewXPubEngineV(false) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const n = 64
			before := runtime.NumGoroutine()
			eng := tc.eng()
			engineEnds, peerEnds := makePairs(n, 4, 4)
			defer closePipes(append(engineEnds, peerEnds...)...)
			mustAttach(t, eng, engineEnds...)

			ctx := testCtx(t)
			for _, p := range peerEnds {
				require.NoError(t, p.Send(ctx, subscribeMsg(true, []byte("t"))))
			}
			require.NoError(t, eng.Send(ctx, msg.FromFrames([]byte("t"), []byte("x"))))
			for i, p := range peerEnds {
				_, ok := p.TryRecv()
				require.True(t, ok, "peer %d", i)
			}
			require.LessOrEqual(t, runtime.NumGoroutine(), before, "goroutines with %d peers attached", n)

			for _, p := range engineEnds[:n/2] {
				require.NoError(t, eng.DetachPipe(p))
			}
			require.NoError(t, eng.Close())
			require.LessOrEqual(t, runtime.NumGoroutine(), before, "goroutines after detach and close")
		})
	}
}

// TestPub_SubscriptionAppliesAtNextSend: a SUBSCRIBE or CANCEL queued
// before a Send filters that very Send, with no wait in between.
func TestPub_SubscriptionAppliesAtNextSend(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		eng  func() socket.Engine
	}{
		{"pub", socket.NewPubEngine},
		{"xpub", func() socket.Engine { return socket.NewXPubEngineV(false) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng := tc.eng()
			defer eng.Close()
			// Two peers, so the FairQueue is off its single-pipe path.
			a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
			other, otherPeer := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
			defer closePipes(a, b, other, otherPeer)
			mustAttach(t, eng, a, other)
			ctx := testCtx(t)

			require.NoError(t, b.Send(ctx, subscribeMsg(true, []byte("A"))))
			require.NoError(t, eng.Send(ctx, msg.FromFrames([]byte("A"), []byte("1"))))
			require.NoError(t, eng.Send(ctx, msg.FromFrames([]byte("B"), []byte("2"))))
			m, ok := b.TryRecv()
			require.True(t, ok, "subscribed topic not delivered")
			require.Equal(t, "1", string(m.Frames()[1]))
			_, ok = b.TryRecv()
			require.False(t, ok, "unsubscribed topic delivered")

			require.NoError(t, b.Send(ctx, subscribeMsg(false, []byte("A"))))
			require.NoError(t, eng.Send(ctx, msg.FromFrames([]byte("A"), []byte("3"))))
			_, ok = b.TryRecv()
			require.False(t, ok, "delivered after CANCEL")
		})
	}
}

// TestPub_SlowSubscriberDoesNotDelayOthers: a peer at HWM is dropped
// immediately (libzmq PUB semantics) — it must not add a serial grace
// period that delays delivery to free subscribers.
func TestPub_SlowSubscriberDoesNotDelayOthers(t *testing.T) {
	t.Parallel()
	eng := socket.NewPubEngine()
	defer eng.Close()

	slow, slowPeer := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	fast, fastPeer := pipe.NewPair(64, 64, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(slow, slowPeer, fast, fastPeer)
	mustAttach(t, eng, slow, fast)

	ctx := testCtx(t)
	require.NoError(t, slowPeer.Send(ctx, subscribeMsg(true, []byte{})), "slow subscribe all")
	require.NoError(t, fastPeer.Send(ctx, subscribeMsg(true, []byte{})), "fast subscribe all")

	// Saturate the slow peer, then publish a burst: with immediate
	// drop-on-HWM the whole burst is non-blocking. The old 10ms-per-peer
	// fallback would stall ~10ms × messages here.
	const burst = 20
	start := time.Now()
	for i := 0; i < burst; i++ {
		require.NoError(t, eng.Send(ctx, msg.FromFrames([]byte("t"), []byte{byte(i)})), "send %d", i)
	}
	require.Less(t, time.Since(start), 100*time.Millisecond, "slow subscriber stalled the publisher")

	// The fast peer must have received the full burst.
	for i := 0; i < burst; i++ {
		got, err := fastPeer.Recv(ctx)
		require.NoError(t, err, "fast recv %d", i)
		require.Equal(t, byte(i), got.Frames()[1][0], "fast recv %d", i)
	}
}

// TestPub_FanoutSharesOneMessage: fanout hands the SAME *Msg value to
// every matching pipe (single-copy fanout) and leaves its frames intact.
func TestPub_FanoutSharesOneMessage(t *testing.T) {
	t.Parallel()
	eng := socket.NewPubEngine()
	defer eng.Close()

	a1, b1 := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	a2, b2 := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a1, b1, a2, b2)
	mustAttach(t, eng, a1, a2)

	ctx := testCtx(t)
	require.NoError(t, b1.Send(ctx, subscribeMsg(true, []byte{})), "peer1 subscribe")
	require.NoError(t, b2.Send(ctx, subscribeMsg(true, []byte{})), "peer2 subscribe")

	payload := make([]byte, 8*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	m := msg.FromFrames([]byte("t"), payload)
	require.NoError(t, eng.Send(ctx, m), "fanout send")

	for i, peer := range []*pipe.End{b1, b2} {
		got, err := peer.Recv(ctx)
		require.NoError(t, err, "peer%d recv", i+1)
		require.Same(t, m, got, "peer%d must receive the fanned-out message", i+1)
		require.Equal(t, payload, got.Frames()[1], "peer%d payload", i+1)
	}
}
