package socket_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/pipe"
	"github.com/rkm/gozmq/internal/socket"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// TestSub_FilterDropsNonMatching: a SUB with one subscription drops
// inbound messages whose first frame doesn't have the prefix.
func TestSub_FilterDropsNonMatching(t *testing.T) {
	t.Parallel()
	eng, sub := socket.NewSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := sub.Subscribe([]byte("go")); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// Drain the forwarded subscription that Subscribe pushed onto the
	// peer pipe so it doesn't interfere with the test.
	drainCtx, dc := context.WithTimeout(ctx, 100*time.Millisecond)
	_, _ = b.Recv(drainCtx)
	dc()

	// Peer sends three messages: one matches, two don't.
	if err := b.Send(ctx, msg.FromFrames([]byte("rust"), []byte("x"))); err != nil {
		t.Fatalf("peer send rust: %v", err)
	}
	if err := b.Send(ctx, msg.FromFrames([]byte("gopher"), []byte("y"))); err != nil {
		t.Fatalf("peer send gopher: %v", err)
	}
	if err := b.Send(ctx, msg.FromFrames([]byte("python"), []byte("z"))); err != nil {
		t.Fatalf("peer send python: %v", err)
	}

	m, err := eng.Recv(ctx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if m.NumFrames() != 2 || string(m.Frames()[0]) != "gopher" {
		t.Fatalf("recv = %v, want gopher/y", m.Frames())
	}
	// The next Recv should not yield anything within a short window.
	recvCtx, rc := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer rc()
	_, err = eng.Recv(recvCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("recv err = %v, want DeadlineExceeded", err)
	}
}

// TestSub_EmptySubscriptionAcceptsEverything: an empty prefix matches
// all topics.
func TestSub_EmptySubscriptionAcceptsEverything(t *testing.T) {
	t.Parallel()
	eng, sub := socket.NewSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	if err := sub.Subscribe([]byte{}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Drain forwarded subscribe.
	drainCtx, dc := context.WithTimeout(ctx, 100*time.Millisecond)
	_, _ = b.Recv(drainCtx)
	dc()

	if err := b.Send(ctx, msg.FromFrames([]byte("anything"), []byte("payload"))); err != nil {
		t.Fatalf("peer send: %v", err)
	}
	m, err := eng.Recv(ctx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if string(m.Frames()[0]) != "anything" {
		t.Fatalf("topic = %q, want anything", m.Frames()[0])
	}
}

// TestSub_ForwardsSubscriptionToAttachedPeer: Subscribe forwards a
// synthetic SUBSCRIBE message to every current peer.
func TestSub_ForwardsSubscriptionToAttachedPeer(t *testing.T) {
	t.Parallel()
	eng, sub := socket.NewSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	if err := sub.Subscribe([]byte("topic")); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	m, err := b.Recv(ctx)
	if err != nil {
		t.Fatalf("peer recv subscribe: %v", err)
	}
	if m.NumFrames() != 1 {
		t.Fatalf("expected single-frame subscription msg; got %d frames", m.NumFrames())
	}
	body := m.Frames()[0]
	if len(body) != 1+len("topic") || body[0] != socket.SubscribeByte || string(body[1:]) != "topic" {
		t.Fatalf("subscription frame = %v, want [0x01, topic...]", body)
	}
}

// TestSub_ReplaysSubscriptionsOnAttach: when a peer attaches AFTER a
// Subscribe, all current subscriptions are replayed to that peer.
func TestSub_ReplaysSubscriptionsOnAttach(t *testing.T) {
	t.Parallel()
	eng, sub := socket.NewSubEngine()
	defer eng.Close()

	// Subscribe before any peer is attached.
	if err := sub.Subscribe([]byte("foo")); err != nil {
		t.Fatalf("subscribe foo: %v", err)
	}
	if err := sub.Subscribe([]byte("bar")); err != nil {
		t.Fatalf("subscribe bar: %v", err)
	}

	a, b := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		m, err := b.Recv(ctx)
		if err != nil {
			t.Fatalf("peer recv %d: %v", i, err)
		}
		body := m.Frames()[0]
		if body[0] != socket.SubscribeByte {
			t.Fatalf("expected subscribe byte; got 0x%02x", body[0])
		}
		got[string(body[1:])] = true
	}
	if !got["foo"] || !got["bar"] {
		t.Fatalf("replayed subs = %v, want foo+bar", got)
	}
}

// TestSub_RefcountedSubscribe: two Subscribe(foo) calls only forward
// one SUBSCRIBE, and require two Unsubscribes before CANCEL forwards.
func TestSub_RefcountedSubscribe(t *testing.T) {
	t.Parallel()
	eng, sub := socket.NewSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := sub.Subscribe([]byte("x")); err != nil {
		t.Fatalf("sub1: %v", err)
	}
	if err := sub.Subscribe([]byte("x")); err != nil {
		t.Fatalf("sub2: %v", err)
	}
	// Exactly one SUBSCRIBE should surface.
	m, err := b.Recv(ctx)
	if err != nil {
		t.Fatalf("recv sub: %v", err)
	}
	if m.Frames()[0][0] != socket.SubscribeByte || string(m.Frames()[0][1:]) != "x" {
		t.Fatalf("first subscribe frame = %v", m.Frames()[0])
	}
	// No second subscribe should be pending.
	shortCtx, sc := context.WithTimeout(ctx, 100*time.Millisecond)
	if _, err := b.Recv(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected no second SUBSCRIBE; got err=%v", err)
	}
	sc()

	// One Unsubscribe: still nothing (refcount still >0).
	if err := sub.Unsubscribe([]byte("x")); err != nil {
		t.Fatalf("unsub1: %v", err)
	}
	shortCtx, sc = context.WithTimeout(ctx, 100*time.Millisecond)
	if _, err := b.Recv(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected CANCEL after first unsubscribe; err=%v", err)
	}
	sc()

	// Second Unsubscribe: CANCEL should surface.
	if err := sub.Unsubscribe([]byte("x")); err != nil {
		t.Fatalf("unsub2: %v", err)
	}
	m, err = b.Recv(ctx)
	if err != nil {
		t.Fatalf("recv cancel: %v", err)
	}
	if m.Frames()[0][0] != socket.UnsubscribeByte || string(m.Frames()[0][1:]) != "x" {
		t.Fatalf("cancel frame = %v", m.Frames()[0])
	}
}

// TestSub_SendIsFSMViolation: SUB is recv-only.
func TestSub_SendIsFSMViolation(t *testing.T) {
	t.Parallel()
	eng, _ := socket.NewSubEngine()
	defer eng.Close()
	err := eng.Send(context.Background(), msg.FromBytes([]byte("x")))
	if !errors.Is(err, zmqerr.ErrFSMViolation) {
		t.Fatalf("send err = %v, want ErrFSMViolation", err)
	}
}

// TestSub_CloseUnblocksRecv.
func TestSub_CloseUnblocksRecv(t *testing.T) {
	t.Parallel()
	eng, _ := socket.NewSubEngine()

	done := make(chan error, 1)
	go func() {
		_, err := eng.Recv(context.Background())
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := eng.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, zmqerr.ErrSocketClosed) {
			t.Fatalf("recv err = %v, want ErrSocketClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("recv did not unblock on close")
	}
}
