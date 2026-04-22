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

// TestXSub_SendSubscribeViaMessage: XSUB's Send accepts a synthetic
// subscribe message and forwards it to every attached peer while also
// updating the local filter.
func TestXSub_SendSubscribeViaMessage(t *testing.T) {
	t.Parallel()
	eng, _ := socket.NewXSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := eng.Send(ctx, subscribeMsg(true, []byte("topic"))); err != nil {
		t.Fatalf("xsub send: %v", err)
	}
	// Peer should receive the synthetic SUBSCRIBE forwarded by XSUB.
	m, err := b.Recv(ctx)
	if err != nil {
		t.Fatalf("peer recv: %v", err)
	}
	body := m.Frames()[0]
	if body[0] != socket.SubscribeByte || string(body[1:]) != "topic" {
		t.Fatalf("peer got %v, want subscribe-topic", body)
	}

	// Now peer sends a matching data message; Recv surfaces it.
	if err := b.Send(ctx, msg.FromFrames([]byte("topical"), []byte("payload"))); err != nil {
		t.Fatalf("peer send: %v", err)
	}
	got, err := eng.Recv(ctx)
	if err != nil {
		t.Fatalf("xsub recv: %v", err)
	}
	if got.NumFrames() != 2 || string(got.Frames()[0]) != "topical" {
		t.Fatalf("xsub recv = %v", got.Frames())
	}
}

// TestXSub_SendDuplicateSubscribeForwardsTwice verifies that app-driven
// synthetic subscribe messages are always forwarded, even when the local
// filter already contains the prefix. Verbose XPUB peers rely on seeing each
// duplicate command.
func TestXSub_SendDuplicateSubscribeForwardsTwice(t *testing.T) {
	t.Parallel()
	eng, _ := socket.NewXSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sub := subscribeMsg(true, []byte("topic"))
	if err := eng.Send(ctx, sub); err != nil {
		t.Fatalf("xsub send #1: %v", err)
	}
	if err := eng.Send(ctx, sub); err != nil {
		t.Fatalf("xsub send #2: %v", err)
	}
	for i := 0; i < 2; i++ {
		m, err := b.Recv(ctx)
		if err != nil {
			t.Fatalf("peer recv #%d: %v", i+1, err)
		}
		body := m.Frames()[0]
		if body[0] != socket.SubscribeByte || string(body[1:]) != "topic" {
			t.Fatalf("peer got %v, want subscribe-topic", body)
		}
	}
}

// TestXSub_SendNonSubscribeRejected: XSUB Send must be a subscription
// command; anything else is an FSM violation.
func TestXSub_SendNonSubscribeRejected(t *testing.T) {
	t.Parallel()
	eng, _ := socket.NewXSubEngine()
	defer eng.Close()

	err := eng.Send(context.Background(), msg.FromBytes([]byte("not a sub msg")))
	if !errors.Is(err, zmqerr.ErrFSMViolation) {
		t.Fatalf("xsub send arbitrary = %v, want ErrFSMViolation", err)
	}
}

// TestXSub_RecvFiltersByLocalSubscription confirms XSUB still applies
// the local subscription filter on inbound data.
func TestXSub_RecvFiltersByLocalSubscription(t *testing.T) {
	t.Parallel()
	eng, _ := socket.NewXSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := eng.Send(ctx, subscribeMsg(true, []byte("ok"))); err != nil {
		t.Fatalf("xsub subscribe: %v", err)
	}
	// Drain the forwarded subscription so it doesn't collide with data.
	drainCtx, dc := context.WithTimeout(ctx, 100*time.Millisecond)
	_, _ = b.Recv(drainCtx)
	dc()

	if err := b.Send(ctx, msg.FromFrames([]byte("bad"), []byte("x"))); err != nil {
		t.Fatalf("peer send bad: %v", err)
	}
	if err := b.Send(ctx, msg.FromFrames([]byte("okay"), []byte("y"))); err != nil {
		t.Fatalf("peer send good: %v", err)
	}

	m, err := eng.Recv(ctx)
	if err != nil {
		t.Fatalf("xsub recv: %v", err)
	}
	if string(m.Frames()[0]) != "okay" {
		t.Fatalf("recv topic = %q, want okay", m.Frames()[0])
	}
}
