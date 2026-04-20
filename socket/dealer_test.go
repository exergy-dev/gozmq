package socket_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/pipe"
	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/rkm/gozmq/socket"
)

// TestDealer_LoadBalancesOutbound asserts that DEALER.Send distributes
// messages round-robin across attached peer pipes (reusing the same
// semantics as PUSH). Sending send=9 messages across n=3 peers leaves
// each peer with exactly 3.
func TestDealer_LoadBalancesOutbound(t *testing.T) {
	t.Parallel()
	eng := socket.NewDealerEngine()
	defer eng.Close()

	const n = 3
	const send = 9
	engineEnds, peerEnds := makePairs(n, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)

	for _, p := range engineEnds {
		if err := eng.AttachPipe(p); err != nil {
			t.Fatalf("attach: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < send; i++ {
		if err := eng.Send(ctx, msg.FromBytes([]byte{byte(i)})); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	counts := make([]int, n)
	for i := 0; i < n; i++ {
		drainCtx, c := context.WithTimeout(context.Background(), time.Second)
		for counts[i] < send/n {
			_, err := peerEnds[i].Recv(drainCtx)
			if err != nil {
				break
			}
			counts[i]++
		}
		c()
	}
	for i, c := range counts {
		if c != send/n {
			t.Fatalf("peer %d got %d messages, expected %d", i, c, send/n)
		}
	}
}

// TestDealer_FairQueuesInbound asserts that DEALER.Recv fair-queues
// messages from every attached pipe.
func TestDealer_FairQueuesInbound(t *testing.T) {
	t.Parallel()
	eng := socket.NewDealerEngine()
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
	want := map[byte]bool{}
	for i := 0; i < n; i++ {
		b := byte('a' + i)
		want[b] = true
		if err := peerEnds[i].Send(ctx, msg.FromBytes([]byte{b})); err != nil {
			t.Fatalf("peer send: %v", err)
		}
	}
	got := map[byte]bool{}
	for i := 0; i < n; i++ {
		m, err := eng.Recv(ctx)
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		got[m.Bytes()[0]] = true
	}
	for b := range want {
		if !got[b] {
			t.Fatalf("missing %c", b)
		}
	}
}

// TestDealer_DoesNotMangleIdentity asserts that DEALER passes the message
// body through unchanged (no empty-delimiter insertion, no identity
// prepend). This is what differentiates DEALER from REQ.
func TestDealer_DoesNotMangleIdentity(t *testing.T) {
	t.Parallel()
	eng := socket.NewDealerEngine()
	defer eng.Close()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sent := msg.FromFrames([]byte("hello"), []byte("world"))
	if err := eng.Send(ctx, sent); err != nil {
		t.Fatalf("send: %v", err)
	}
	got, err := b.Recv(ctx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if got.NumFrames() != 2 {
		t.Fatalf("frames = %d, want 2 (no delimiter)", got.NumFrames())
	}
	if string(got.Frames()[0]) != "hello" || string(got.Frames()[1]) != "world" {
		t.Fatalf("frames = %q %q", got.Frames()[0], got.Frames()[1])
	}
}

// TestDealer_CloseUnblocksBoth asserts Close unblocks both pending Send
// and pending Recv with ErrSocketClosed.
func TestDealer_CloseUnblocksBoth(t *testing.T) {
	t.Parallel()
	eng := socket.NewDealerEngine()

	// Spawn a blocking Recv with no pipes attached.
	recvDone := make(chan error, 1)
	go func() {
		_, err := eng.Recv(context.Background())
		recvDone <- err
	}()

	// Spawn a blocking Send with no pipes attached (no peer → blocks
	// waiting for one).
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- eng.Send(context.Background(), msg.FromBytes([]byte("x")))
	}()

	time.Sleep(50 * time.Millisecond)
	if err := eng.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	for _, ch := range []chan error{recvDone, sendDone} {
		select {
		case err := <-ch:
			if !errors.Is(err, zmqerr.ErrSocketClosed) {
				t.Fatalf("err = %v, want ErrSocketClosed", err)
			}
		case <-time.After(time.Second):
			t.Fatal("did not unblock on close")
		}
	}
}

// TestDealer_AttachAfterCloseFails asserts attaching after Close yields
// ErrSocketClosed.
func TestDealer_AttachAfterCloseFails(t *testing.T) {
	t.Parallel()
	eng := socket.NewDealerEngine()
	if err := eng.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); !errors.Is(err, zmqerr.ErrSocketClosed) {
		t.Fatalf("attach after close = %v, want ErrSocketClosed", err)
	}
}
