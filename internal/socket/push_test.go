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

func TestPush_LoadBalances(t *testing.T) {
	t.Parallel()
	eng := socket.NewPushEngine()
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

func TestPush_RecvIsFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewPushEngine()
	defer eng.Close()
	_, err := eng.Recv(context.Background())
	if !errors.Is(err, zmqerr.ErrFSMViolation) {
		t.Fatalf("recv err = %v, want ErrFSMViolation", err)
	}
}

func TestPush_BlocksOnHWMUnblocksOnDrain(t *testing.T) {
	t.Parallel()
	eng := socket.NewPushEngine()
	defer eng.Close()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := eng.Send(ctx, msg.FromBytes([]byte("one"))); err != nil {
		t.Fatalf("first send: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Send(ctx, msg.FromBytes([]byte("two"))) }()
	select {
	case err := <-done:
		t.Fatalf("second send completed early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	drainCtx, dc := context.WithTimeout(context.Background(), time.Second)
	defer dc()
	if _, err := b.Recv(drainCtx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second send: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second send did not unblock")
	}
	if _, err := b.Recv(drainCtx); err != nil {
		t.Fatalf("drain two: %v", err)
	}
}

func TestPush_CloseUnblocksSend(t *testing.T) {
	t.Parallel()
	eng := socket.NewPushEngine()
	a, b := pipe.NewPair(0, 0, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Send(context.Background(), msg.FromBytes([]byte("x"))) }()
	time.Sleep(50 * time.Millisecond)
	if err := eng.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, zmqerr.ErrSocketClosed) {
			t.Fatalf("send err = %v, want ErrSocketClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("send did not unblock on close")
	}
}
