package socket_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/pipe"
	"github.com/rkm/gozmq/socket"
)

func TestPull_FairQueues(t *testing.T) {
	t.Parallel()
	eng := socket.NewPullEngine()
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
	want := make(map[byte]bool)
	for i := 0; i < n; i++ {
		body := byte('a' + i)
		want[body] = true
		if err := peerEnds[i].Send(ctx, msg.FromBytes([]byte{body})); err != nil {
			t.Fatalf("peer %d send: %v", i, err)
		}
	}
	got := make(map[byte]bool)
	for i := 0; i < n; i++ {
		m, err := eng.Recv(ctx)
		if err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		got[m.Bytes()[0]] = true
	}
	for b := range want {
		if !got[b] {
			t.Fatalf("missing message %c", b)
		}
	}
}

func TestPull_RecvWakesOnAttach(t *testing.T) {
	t.Parallel()
	eng := socket.NewPullEngine()
	defer eng.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type result struct {
		m   *msg.Msg
		err error
	}
	done := make(chan result, 1)
	go func() {
		m, err := eng.Recv(ctx)
		done <- result{m: m, err: err}
	}()
	select {
	case <-done:
		t.Fatal("recv returned before any pipe attached")
	case <-time.After(50 * time.Millisecond):
	}
	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := b.Send(ctx, msg.FromBytes([]byte("hi"))); err != nil {
		t.Fatalf("peer send: %v", err)
	}
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("recv: %v", r.err)
		}
		if string(r.m.Bytes()) != "hi" {
			t.Fatalf("recv body = %q", r.m.Bytes())
		}
	case <-time.After(time.Second):
		t.Fatal("recv did not wake on attach")
	}
}

func TestPull_SendIsFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewPullEngine()
	defer eng.Close()
	err := eng.Send(context.Background(), msg.FromBytes([]byte("x")))
	if !errors.Is(err, zmqerr.ErrFSMViolation) {
		t.Fatalf("send err = %v, want ErrFSMViolation", err)
	}
}

func TestPull_CloseUnblocksRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewPullEngine()
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
