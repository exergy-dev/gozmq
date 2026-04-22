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

// helper: build N pipe pairs with given HWMs. Returns engine ends and
// peer ends. The caller is responsible for Close-ing both ends.
func makePairs(n, sendHWM, recvHWM int) (engineEnds, peerEnds []iface.Pipe) {
	engineEnds = make([]iface.Pipe, n)
	peerEnds = make([]iface.Pipe, n)
	for i := 0; i < n; i++ {
		a, b := pipe.NewPair(sendHWM, recvHWM, pipe.PeerInfo{}, pipe.PeerInfo{})
		engineEnds[i] = a
		peerEnds[i] = b
	}
	return
}

func closePipes(ps ...iface.Pipe) {
	for _, p := range ps {
		_ = p.Close()
	}
}

// ---------------------- LoadBalancer ----------------------

func TestLoadBalancer_RoundRobin(t *testing.T) {
	t.Parallel()
	lb := socket.NewLoadBalancer()
	defer lb.Close()

	const n = 3
	const send = 9
	engineEnds, peerEnds := makePairs(n, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)

	for _, p := range engineEnds {
		if err := lb.Attach(p); err != nil {
			t.Fatalf("attach: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < send; i++ {
		m := msg.FromBytes([]byte{byte(i)})
		if err := lb.Send(ctx, m); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	// Drain each peer end and assert each got exactly send/n messages.
	counts := make([]int, n)
	for i := 0; i < n; i++ {
		drainCtx, c := context.WithTimeout(context.Background(), time.Second)
		for {
			m, err := peerEnds[i].Recv(drainCtx)
			if err != nil {
				break
			}
			_ = m
			counts[i]++
			if counts[i] >= send/n {
				break
			}
		}
		c()
	}
	for i, c := range counts {
		if c != send/n {
			t.Fatalf("peer %d got %d messages, expected %d", i, c, send/n)
		}
	}
}

func TestLoadBalancer_BlocksOnHWMUnblocksOnDrain(t *testing.T) {
	t.Parallel()
	lb := socket.NewLoadBalancer()
	defer lb.Close()

	// Single peer, HWM=1. First send fits in the channel; second send
	// blocks until the peer drains.
	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := lb.Attach(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := lb.Send(ctx, msg.FromBytes([]byte("one"))); err != nil {
		t.Fatalf("first send: %v", err)
	}

	// Second send must block.
	sendDone := make(chan error, 1)
	go func() { sendDone <- lb.Send(ctx, msg.FromBytes([]byte("two"))) }()
	select {
	case err := <-sendDone:
		t.Fatalf("second send completed early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Drain peer. Drains "one"; then second send completes; then we drain "two".
	drainCtx, dc := context.WithTimeout(context.Background(), time.Second)
	defer dc()
	if _, err := b.Recv(drainCtx); err != nil {
		t.Fatalf("drain one: %v", err)
	}
	select {
	case err := <-sendDone:
		if err != nil {
			t.Fatalf("second send err: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second send did not unblock after peer drained")
	}
	if _, err := b.Recv(drainCtx); err != nil {
		t.Fatalf("drain two: %v", err)
	}
}

func TestLoadBalancer_CloseUnblocksSend(t *testing.T) {
	t.Parallel()
	lb := socket.NewLoadBalancer()

	a, b := pipe.NewPair(0, 0, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := lb.Attach(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	// HWM=0 → unbuffered; Send blocks until a Recv on the other side.
	ctx := context.Background()
	sendErr := make(chan error, 1)
	go func() { sendErr <- lb.Send(ctx, msg.FromBytes([]byte("x"))) }()

	time.Sleep(50 * time.Millisecond)
	if err := lb.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-sendErr:
		if !errors.Is(err, zmqerr.ErrSocketClosed) {
			t.Fatalf("send err = %v, want ErrSocketClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("send did not unblock on close")
	}
}

func TestLoadBalancer_CloseUnblocksWaitForPeer(t *testing.T) {
	t.Parallel()
	lb := socket.NewLoadBalancer()

	// No peers attached — Send should block waiting for an Attach.
	ctx := context.Background()
	sendErr := make(chan error, 1)
	go func() { sendErr <- lb.Send(ctx, msg.FromBytes([]byte("x"))) }()

	time.Sleep(50 * time.Millisecond)
	if err := lb.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-sendErr:
		if !errors.Is(err, zmqerr.ErrSocketClosed) {
			t.Fatalf("send err = %v, want ErrSocketClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("send did not unblock on close")
	}
}

func TestLoadBalancer_CtxCancelUnblocksSend(t *testing.T) {
	t.Parallel()
	lb := socket.NewLoadBalancer()
	defer lb.Close()

	a, b := pipe.NewPair(0, 0, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := lb.Attach(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sendErr := make(chan error, 1)
	go func() { sendErr <- lb.Send(ctx, msg.FromBytes([]byte("x"))) }()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-sendErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("send err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("send did not unblock on ctx cancel")
	}
}

// ---------------------- FairQueue ----------------------

func TestFairQueue_FairAcrossPipes(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()
	defer fq.Close()

	const n = 3
	engineEnds, peerEnds := makePairs(n, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)

	for _, p := range engineEnds {
		if err := fq.Attach(p); err != nil {
			t.Fatalf("attach: %v", err)
		}
	}

	// Pre-load each peer end with one distinct message.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	want := make(map[byte]bool)
	for i := 0; i < n; i++ {
		b := byte('a' + i)
		want[b] = true
		if err := peerEnds[i].Send(ctx, msg.FromBytes([]byte{b})); err != nil {
			t.Fatalf("peer %d send: %v", i, err)
		}
	}

	got := make(map[byte]bool)
	for i := 0; i < n; i++ {
		m, err := fq.Recv(ctx)
		if err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		if m.NumFrames() != 1 {
			t.Fatalf("recv %d: frames=%d", i, m.NumFrames())
		}
		got[m.Bytes()[0]] = true
	}
	for b := range want {
		if !got[b] {
			t.Fatalf("missing message %c (got %v)", b, got)
		}
	}
}

func TestFairQueue_RecvWakesOnAttach(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()
	defer fq.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	type result struct {
		m   *msg.Msg
		err error
	}
	done := make(chan result, 1)
	go func() {
		m, err := fq.Recv(ctx)
		done <- result{m: m, err: err}
	}()

	// Recv must block until we attach a pipe with a message available.
	select {
	case <-done:
		t.Fatal("recv returned before any pipe attached")
	case <-time.After(50 * time.Millisecond):
	}

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := b.Send(ctx, msg.FromBytes([]byte("hello"))); err != nil {
		t.Fatalf("peer send: %v", err)
	}
	if err := fq.Attach(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("recv: %v", r.err)
		}
		if string(r.m.Bytes()) != "hello" {
			t.Fatalf("recv body = %q, want %q", r.m.Bytes(), "hello")
		}
	case <-time.After(time.Second):
		t.Fatal("recv did not wake on attach")
	}
}

func TestFairQueue_DetachCancelsBlockedForwarder(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)

	if err := fq.Attach(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	if err := fq.Detach(a); err != nil {
		t.Fatalf("detach: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- fq.Close() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after Detach canceled forwarder")
	}
}

func TestFairQueue_CloseUnblocksRecv(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()

	ctx := context.Background()
	done := make(chan error, 1)
	go func() {
		_, err := fq.Recv(ctx)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := fq.Close(); err != nil {
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

func TestFairQueue_CloseStopsForwarders(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()

	const n = 4
	engineEnds, peerEnds := makePairs(n, 1, 1)
	defer closePipes(peerEnds...) // engine ends will be reaped via fq.Close

	for _, p := range engineEnds {
		if err := fq.Attach(p); err != nil {
			t.Fatalf("attach: %v", err)
		}
	}

	if err := fq.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Now close the engine ends explicitly (no-op cleanup; forwarders
	// already exited in fq.Close).
	closePipes(engineEnds...)
}

func TestFairQueue_DetachStopsForwarderOnPipeClose(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()
	defer fq.Close()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	if err := fq.Attach(a); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := fq.Detach(a); err != nil {
		t.Fatalf("detach: %v", err)
	}
	// Close the pipe so the forwarder (which may still be in Pipe.Recv)
	// exits via EOF on the next iteration. fq.Close in the deferred call
	// also drives this, but doing it here exercises the "pipe close
	// frees a parked forwarder" path.
	closePipes(a, b)
}

// ---------------------- shared sanity ----------------------

func TestLoadBalancer_DetachAdvancesCursor(t *testing.T) {
	t.Parallel()
	lb := socket.NewLoadBalancer()
	defer lb.Close()

	const n = 3
	engineEnds, peerEnds := makePairs(n, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)
	for _, p := range engineEnds {
		_ = lb.Attach(p)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// First send should hit peer 0.
	if err := lb.Send(ctx, msg.FromBytes([]byte{0})); err != nil {
		t.Fatalf("send: %v", err)
	}
	// Detach peer 0 (cursor should now point at what was index 1).
	_ = lb.Detach(engineEnds[0])

	// Drain the message peer 0 already received so its forwarder doesn't leak it.
	if _, err := peerEnds[0].Recv(ctx); err != nil {
		t.Fatalf("drain peer 0: %v", err)
	}

	// Two more sends should land on the surviving peers, one each.
	for i := 0; i < 2; i++ {
		if err := lb.Send(ctx, msg.FromBytes([]byte{byte(i + 1)})); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	for i := 1; i < n; i++ {
		if _, err := peerEnds[i].Recv(ctx); err != nil {
			t.Fatalf("peer %d recv: %v", i, err)
		}
	}
}

func TestLoadBalancer_AttachAfterClose(t *testing.T) {
	t.Parallel()
	lb := socket.NewLoadBalancer()
	if err := lb.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := lb.Attach(a); !errors.Is(err, zmqerr.ErrSocketClosed) {
		t.Fatalf("attach after close = %v, want ErrSocketClosed", err)
	}
}

func TestFairQueue_AttachAfterClose(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()
	if err := fq.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := fq.Attach(a); !errors.Is(err, zmqerr.ErrSocketClosed) {
		t.Fatalf("attach after close = %v, want ErrSocketClosed", err)
	}
}
