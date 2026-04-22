package socket_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/pipe"
	"github.com/rkm/gozmq/internal/socket"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// TestRep_StripsEnvelopeOnRecv asserts that REP.Recv returns only the
// body (frames after the first empty delimiter).
func TestRep_StripsEnvelopeOnRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// REQ-style wire: [identityA, empty, body]
	wire := msg.FromFrames([]byte("identityA"), []byte{}, []byte("hello"))
	if err := b.Send(ctx, wire); err != nil {
		t.Fatalf("peer send: %v", err)
	}
	got, err := eng.Recv(ctx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if got.NumFrames() != 1 {
		t.Fatalf("frames = %d, want 1 (body only)", got.NumFrames())
	}
	if !bytes.Equal(got.Bytes(), []byte("hello")) {
		t.Fatalf("body = %q, want hello", got.Bytes())
	}
}

// TestRep_ReplayEnvelopeOnSend asserts that REP.Send prepends the stashed
// envelope back onto the outbound body.
func TestRep_ReplayEnvelopeOnSend(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Deliver a REQ-shape inbound frame.
	wire := msg.FromFrames([]byte("idA"), []byte("idB"), []byte{}, []byte("hi"))
	if err := b.Send(ctx, wire); err != nil {
		t.Fatalf("peer send: %v", err)
	}
	if _, err := eng.Recv(ctx); err != nil {
		t.Fatalf("recv: %v", err)
	}

	if err := eng.Send(ctx, msg.FromBytes([]byte("reply"))); err != nil {
		t.Fatalf("send: %v", err)
	}

	got, err := b.Recv(ctx)
	if err != nil {
		t.Fatalf("peer recv: %v", err)
	}
	// Expected outbound: [idA, idB, empty, reply]
	frames := got.Frames()
	if got.NumFrames() != 4 {
		t.Fatalf("frames = %d want 4: %q", got.NumFrames(), frames)
	}
	if !bytes.Equal(frames[0], []byte("idA")) {
		t.Fatalf("frame 0 = %q, want idA", frames[0])
	}
	if !bytes.Equal(frames[1], []byte("idB")) {
		t.Fatalf("frame 1 = %q, want idB", frames[1])
	}
	if len(frames[2]) != 0 {
		t.Fatalf("frame 2 = %q, want empty", frames[2])
	}
	if !bytes.Equal(frames[3], []byte("reply")) {
		t.Fatalf("frame 3 = %q, want reply", frames[3])
	}
}

// TestRep_DoubleRecvFSMViolation asserts that Recv twice without Send
// between returns ErrFSMViolation.
func TestRep_DoubleRecvFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	wire := msg.FromFrames([]byte{}, []byte("q"))
	if err := b.Send(ctx, wire); err != nil {
		t.Fatalf("peer send: %v", err)
	}
	if _, err := eng.Recv(ctx); err != nil {
		t.Fatalf("first recv: %v", err)
	}
	_, err := eng.Recv(ctx)
	if !errors.Is(err, zmqerr.ErrFSMViolation) {
		t.Fatalf("second recv err = %v, want ErrFSMViolation", err)
	}
}

// TestRep_SendBeforeRecvFSMViolation asserts that Send without a prior
// Recv returns ErrFSMViolation.
func TestRep_SendBeforeRecvFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
	defer eng.Close()
	err := eng.Send(context.Background(), msg.FromBytes([]byte("x")))
	if !errors.Is(err, zmqerr.ErrFSMViolation) {
		t.Fatalf("send err = %v, want ErrFSMViolation", err)
	}
}

// TestRep_DoubleSendFSMViolation asserts that Send twice after one Recv
// returns ErrFSMViolation on the second Send.
func TestRep_DoubleSendFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	wire := msg.FromFrames([]byte{}, []byte("q"))
	if err := b.Send(ctx, wire); err != nil {
		t.Fatalf("peer send: %v", err)
	}
	if _, err := eng.Recv(ctx); err != nil {
		t.Fatalf("recv: %v", err)
	}
	if err := eng.Send(ctx, msg.FromBytes([]byte("a"))); err != nil {
		t.Fatalf("first send: %v", err)
	}
	err := eng.Send(ctx, msg.FromBytes([]byte("a2")))
	if !errors.Is(err, zmqerr.ErrFSMViolation) {
		t.Fatalf("second send err = %v, want ErrFSMViolation", err)
	}
}

// TestRep_StickyPeer_AfterMultiplePeers asserts that REP always replies
// on the pipe that delivered the current request, even when multiple
// peers are attached.
func TestRep_StickyPeer_AfterMultiplePeers(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
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

	// Peer 1 sends a request.
	wire := msg.FromFrames([]byte{}, []byte("q"))
	if err := peerEnds[1].Send(ctx, wire); err != nil {
		t.Fatalf("peer1 send: %v", err)
	}
	if _, err := eng.Recv(ctx); err != nil {
		t.Fatalf("recv: %v", err)
	}
	if err := eng.Send(ctx, msg.FromBytes([]byte("a"))); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Only peer 1 should receive the reply.
	got, err := peerEnds[1].Recv(ctx)
	if err != nil {
		t.Fatalf("peer1 recv: %v", err)
	}
	if got.NumFrames() != 2 {
		t.Fatalf("frames = %d want 2", got.NumFrames())
	}
	if !bytes.Equal(got.Frames()[1], []byte("a")) {
		t.Fatalf("body = %q want a", got.Frames()[1])
	}
	// Others should be empty.
	for _, idx := range []int{0, 2} {
		ctx2, c := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, err := peerEnds[idx].Recv(ctx2)
		c()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("peer %d unexpectedly got reply (err=%v)", idx, err)
		}
	}
}

// TestRep_CloseUnblocksRecv asserts Close unblocks a parked Recv.
func TestRep_CloseUnblocksRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	recvDone := make(chan error, 1)
	go func() {
		_, err := eng.Recv(context.Background())
		recvDone <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := eng.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-recvDone:
		if !errors.Is(err, zmqerr.ErrSocketClosed) {
			t.Fatalf("recv err = %v, want ErrSocketClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("recv did not unblock on close")
	}
}
