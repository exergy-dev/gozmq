package socket_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/pipe"
	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/rkm/gozmq/socket"
)

// TestReq_PrependsEmptyDelimiter asserts that REQ.Send wraps the caller's
// body with a leading empty delimiter frame.
func TestReq_PrependsEmptyDelimiter(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := eng.Send(ctx, msg.FromBytes([]byte("hello"))); err != nil {
		t.Fatalf("send: %v", err)
	}
	got, err := b.Recv(ctx)
	if err != nil {
		t.Fatalf("peer recv: %v", err)
	}
	if got.NumFrames() != 2 {
		t.Fatalf("frames = %d, want 2 (empty + body)", got.NumFrames())
	}
	if len(got.Frames()[0]) != 0 {
		t.Fatalf("leading frame = %q, want empty delimiter", got.Frames()[0])
	}
	if !bytes.Equal(got.Frames()[1], []byte("hello")) {
		t.Fatalf("body = %q, want hello", got.Frames()[1])
	}
}

// TestReq_StripsEmptyDelimiterOnRecv asserts that REQ.Recv strips the
// leading empty frame the REP peer prepends to its reply.
func TestReq_StripsEmptyDelimiterOnRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// First send so the FSM moves to canRecv and a sticky forwarder launches.
	if err := eng.Send(ctx, msg.FromBytes([]byte("q"))); err != nil {
		t.Fatalf("send: %v", err)
	}
	// Drain the request from the peer side so we keep pipe clean.
	if _, err := b.Recv(ctx); err != nil {
		t.Fatalf("drain request: %v", err)
	}

	// Peer-side REP-shape reply: [empty, "reply"].
	reply := msg.FromFrames([]byte{}, []byte("reply"))
	if err := b.Send(ctx, reply); err != nil {
		t.Fatalf("peer send: %v", err)
	}
	got, err := eng.Recv(ctx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if got.NumFrames() != 1 {
		t.Fatalf("frames = %d, want 1", got.NumFrames())
	}
	if !bytes.Equal(got.Bytes(), []byte("reply")) {
		t.Fatalf("body = %q, want reply", got.Bytes())
	}
}

// TestReq_DoubleSendFSMViolation asserts that calling Send twice
// without a matching Recv returns ErrFSMViolation.
func TestReq_DoubleSendFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := eng.Send(ctx, msg.FromBytes([]byte("1"))); err != nil {
		t.Fatalf("first send: %v", err)
	}
	err := eng.Send(ctx, msg.FromBytes([]byte("2")))
	if !errors.Is(err, zmqerr.ErrFSMViolation) {
		t.Fatalf("second send err = %v, want ErrFSMViolation", err)
	}
}

// TestReq_RecvBeforeSendFSMViolation asserts that Recv without a prior
// Send returns ErrFSMViolation.
func TestReq_RecvBeforeSendFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	_, err := eng.Recv(context.Background())
	if !errors.Is(err, zmqerr.ErrFSMViolation) {
		t.Fatalf("recv err = %v, want ErrFSMViolation", err)
	}
}

// TestReq_DoubleRecvFSMViolation asserts that a second Recv after one
// successful round-trip, without an intervening Send, returns FSM
// violation.
func TestReq_DoubleRecvFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := eng.Send(ctx, msg.FromBytes([]byte("q"))); err != nil {
		t.Fatalf("send: %v", err)
	}
	// Drain request; peer replies.
	if _, err := b.Recv(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if err := b.Send(ctx, msg.FromFrames([]byte{}, []byte("a"))); err != nil {
		t.Fatalf("peer send: %v", err)
	}
	if _, err := eng.Recv(ctx); err != nil {
		t.Fatalf("first recv: %v", err)
	}
	// A second Recv with no Send between must fail FSM.
	_, err := eng.Recv(ctx)
	if !errors.Is(err, zmqerr.ErrFSMViolation) {
		t.Fatalf("second recv err = %v, want ErrFSMViolation", err)
	}
}

// TestReq_Rep_RoundTrip_Direct wires two engines (REQ + REP) directly via
// a pipe pair (no session layer) and asserts a request/reply alternation
// survives multiple rounds.
func TestReq_Rep_RoundTrip_Direct(t *testing.T) {
	t.Parallel()
	reqEng := socket.NewReqEngine()
	repEng := socket.NewRepEngine()
	defer reqEng.Close()
	defer repEng.Close()

	// Pipe pair: one end attached to REQ, the other to REP.
	reqSide, repSide := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(reqSide, repSide)
	if err := reqEng.AttachPipe(reqSide); err != nil {
		t.Fatalf("req attach: %v", err)
	}
	if err := repEng.AttachPipe(repSide); err != nil {
		t.Fatalf("rep attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const rounds = 4
	for i := 0; i < rounds; i++ {
		body := []byte{byte('A' + i)}
		// REQ → REP
		errCh := make(chan error, 1)
		go func() {
			errCh <- reqEng.Send(ctx, msg.FromBytes(body))
		}()

		got, err := repEng.Recv(ctx)
		if err != nil {
			t.Fatalf("rep recv %d: %v", i, err)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("req send %d: %v", i, err)
		}
		if got.NumFrames() != 1 || !bytes.Equal(got.Bytes(), body) {
			t.Fatalf("rep got %q frames=%d", got.Frames(), got.NumFrames())
		}

		// REP → REQ
		reply := append([]byte("r:"), body...)
		if err := repEng.Send(ctx, msg.FromBytes(reply)); err != nil {
			t.Fatalf("rep send %d: %v", i, err)
		}
		gotReply, err := reqEng.Recv(ctx)
		if err != nil {
			t.Fatalf("req recv %d: %v", i, err)
		}
		if gotReply.NumFrames() != 1 || !bytes.Equal(gotReply.Bytes(), reply) {
			t.Fatalf("req got %q frames=%d", gotReply.Frames(), gotReply.NumFrames())
		}
	}
}

// TestReq_Close_UnblocksRecv asserts Close unblocks a parked Recv.
func TestReq_Close_UnblocksRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx := context.Background()
	// Send to move to canRecv, then drain so the peer side is idle.
	if err := eng.Send(ctx, msg.FromBytes([]byte("q"))); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := b.Recv(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}

	recvDone := make(chan error, 1)
	go func() {
		_, err := eng.Recv(ctx)
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

// TestReq_Send_WaitsForPeer asserts that Send parks when no peer is
// attached and wakes when one arrives.
func TestReq_Send_WaitsForPeer(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- eng.Send(ctx, msg.FromBytes([]byte("q")))
	}()
	select {
	case err := <-sendDone:
		t.Fatalf("send returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}
	select {
	case err := <-sendDone:
		if err != nil {
			t.Fatalf("send err: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("send did not unblock on attach")
	}
	// Drain the request so the pipe doesn't hold a message at close.
	if _, err := b.Recv(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
}
