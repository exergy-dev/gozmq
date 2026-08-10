package socket_test

import (
	"context"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
	"github.com/stretchr/testify/require"
)

// TestReq_PrependsEmptyDelimiter asserts that REQ.Send wraps the caller's
// body with a leading empty delimiter frame.
func TestReq_PrependsEmptyDelimiter(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testCtx(t)

	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("hello"))), "send")
	got, err := b.Recv(ctx)
	require.NoError(t, err, "peer recv")
	require.Equal(t, 2, got.NumFrames(), "frames (empty + body)")
	require.Empty(t, got.Frames()[0], "leading frame should be empty delimiter")
	require.Equal(t, []byte("hello"), got.Frames()[1])
}

// TestReq_StripsEmptyDelimiterOnRecv asserts that REQ.Recv strips the
// leading empty frame the REP peer prepends to its reply.
func TestReq_StripsEmptyDelimiterOnRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testCtx(t)

	// First send so the FSM moves to canRecv and a sticky forwarder launches.
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q"))), "send")
	// Drain the request from the peer side so we keep pipe clean.
	_, err := b.Recv(ctx)
	require.NoError(t, err, "drain request")

	// Peer-side REP-shape reply: [empty, "reply"].
	reply := msg.FromFrames([]byte{}, []byte("reply"))
	require.NoError(t, b.Send(ctx, reply), "peer send")
	got, err := eng.Recv(ctx)
	require.NoError(t, err, "recv")
	require.Equal(t, 1, got.NumFrames())
	require.Equal(t, []byte("reply"), got.Bytes())
}

// TestReq_DoubleSendFSMViolation asserts that calling Send twice
// without a matching Recv returns ErrFSMViolation.
func TestReq_DoubleSendFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testCtx(t)

	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("1"))), "first send")
	err := eng.Send(ctx, msg.FromBytes([]byte("2")))
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation, "second send")
}

// TestReq_RecvBeforeSendFSMViolation asserts that Recv without a prior
// Send returns ErrFSMViolation.
func TestReq_RecvBeforeSendFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	_, err := eng.Recv(context.Background())
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation)
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
	mustAttach(t, eng, a)

	ctx := testCtx(t)
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q"))), "send")
	// Drain request; peer replies.
	_, err := b.Recv(ctx)
	require.NoError(t, err, "drain")
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("a"))), "peer send")
	_, err = eng.Recv(ctx)
	require.NoError(t, err, "first recv")
	// A second Recv with no Send between must fail FSM.
	_, err = eng.Recv(ctx)
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation, "second recv")
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
	mustAttach(t, reqEng, reqSide)
	mustAttach(t, repEng, repSide)

	ctx := testCtx(t)

	const rounds = 4
	for i := 0; i < rounds; i++ {
		body := []byte{byte('A' + i)}
		// REQ → REP
		errCh := make(chan error, 1)
		go func() {
			errCh <- reqEng.Send(ctx, msg.FromBytes(body))
		}()

		got, err := repEng.Recv(ctx)
		require.NoError(t, err, "rep recv %d", i)
		require.NoError(t, <-errCh, "req send %d", i)
		require.Equal(t, 1, got.NumFrames())
		require.Equal(t, body, got.Bytes())

		// REP → REQ
		reply := append([]byte("r:"), body...)
		require.NoError(t, repEng.Send(ctx, msg.FromBytes(reply)), "rep send %d", i)
		gotReply, err := reqEng.Recv(ctx)
		require.NoError(t, err, "req recv %d", i)
		require.Equal(t, 1, gotReply.NumFrames())
		require.Equal(t, reply, gotReply.Bytes())
	}
}

// TestReq_Close_UnblocksRecv asserts Close unblocks a parked Recv.
func TestReq_Close_UnblocksRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := context.Background()
	// Send to move to canRecv, then drain so the peer side is idle.
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q"))), "send")
	_, err := b.Recv(ctx)
	require.NoError(t, err, "drain")

	recvDone := make(chan error, 1)
	go func() {
		_, err := eng.Recv(ctx)
		recvDone <- err
	}()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, eng.Close(), "close")
	select {
	case err := <-recvDone:
		require.ErrorIs(t, err, zmqerr.ErrSocketClosed)
	case <-time.After(time.Second):
		require.Fail(t, "recv did not unblock on close")
	}
}

// TestReq_Send_WaitsForPeer asserts that Send parks when no peer is
// attached and wakes when one arrives.
func TestReq_Send_WaitsForPeer(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	ctx := testCtx(t)

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- eng.Send(ctx, msg.FromBytes([]byte("q")))
	}()
	select {
	case err := <-sendDone:
		require.Fail(t, "send returned early", "%v", err)
	case <-time.After(50 * time.Millisecond):
	}

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	select {
	case err := <-sendDone:
		require.NoError(t, err, "send")
	case <-time.After(time.Second):
		require.Fail(t, "send did not unblock on attach")
	}
	// Drain the request so the pipe doesn't hold a message at close.
	_, err := b.Recv(ctx)
	require.NoError(t, err, "drain")
}

// TestReq_RecvCtxErrorKeepsAwaitingReply pins the libzmq EAGAIN semantic:
// a Recv that fails with a context error must NOT flip the FSM or drop
// the sticky pipe — a retried Recv still gets the in-flight reply.
func TestReq_RecvCtxErrorKeepsAwaitingReply(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testCtx(t)
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q"))), "send")
	_, err := b.Recv(ctx)
	require.NoError(t, err, "drain request")

	// Recv times out before the reply exists.
	shortCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	_, err = eng.Recv(shortCtx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "first recv")

	// The reply lands late; a retried Recv must deliver it (previously
	// the failed Recv reset the FSM to canSend and this returned
	// ErrFSMViolation / lost the reply).
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("late-but-mine"))), "peer reply")
	got, err := eng.Recv(ctx)
	require.NoError(t, err, "retried recv")
	require.Equal(t, "late-but-mine", string(got.Bytes()))
}

// TestReq_StaleReplyDiscardedOnNextSend: a reply buffered on a pipe from
// an exchange the FSM already abandoned must not be delivered as the
// answer to a later request — Send drains the chosen pipe first.
func TestReq_StaleReplyDiscardedOnNextSend(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testCtx(t)
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q1"))), "send q1")
	_, err := b.Recv(ctx)
	require.NoError(t, err, "drain q1")

	// Malformed reply (no empty delimiter) aborts the exchange and flips
	// the FSM back to canSend.
	require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("garbage"))), "malformed reply")
	_, err = eng.Recv(ctx)
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, "recv malformed")

	// The "real" reply to q1 arrives after the abort: it is now stale.
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("stale"))), "stale reply")

	// Next request must discard the stale reply at Send time...
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q2"))), "send q2")
	got, err := b.Recv(ctx)
	require.NoError(t, err, "drain q2")
	require.Equal(t, "q2", string(got.Frames()[1]))
	// ...so the reply Recv returns q2's answer, not the stale one.
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("fresh"))), "fresh reply")
	reply, err := eng.Recv(ctx)
	require.NoError(t, err, "recv fresh")
	require.Equal(t, "fresh", string(reply.Bytes()), "stale reply delivered for a later request")
}

// TestReq_CloseUnblocksSendAtHWM: a Send blocked on a full sticky-candidate
// pipe must honor the Close contract and unblock with ErrSocketClosed
// (the old code parked in a bare Pipe.Send with no closeCh arm).
func TestReq_CloseUnblocksSendAtHWM(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	// Fill the single HWM slot from the engine side so eng.Send blocks.
	require.NoError(t, a.Send(context.Background(), msg.FromBytes([]byte("fill"))), "fill")

	sendDone := make(chan error, 1)
	go func() { sendDone <- eng.Send(context.Background(), msg.FromBytes([]byte("q"))) }()
	select {
	case err := <-sendDone:
		require.Fail(t, "send returned before close", "%v", err)
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, eng.Close(), "close")
	select {
	case err := <-sendDone:
		require.ErrorIs(t, err, zmqerr.ErrSocketClosed, "blocked send")
	case <-time.After(time.Second):
		require.Fail(t, "send at HWM did not unblock on close")
	}
}
