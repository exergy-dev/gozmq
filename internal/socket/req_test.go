package socket_test

import (
	"context"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/pipe"
	"github.com/rkm/gozmq/internal/socket"
	"github.com/rkm/gozmq/internal/zmqerr"
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
