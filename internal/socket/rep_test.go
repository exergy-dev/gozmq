package socket_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// TestRep_StripsEnvelopeOnRecv asserts that REP.Recv returns only the
// body (frames after the first empty delimiter).
func TestRep_StripsEnvelopeOnRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)

	// REQ-style wire: [identityA, empty, body]
	wire := msg.FromFrames([]byte("identityA"), []byte{}, []byte("hello"))
	require.NoError(t, b.Send(ctx, wire), "peer send")
	got, err := eng.Recv(ctx)
	require.NoError(t, err, "recv")
	require.Equal(t, 1, got.NumFrames(), "frames (body only)")
	require.Equal(t, []byte("hello"), got.Bytes())
}

// TestRep_ReplayEnvelopeOnSend asserts that REP.Send prepends the stashed
// envelope back onto the outbound body.
func TestRep_ReplayEnvelopeOnSend(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)

	// Deliver a REQ-shape inbound frame.
	wire := msg.FromFrames([]byte("idA"), []byte("idB"), []byte{}, []byte("hi"))
	require.NoError(t, b.Send(ctx, wire), "peer send")
	_, err := eng.Recv(ctx)
	require.NoError(t, err, "recv")

	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("reply"))), "send")

	got, err := b.Recv(ctx)
	require.NoError(t, err, "peer recv")
	// Expected outbound: [idA, idB, empty, reply]
	frames := got.Frames()
	require.Equal(t, 4, got.NumFrames(), "frames: %q", frames)
	require.Equal(t, []byte("idA"), frames[0])
	require.Equal(t, []byte("idB"), frames[1])
	require.Empty(t, frames[2], "frame 2 should be empty")
	require.Equal(t, []byte("reply"), frames[3])
}

// TestRep_DoubleRecvFSMViolation asserts that Recv twice without Send
// between returns ErrFSMViolation.
func TestRep_DoubleRecvFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)
	wire := msg.FromFrames([]byte{}, []byte("q"))
	require.NoError(t, b.Send(ctx, wire), "peer send")
	_, err := eng.Recv(ctx)
	require.NoError(t, err, "first recv")
	_, err = eng.Recv(ctx)
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation, "second recv")
}

// TestRep_ConcurrentRecvFSMViolation: a second Recv while one is already
// parked fails fast instead of overwriting the first one's envelope.
func TestRep_ConcurrentRecvFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)
	mc1, ec1 := recvAsync(ctx, eng)
	time.Sleep(20 * time.Millisecond)
	rctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, err := eng.Recv(rctx)
	cancel()
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation, "concurrent recv")

	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte("id"), []byte{}, []byte("q"))))
	select {
	case m := <-mc1:
		require.Equal(t, "q", string(m.Bytes()))
	case err := <-ec1:
		require.Failf(t, "first recv failed", "%v", err)
	case <-ctx.Done():
		require.Fail(t, "first recv did not get the request")
	}
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("a"))))
	got, err := b.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("id"), got.Frames()[0], "reply keeps the first request's envelope")
}

// TestRep_RecvCtxCancelRestoresCanRecv: a Recv abandoned by its context
// releases the in-flight reservation.
func TestRep_RecvCtxCancelRestoresCanRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	rctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := eng.Recv(rctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	ctx := testutil.Ctx(t)
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("q"))))
	got, err := eng.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, "q", string(got.Bytes()))
}

// TestRep_SendToVanishedRequester: a reply whose requester closed before
// the socket layer detached its pipe is dropped silently (libzmq), not
// surfaced as a raw io.ErrClosedPipe; the FSM returns to canRecv.
func TestRep_SendToVanishedRequester(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("q"))))
	_, err := eng.Recv(ctx)
	require.NoError(t, err)
	require.NoError(t, b.Close())

	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("a"))))
	require.Zero(t, eng.Metrics().Sent, "dropped reply must not count as sent")
	err = eng.Send(ctx, msg.FromBytes([]byte("a")))
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation, "FSM back to canRecv")
}

// TestRep_SkipsMalformedRequest: a request with no empty delimiter is
// dropped and Recv moves on to the next one.
func TestRep_SkipsMalformedRequest(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)
	require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("no-delimiter"))))
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("good"))))
	got, err := eng.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, "good", string(got.Bytes()))
}

// TestRep_SendBeforeRecvFSMViolation asserts that Send without a prior
// Recv returns ErrFSMViolation.
func TestRep_SendBeforeRecvFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
	defer eng.Close()
	err := eng.Send(context.Background(), msg.FromBytes([]byte("x")))
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation)
}

// TestRep_DoubleSendFSMViolation asserts that Send twice after one Recv
// returns ErrFSMViolation on the second Send.
func TestRep_DoubleSendFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	ctx := testutil.Ctx(t)
	wire := msg.FromFrames([]byte{}, []byte("q"))
	require.NoError(t, b.Send(ctx, wire), "peer send")
	_, err := eng.Recv(ctx)
	require.NoError(t, err, "recv")
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("a"))), "first send")
	err = eng.Send(ctx, msg.FromBytes([]byte("a2")))
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation, "second send")
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
	mustAttach(t, eng, engineEnds...)

	ctx := testutil.Ctx(t)

	// Peer 1 sends a request.
	wire := msg.FromFrames([]byte{}, []byte("q"))
	require.NoError(t, peerEnds[1].Send(ctx, wire), "peer1 send")
	_, err := eng.Recv(ctx)
	require.NoError(t, err, "recv")
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("a"))), "send")

	// Only peer 1 should receive the reply.
	got, err := peerEnds[1].Recv(ctx)
	require.NoError(t, err, "peer1 recv")
	require.Equal(t, 2, got.NumFrames())
	require.Equal(t, []byte("a"), got.Frames()[1])
	// Others should be empty.
	for _, idx := range []int{0, 2} {
		ctx2, c := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, err := peerEnds[idx].Recv(ctx2)
		c()
		require.ErrorIs(t, err, context.DeadlineExceeded, "peer %d unexpectedly got reply", idx)
	}
}

// TestRep_CloseUnblocksRecv asserts Close unblocks a parked Recv.
func TestRep_CloseUnblocksRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	err := testutil.RequireUnblocks(t, func() error {
		_, err := eng.Recv(context.Background())
		return err
	}, func() { require.NoError(t, eng.Close(), "close") })
	require.ErrorIs(t, err, zmqerr.ErrSocketClosed)
}

// TestRep_CloseUnblocksSendAtHWM: a reply Send blocked on the requester's
// full pipe must unblock with ErrSocketClosed when the engine closes,
// instead of waiting for the caller's ctx.
func TestRep_CloseUnblocksSendAtHWM(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("q"))), "request")
	_, err := eng.Recv(ctx)
	require.NoError(t, err, "recv")
	// Fill the single HWM slot toward the requester so the reply blocks.
	require.NoError(t, a.Send(ctx, msg.FromBytes([]byte("fill"))), "fill")

	err = testutil.RequireUnblocks(t,
		func() error { return eng.Send(context.Background(), msg.FromBytes([]byte("reply"))) },
		func() { require.NoError(t, eng.Close(), "close") })
	require.ErrorIs(t, err, zmqerr.ErrSocketClosed, "blocked send")
}
