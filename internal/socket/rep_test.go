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

// TestRep_StripsEnvelopeOnRecv asserts that REP.Recv returns only the
// body (frames after the first empty delimiter).
func TestRep_StripsEnvelopeOnRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testCtx(t)

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

	ctx := testCtx(t)

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

	ctx := testCtx(t)
	wire := msg.FromFrames([]byte{}, []byte("q"))
	require.NoError(t, b.Send(ctx, wire), "peer send")
	_, err := eng.Recv(ctx)
	require.NoError(t, err, "first recv")
	_, err = eng.Recv(ctx)
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation, "second recv")
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
	ctx := testCtx(t)
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

	ctx := testCtx(t)

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

	recvDone := make(chan error, 1)
	go func() {
		_, err := eng.Recv(context.Background())
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
