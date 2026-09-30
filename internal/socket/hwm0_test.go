package socket_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/msg"
)

// The HWM-0 tests below push more messages than an unbounded direction's
// channel holds before the consumer reads anything, so most of them sit in
// the pipe's overflow slice. Each consumer of a pipe's receive side must
// get every one, in order; the package goleak TestMain proves no goroutine
// is left behind.

// hwm0Backlog comfortably exceeds the unbounded channel capacity (1024).
const hwm0Backlog = 3000

func seqMsg(src, i int) *msg.Msg {
	return msg.FromFrames([]byte{byte(src)}, binary.BigEndian.AppendUint32(nil, uint32(i)))
}

func seqOf(m *msg.Msg) (src, i int) {
	f := m.Frames()
	return int(f[0][0]), int(binary.BigEndian.Uint32(f[1]))
}

func sendBacklog(t *testing.T, p *pipe.End, src, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		require.NoError(t, p.Send(testCtx(t), seqMsg(src, i)))
	}
}

func TestHWM0_FairQueueSingle(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()
	defer fq.Close()
	a, b := pipe.NewPair(0, 0, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	require.NoError(t, fq.Attach(a)) // one pipe: single-pipe fast path

	sendBacklog(t, b, 0, hwm0Backlog)
	for i := 0; i < hwm0Backlog; i++ {
		m, err := fq.Recv(testCtx(t))
		require.NoError(t, err)
		_, got := seqOf(m)
		require.Equal(t, i, got)
	}
	require.False(t, fq.CanRecv())
}

func TestHWM0_FairQueueMulti(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()
	defer fq.Close()
	eng, peers := makePairs(2, 0, 0)
	defer closePipes(append(eng, peers...)...)
	for _, p := range eng {
		require.NoError(t, fq.Attach(p)) // two pipes: round-robin walk
	}
	for src, p := range peers {
		sendBacklog(t, p, src, hwm0Backlog)
	}
	// The second peer leaves before anything is read: the queue must
	// still drain the whole backlog.
	require.NoError(t, peers[1].Close())
	require.NoError(t, fq.Detach(eng[1]))

	next := make([]int, 2)
	for n := 0; n < 2*hwm0Backlog; n++ {
		m, err := fq.Recv(testCtx(t))
		require.NoError(t, err, "after %d", n)
		src, i := seqOf(m)
		require.Equal(t, next[src], i, "peer %d order", src)
		next[src]++
	}
}

func TestHWM0_Pair(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
	defer eng.Close()
	a, b := pipe.NewPair(0, 0, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	sendBacklog(t, b, 0, hwm0Backlog)
	for i := 0; i < hwm0Backlog/2; i++ {
		m, err := eng.Recv(testCtx(t))
		require.NoError(t, err)
		_, got := seqOf(m)
		require.Equal(t, i, got)
	}
	// The rest is drained from the retired pipe after the peer leaves.
	require.NoError(t, b.Close())
	require.NoError(t, eng.DetachPipe(a))
	for i := hwm0Backlog / 2; i < hwm0Backlog; i++ {
		m, err := eng.Recv(testCtx(t))
		require.NoError(t, err)
		_, got := seqOf(m)
		require.Equal(t, i, got)
	}
	require.False(t, eng.CanRecvNow())
}

// TestHWM0_ReqDiscardsWholeStaleBacklog: REQ discards replies left over
// from an abandoned exchange before sending the next request. With HWM 0
// the stale backlog spills into overflow; all of it must go, or a stale
// reply would be paired with the new request.
func TestHWM0_ReqDiscardsWholeStaleBacklog(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()
	a, b := pipe.NewPair(0, 0, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	ctx := testCtx(t)

	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q1"))))
	_, err := b.Recv(ctx)
	require.NoError(t, err)
	for i := 0; i < hwm0Backlog; i++ {
		require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("stale"))))
	}
	m, err := eng.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, "stale", string(m.Bytes()), "first reply answers q1")

	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q2"))))
	_, err = b.Recv(ctx)
	require.NoError(t, err)
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("r2"))))
	m, err = eng.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, "r2", string(m.Bytes()), "a stale reply leaked into the next exchange")
}
