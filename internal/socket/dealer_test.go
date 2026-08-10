package socket_test

import (
	"context"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq/internal/msg"
	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/stretchr/testify/require"
)

// TestDealer_LoadBalancesOutbound asserts that DEALER.Send distributes
// messages round-robin across attached peer pipes (reusing the same
// semantics as PUSH). Sending send=9 messages across n=3 peers leaves
// each peer with exactly 3.
func TestDealer_LoadBalancesOutbound(t *testing.T) {
	t.Parallel()
	eng := socket.NewDealerEngine()
	defer eng.Close()

	const n = 3
	const send = 9
	engineEnds, peerEnds := makePairs(n, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)

	mustAttach(t, eng, engineEnds...)

	ctx := testCtx(t)
	for i := 0; i < send; i++ {
		require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte{byte(i)})), "send %d", i)
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
		require.Equal(t, send/n, c, "peer %d got %d messages, expected %d", i, c, send/n)
	}
}

// TestDealer_FairQueuesInbound asserts that DEALER.Recv fair-queues
// messages from every attached pipe.
func TestDealer_FairQueuesInbound(t *testing.T) {
	t.Parallel()
	eng := socket.NewDealerEngine()
	defer eng.Close()

	const n = 3
	engineEnds, peerEnds := makePairs(n, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)

	mustAttach(t, eng, engineEnds...)

	ctx := testCtx(t)
	want := map[byte]bool{}
	for i := 0; i < n; i++ {
		b := byte('a' + i)
		want[b] = true
		require.NoError(t, peerEnds[i].Send(ctx, msg.FromBytes([]byte{b})), "peer send")
	}
	got := map[byte]bool{}
	for i := 0; i < n; i++ {
		m, err := eng.Recv(ctx)
		require.NoError(t, err, "recv")
		got[m.Bytes()[0]] = true
	}
	for b := range want {
		require.True(t, got[b], "missing %c", b)
	}
}

// TestDealer_DoesNotMangleIdentity asserts that DEALER passes the message
// body through unchanged (no empty-delimiter insertion, no identity
// prepend). This is what differentiates DEALER from REQ.
func TestDealer_DoesNotMangleIdentity(t *testing.T) {
	t.Parallel()
	eng := socket.NewDealerEngine()
	defer eng.Close()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testCtx(t)

	sent := msg.FromFrames([]byte("hello"), []byte("world"))
	require.NoError(t, eng.Send(ctx, sent), "send")
	got, err := b.Recv(ctx)
	require.NoError(t, err, "recv")
	require.Equal(t, 2, got.NumFrames(), "frames (no delimiter)")
	require.Equal(t, "hello", string(got.Frames()[0]))
	require.Equal(t, "world", string(got.Frames()[1]))
}

// TestDealer_CloseUnblocksBoth asserts Close unblocks both pending Send
// and pending Recv with ErrSocketClosed.
func TestDealer_CloseUnblocksBoth(t *testing.T) {
	t.Parallel()
	eng := socket.NewDealerEngine()

	// Spawn a blocking Recv with no pipes attached.
	recvDone := make(chan error, 1)
	go func() {
		_, err := eng.Recv(context.Background())
		recvDone <- err
	}()

	// Spawn a blocking Send with no pipes attached (no peer → blocks
	// waiting for one).
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- eng.Send(context.Background(), msg.FromBytes([]byte("x")))
	}()

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, eng.Close(), "close")
	for _, ch := range []chan error{recvDone, sendDone} {
		select {
		case err := <-ch:
			require.ErrorIs(t, err, zmqerr.ErrSocketClosed)
		case <-time.After(time.Second):
			require.Fail(t, "did not unblock on close")
		}
	}
}
