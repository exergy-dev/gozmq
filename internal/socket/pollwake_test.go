package socket_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/msg"
)

// These tests pin the level-trigger contract that a root-level blocking
// Poller.Poll depends on: a goroutine parked on the channel obtained from
// ReadyRecv() BEFORE any data existed must wake when a message is
// delivered — with no polling loop and no re-call of ReadyRecv. They
// cover the three paths with no forwarder goroutine between the pipe and
// the engine: FairQueue direct mode, pairEngine, and reqEngine's sticky
// pipe.

// parkOnGate parks a goroutine reading ch and returns a channel that
// closes when the read completes.
func parkOnGate(ch <-chan struct{}) <-chan struct{} {
	woke := make(chan struct{})
	go func() {
		<-ch
		close(woke)
	}()
	return woke
}

func requireNoWake(t *testing.T, woke <-chan struct{}) {
	t.Helper()
	select {
	case <-woke:
		require.Fail(t, "ready gate armed before any message was delivered")
	case <-time.After(50 * time.Millisecond):
	}
}

func requireWake(t *testing.T, woke <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-woke:
	case <-time.After(time.Second):
		require.Fail(t, "parked poller did not wake", what)
	}
}

func TestFairQueue_ReadyRecvWakesOnDirectDelivery(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()
	defer fq.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	require.NoError(t, fq.Attach(a), "attach") // single pipe → direct mode

	woke := parkOnGate(fq.ReadyRecv())
	requireNoWake(t, woke)

	require.NoError(t, b.Send(testCtx(t), msg.FromBytes([]byte("x"))), "peer send")
	requireWake(t, woke, "fairqueue direct mode")
	require.True(t, fq.HasPending(), "message must still be pending (non-consuming signal)")
}

func TestPair_ReadyRecvWakesOnDelivery(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	woke := parkOnGate(eng.ReadyRecv())
	requireNoWake(t, woke)

	require.NoError(t, b.Send(testCtx(t), msg.FromBytes([]byte("x"))), "peer send")
	requireWake(t, woke, "pair engine")
	require.True(t, eng.CanRecvNow())
}

func TestReq_ReadyRecvWakesOnReply(t *testing.T) {
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

	woke := parkOnGate(eng.ReadyRecv())
	requireNoWake(t, woke)

	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("reply"))), "peer reply")
	requireWake(t, woke, "req sticky pipe")
	require.True(t, eng.CanRecvNow())
}
