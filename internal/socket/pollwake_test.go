package socket_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/msg"
)

// These tests pin the wake-up contract that a root-level blocking
// Poller.Poll depends on: after CanRecvNow answers false, a goroutine
// parked on the engine's in-gate must wake when the engine becomes
// receivable — with no polling loop and no further call into the engine.
// They cover FairQueue's single-pipe mode, pairEngine, reqEngine's sticky
// pipe, REP's FSM and XPUB's subscription events.

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

// parkOnIn checks eng the way a Poller scan does and parks on its in-gate.
func parkOnIn(t *testing.T, eng socket.Engine) <-chan struct{} {
	t.Helper()
	require.False(t, eng.CanRecvNow())
	in, _ := eng.Gates()
	return parkOnGate(in)
}

func TestFairQueue_GateWakesOnSinglePipeDelivery(t *testing.T) {
	t.Parallel()
	gate := make(chan struct{}, 1)
	fq := socket.NewFairQueue(gate)
	defer fq.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	require.NoError(t, fq.Attach(a), "attach") // single pipe → single-pipe path

	require.False(t, fq.CanRecv())
	woke := parkOnGate(gate)
	requireNoWake(t, woke)

	require.NoError(t, b.Send(testutil.Ctx(t), msg.FromBytes([]byte("x"))), "peer send")
	requireWake(t, woke, "fairqueue single pipe")
	require.True(t, fq.CanRecv(), "message must still be pending (non-consuming signal)")
}

func TestPair_InGateWakesOnDelivery(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	woke := parkOnIn(t, eng)
	requireNoWake(t, woke)

	require.NoError(t, b.Send(testutil.Ctx(t), msg.FromBytes([]byte("x"))), "peer send")
	requireWake(t, woke, "pair engine")
	require.True(t, eng.CanRecvNow())
}

func TestReq_InGateWakesOnReply(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q"))), "send")
	_, err := b.Recv(ctx)
	require.NoError(t, err, "drain request")

	woke := parkOnIn(t, eng)
	requireNoWake(t, woke)

	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("reply"))), "peer reply")
	requireWake(t, woke, "req sticky pipe")
	require.True(t, eng.CanRecvNow())
}

// TestXPub_InGateWakesOnSubscription: a Poller parked on XPUB's PollIn
// wakes when a peer subscribes, with no goroutine applying the command.
func TestXPub_InGateWakesOnSubscription(t *testing.T) {
	t.Parallel()
	eng := socket.NewXPubEngine(false)
	defer eng.Close()

	a1, b1 := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	a2, b2 := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a1, b1, a2, b2)
	mustAttach(t, eng, a1, a2)

	woke := parkOnIn(t, eng)
	requireNoWake(t, woke)

	require.NoError(t, b2.Send(testutil.Ctx(t), msg.FromBytes([]byte("\x01A"))), "peer subscribe")
	requireWake(t, woke, "xpub subscription")
	require.True(t, eng.CanRecvNow())
	require.True(t, eng.CanRecvNow(), "CanRecvNow must not consume the event")
}

// TestRep_InGateWakesAfterSend: a request queued while REP owes a reply is
// not receivable, so a Poller that wakes on its arrival finds nothing and
// parks again. Send moving the FSM back to canRecv must wake it: nothing
// else will arrive.
func TestRep_InGateWakesAfterSend(t *testing.T) {
	t.Parallel()
	eng := socket.NewRepEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)
	for _, q := range []string{"q1", "q2"} {
		require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte(q))), "peer request")
	}
	m, err := eng.Recv(ctx)
	require.NoError(t, err)
	m.Release()

	in, _ := eng.Gates()
	select { // the Poller took q2's token and found REP owing a reply
	case <-in:
	default:
	}
	woke := parkOnIn(t, eng)
	requireNoWake(t, woke)

	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("r1"))), "reply")
	requireWake(t, woke, "rep canRecv")
	require.True(t, eng.CanRecvNow())
}
