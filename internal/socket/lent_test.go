package socket_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/msg"
)

// Engine-level checks of the message-reference contract (see the package
// doc): Send takes over m's reference on success, derives rather than
// modifies, and releases what it drops.

// newLent returns a lent message and a counter of its release calls.
func newLent(frames ...[]byte) (*msg.Msg, *atomic.Int32) {
	released := new(atomic.Int32)
	return msg.NewLent(func([]byte) { released.Add(1) }, [][]byte{nil}, frames...), released
}

// drainPipe discards everything queued on p.
func drainPipe(p *pipe.End) {
	for {
		if _, ok := p.TryRecv(); !ok {
			return
		}
	}
}

// TestPub_LentFanoutRefPerPeer: each matching peer gets its own derived
// message holding one reference, a peer at HWM drops (and releases) its
// copy, and the lent frames go back only after every copy is released.
func TestPub_LentFanoutRefPerPeer(t *testing.T) {
	t.Parallel()
	eng := socket.NewPubEngine()
	defer eng.Close()
	engineEnds, peerEnds := makePairs(3, 1, 1)
	defer closePipes(append(engineEnds, peerEnds...)...)
	mustAttach(t, eng, engineEnds...)
	ctx := testutil.Ctx(t)

	for _, p := range peerEnds {
		require.NoError(t, p.Send(ctx, subscribeMsg(true, nil)))
	}
	for _, p := range peerEnds {
		require.Eventually(t, func() bool {
			_ = eng.Send(ctx, msg.FromBytes([]byte("probe")))
			_, ok := p.TryRecv()
			return ok
		}, 2*time.Second, 5*time.Millisecond)
	}
	for _, p := range peerEnds {
		drainPipe(p)
	}
	// Fill peer 2's queue (HWM 1) so the lent message is dropped for it.
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("fill"))))
	drainPipe(peerEnds[0])
	drainPipe(peerEnds[1])

	m, released := newLent([]byte("topic"), []byte("body"))
	require.NoError(t, eng.Send(ctx, m))
	d0, ok0 := peerEnds[0].TryRecv()
	d1, ok1 := peerEnds[1].TryRecv()
	require.True(t, ok0 && ok1)
	require.NotSame(t, m, d0)
	require.NotSame(t, d0, d1)
	require.True(t, d0.Lent() && d1.Lent())
	require.Equal(t, m.Frames(), d0.Frames())
	fill, ok := peerEnds[2].TryRecv()
	require.True(t, ok)
	require.Equal(t, "fill", string(fill.Bytes()))
	_, ok = peerEnds[2].TryRecv()
	require.False(t, ok, "peer at HWM must not receive the lent message")

	d0.Release()
	require.Zero(t, released.Load())
	d1.Release()
	require.EqualValues(t, 1, released.Load())
}

// TestRouter_LentSendDerives: ROUTER strips the identity into a derived
// message without touching the caller's, and releases the caller's
// reference; an unroutable message is released as a drop.
func TestRouter_LentSendDerives(t *testing.T) {
	t.Parallel()
	eng := socket.NewRouterEngine(false)
	defer eng.Close()
	a, b := pipe.NewPair(4, 4, []byte("peer"), nil)
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	ctx := testutil.Ctx(t)

	m, released := newLent([]byte("peer"), []byte("body"))
	m.Retain() // keep a reference to inspect m afterwards
	require.NoError(t, eng.Send(ctx, m))
	require.Equal(t, [][]byte{[]byte("peer"), []byte("body")}, m.Frames(), "caller's message modified")
	d, ok := b.TryRecv()
	require.True(t, ok)
	require.Equal(t, [][]byte{[]byte("body")}, d.Frames())
	require.True(t, d.Lent())
	m.Release()
	require.Zero(t, released.Load(), "the routed message still holds the frames")
	d.Release()
	require.EqualValues(t, 1, released.Load())

	lost, lostReleased := newLent([]byte("nobody"), []byte("body"))
	require.NoError(t, eng.Send(ctx, lost))
	require.EqualValues(t, 1, lostReleased.Load(), "unroutable drop must release")
}

// TestSub_LentFilterMissReleased: a message SUB filters out is released.
func TestSub_LentFilterMissReleased(t *testing.T) {
	t.Parallel()
	sub := socket.NewSubEngine()
	eng := socket.Engine(sub)
	defer eng.Close()
	require.NoError(t, sub.Subscribe([]byte("A")))
	engineEnds, peerEnds := makePairs(1, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)
	mustAttach(t, eng, engineEnds...)
	ctx := testutil.Ctx(t)
	drainPipe(peerEnds[0]) // the forwarded SUBSCRIBE

	miss, released := newLent([]byte("B"))
	require.NoError(t, peerEnds[0].Send(ctx, miss))
	require.NoError(t, peerEnds[0].Send(ctx, msg.FromBytes([]byte("A"))))
	got, err := eng.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, "A", string(got.Bytes()))
	require.EqualValues(t, 1, released.Load())
}
