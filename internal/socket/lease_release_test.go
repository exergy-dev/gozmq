package socket_test

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// A Send whose pipe has closed before the engine's lookup was detached
// must give back every lease reference it took: the buffer returns to its
// pool once the caller releases its own message.

// lentTo returns a lent [id, body...] message and a counter of its put calls.
func lentTo(id []byte, body ...[]byte) (*msg.Msg, *atomic.Int32) {
	puts := new(atomic.Int32)
	buf := []byte("lent-buffer")
	frames := append([][]byte{id}, body...)
	return msg.NewLent(func([]byte) { puts.Add(1) }, [][]byte{buf}, frames...), puts
}

// vanishedPeer attaches a peer named id whose end is already closed.
func vanishedPeer(t *testing.T, eng socket.Engine, id []byte) {
	t.Helper()
	engP, peer := pipe.NewPair(4, 4, id, nil)
	t.Cleanup(func() { closePipes(engP, peer) })
	mustAttach(t, eng, engP)
	require.NoError(t, peer.Close())
}

func TestStream_FailedSendReleasesLease(t *testing.T) {
	t.Parallel()
	id := []byte("gone")
	for name, body := range map[string][][]byte{
		"single frame": {[]byte("lent-buffer")},
		"joined":       {[]byte("a"), []byte("b")},
	} {
		t.Run(name, func(t *testing.T) {
			eng := socket.NewStreamEngine()
			defer eng.Close()
			vanishedPeer(t, eng, id)

			m, puts := lentTo(id, body...)
			err := eng.Send(testutil.Ctx(t), m)
			require.ErrorIs(t, err, zmqerr.ErrRouteUnknown)
			require.Zero(t, puts.Load(), "caller still holds m")
			m.Release()
			require.Equal(t, int32(1), puts.Load(), "a reference leaked")
		})
	}
}

// A successful STREAM Send hands m itself, and its reference, to the peer
// pipe: the buffer goes back once the pipe's reader releases it.
func TestStream_SendHandsReferenceToPipe(t *testing.T) {
	t.Parallel()
	id := []byte("peer")
	eng := socket.NewStreamEngine()
	defer eng.Close()
	engP, peer := pipe.NewPair(4, 4, id, nil)
	defer closePipes(engP, peer)
	mustAttach(t, eng, engP)

	m, puts := lentTo(id, []byte("a"), []byte("b"))
	require.NoError(t, eng.Send(testutil.Ctx(t), m))
	got, err := peer.RecvOr(testutil.Ctx(t), nil)
	require.NoError(t, err)
	require.Same(t, m, got)
	require.Zero(t, puts.Load(), "released before the writer was done")
	got.Release()
	require.Equal(t, int32(1), puts.Load())
}

func TestRouter_FailedSendReleasesLease(t *testing.T) {
	t.Parallel()
	id := []byte("gone")

	t.Run("mandatory", func(t *testing.T) {
		eng := socket.NewRouterEngine(true)
		defer eng.Close()
		vanishedPeer(t, eng, id)

		m, puts := lentTo(id, []byte("lent-buffer"))
		err := eng.Send(testutil.Ctx(t), m)
		require.ErrorIs(t, err, zmqerr.ErrRouteUnknown)
		require.Zero(t, puts.Load(), "caller still holds m")
		m.Release()
		require.Equal(t, int32(1), puts.Load())
	})

	t.Run("dropped", func(t *testing.T) {
		eng := socket.NewRouterEngine(false)
		defer eng.Close()
		vanishedPeer(t, eng, id)

		m, puts := lentTo(id, []byte("lent-buffer"))
		require.NoError(t, eng.Send(testutil.Ctx(t), m))
		require.Equal(t, int32(1), puts.Load(), "Send takes m's reference when it drops")
		require.Equal(t, uint64(1), eng.Metrics().Dropped)
	})

	t.Run("unknown identity", func(t *testing.T) {
		eng := socket.NewRouterEngine(false)
		defer eng.Close()

		m, puts := lentTo([]byte("nobody"), []byte("lent-buffer"))
		require.NoError(t, eng.Send(testutil.Ctx(t), m))
		require.Equal(t, int32(1), puts.Load())
	})
}
