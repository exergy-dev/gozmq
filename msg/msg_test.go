package msg

import (
	"sync"
	"testing"

	"github.com/exergy-dev/gozmq/internal/bufpool"
	"github.com/stretchr/testify/require"
)

func TestMsgAppendFrame(t *testing.T) {
	m := New()
	m.AppendFrame([]byte("a"))
	m.AppendFrame([]byte("bb"))
	require.Equal(t, 2, m.NumFrames())
	require.Equal(t, []byte("a"), m.Frames()[0])
	require.Equal(t, []byte("bb"), m.Frames()[1])
	require.Equal(t, 3, m.TotalSize())
}

func TestMsgBytesSingleFrame(t *testing.T) {
	m := FromBytes([]byte("hello"))
	require.Equal(t, "hello", string(m.Bytes()))
}

func TestMsgBytesPanicsOnMultipart(t *testing.T) {
	m := FromFrames([]byte("a"), []byte("b"))
	require.Panics(t, func() { _ = m.Bytes() }, "expected panic on multipart Bytes")
}

func TestMsgSingleFrame(t *testing.T) {
	m := FromBytes([]byte("hello"))
	b, ok := m.SingleFrame()
	require.True(t, ok)
	require.Equal(t, "hello", string(b))

	m = FromFrames([]byte("a"), []byte("b"))
	b, ok = m.SingleFrame()
	require.False(t, ok)
	require.Nil(t, b)
}

func TestMsgReset(t *testing.T) {
	m := FromFrames([]byte("x"), []byte("y"))
	m.Reset()
	require.Equal(t, 0, m.NumFrames())
}

func TestMsgResetReleasesPooledFrames(t *testing.T) {
	pool := bufpool.Get(1024)
	m := New()
	m.AppendPooledFrame(pool[:16], pool)
	holder := m.pooled
	require.Len(t, holder, 1)

	m.Reset()
	require.Equal(t, 0, m.NumFrames())
	require.Nil(t, m.pooled, "pooled buffers still attached after Reset")

	m.Reset()
}

// TestMsgReleaseNoPooledIsNoop ensures Release on a *Msg with no pooled
// frames is safe (no panic, no negative refcount surprise on later use).
func TestMsgReleaseNoPooledIsNoop(t *testing.T) {
	m := FromBytes([]byte("hello"))
	m.Release()
	m.Release() // double-release with no pool is still a no-op
	require.Equal(t, 1, m.NumFrames())
}

// TestMsgRetainReleaseDecrements verifies Retain bumps the count and the
// pooled buffers survive until the final Release.
func TestMsgRetainReleaseDecrements(t *testing.T) {
	buf := bufpool.Get(4 * 1024)
	for i := range buf {
		buf[i] = byte(i)
	}
	m := New()
	m.AppendPooledFrame(buf[:1024], buf)
	require.Equal(t, int32(1), m.refs.Load())
	m.Retain()
	require.Equal(t, int32(2), m.refs.Load())
	m.Release()
	require.Equal(t, int32(1), m.refs.Load())
	require.NotNil(t, m.pooled, "pooled buffers reclaimed too early")
	m.Release()
	require.Equal(t, int32(0), m.refs.Load())
	require.Nil(t, m.pooled, "pooled buffers not reclaimed after final Release")
}

// TestMsgResetForcesReclamation verifies Reset on a *Msg with pooled
// frames returns them immediately regardless of refcount.
func TestMsgResetForcesReclamation(t *testing.T) {
	buf := bufpool.Get(4 * 1024)
	m := New()
	m.AppendPooledFrame(buf[:512], buf)
	m.Retain()
	m.Reset()
	require.Nil(t, m.pooled, "Reset did not reclaim pooled buffers")
	require.Equal(t, 0, m.NumFrames(), "Reset did not truncate frames")
}

// TestMsgReleaseIdempotentAfterReclamation ensures a Release after the
// pool buffers have been reclaimed (e.g. via Reset) is harmless.
func TestMsgReleaseIdempotentAfterReclamation(t *testing.T) {
	buf := bufpool.Get(4 * 1024)
	m := New()
	m.AppendPooledFrame(buf[:32], buf)
	m.Reset()
	m.Release()
}

// ---------------------------------------------------------------------------
// Ownership machinery: FromFramesOwned, TransferPool, Recycle, IsPooled.
// ---------------------------------------------------------------------------

// TestFromFramesOwnedAdoptsSlice verifies FromFramesOwned adopts the frame
// slice header without copying it.
func TestFromFramesOwnedAdoptsSlice(t *testing.T) {
	frames := [][]byte{[]byte("a"), []byte("bb")}
	m := FromFramesOwned(frames)
	require.Equal(t, 2, m.NumFrames())
	require.Same(t, &frames[0], &m.Frames()[0], "frame slice header was copied")
	require.False(t, m.IsPooled(), "locally built message must not be pooled")
}

// TestTransferPoolMovesOwnership verifies TransferPool moves the pooled
// bookkeeping (and refcount) to the destination and disarms the source.
func TestTransferPoolMovesOwnership(t *testing.T) {
	buf := bufpool.Get(8 * 1024)
	src := New()
	src.AppendPooledFrame(buf[:64], buf)
	src.Retain() // refs = 2; the transferred message must inherit it
	require.True(t, src.IsPooled())

	dst := FromFramesOwned(src.Frames()[:1])
	dst.TransferPool(src)
	require.True(t, dst.IsPooled(), "destination did not inherit pooled buffers")
	require.False(t, src.IsPooled(), "source still owns pooled buffers")
	require.Equal(t, int32(2), dst.refs.Load(), "refcount not transferred")

	// Source is disarmed: recycling it must not reclaim dst's buffers.
	Recycle(src)
	require.True(t, dst.IsPooled())
	Recycle(dst)
	require.True(t, dst.IsPooled(), "first Recycle reclaimed despite refs=2")
	Recycle(dst)
	require.False(t, dst.IsPooled(), "buffers not reclaimed at refcount zero")
}

// TestRecycleIsRefcountAware verifies Recycle decrements the refcount and
// only reclaims when the last reference drops (the PUB fanout contract:
// N retained references, N concurrent post-flush Recycles, one reclaim).
func TestRecycleIsRefcountAware(t *testing.T) {
	buf := bufpool.Get(8 * 1024)
	m := New()
	m.AppendPooledFrame(buf[:128], buf)
	m.Retain() // refs = 2
	Recycle(m)
	require.True(t, m.IsPooled(), "Recycle reclaimed while references remain")
	require.Equal(t, int32(1), m.refs.Load())
	Recycle(m)
	require.False(t, m.IsPooled(), "final Recycle did not reclaim")
}

// TestRecycleDoubleReleaseSafe ensures recycling after reclamation (and on
// nil / non-pooled messages) is a harmless no-op.
func TestRecycleDoubleReleaseSafe(t *testing.T) {
	Recycle(nil)
	plain := FromBytes([]byte("x"))
	Recycle(plain)
	Recycle(plain)
	require.Equal(t, 1, plain.NumFrames())

	buf := bufpool.Get(8 * 1024)
	m := New()
	m.AppendPooledFrame(buf[:8], buf)
	Recycle(m)
	require.False(t, m.IsPooled())
	Recycle(m) // already reclaimed: must not double-Put or go negative
	require.False(t, m.IsPooled())
}

// TestRecycleConcurrent exercises the fanout shape under the race detector:
// N references released from N goroutines must reclaim exactly once.
func TestRecycleConcurrent(t *testing.T) {
	const fanout = 8
	for iter := 0; iter < 200; iter++ {
		buf := bufpool.Get(8 * 1024)
		m := New()
		m.AppendPooledFrame(buf[:4096], buf)
		for i := 1; i < fanout; i++ {
			m.Retain()
		}
		var wg sync.WaitGroup
		wg.Add(fanout)
		for i := 0; i < fanout; i++ {
			go func() {
				defer wg.Done()
				Recycle(m)
			}()
		}
		wg.Wait()
		require.False(t, m.IsPooled(), "buffers not reclaimed after last concurrent Recycle")
		require.Equal(t, int32(0), m.refs.Load(), "refcount drifted: double or missed release")
	}
}
