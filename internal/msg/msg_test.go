package msg

import (
	"testing"

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
	pool := PoolGet(1024)
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
	buf := PoolGet(4 * 1024)
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
	buf := PoolGet(4 * 1024)
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
	buf := PoolGet(4 * 1024)
	m := New()
	m.AppendPooledFrame(buf[:32], buf)
	m.Reset()
	m.Release()
}
