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

// TestMsgRetainReleaseAreNoops: the compatibility no-ops must leave the
// frames untouched however often (and in whatever order) they are called.
func TestMsgRetainReleaseAreNoops(t *testing.T) {
	m := FromFrames([]byte("hello"), []byte("world"))
	m.Release()
	m.Retain()
	m.Release()
	m.Release()
	require.Equal(t, 2, m.NumFrames())
	require.Equal(t, "hello", string(m.Frames()[0]))
	require.Equal(t, "world", string(m.Frames()[1]))
}
