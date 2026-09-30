package msg

import (
	"sync"
	"sync/atomic"
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

// TestMsgRetainReleaseAreNoops: on a plain message Retain and Release must
// leave the frames untouched however often (and in whatever order) they
// are called.
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

// lentMsg returns a lent single-frame message and a counter of the times
// its release func has run.
func lentMsg() (*Msg, *atomic.Int32) {
	released := new(atomic.Int32)
	return NewWithRelease(func() { released.Add(1) }, []byte("lent")), released
}

func TestMsgLent(t *testing.T) {
	require.False(t, FromBytes([]byte("x")).Lent())
	require.False(t, NewWithRelease(nil, []byte("x")).Lent(), "nil release makes a plain message")
	require.False(t, FromBytes([]byte("x")).Derive([]byte("y")).Lent())
	m, _ := lentMsg()
	require.True(t, m.Lent())
	require.True(t, m.Derive().Lent())
}

// TestMsgReleaseRunsOnce: release runs exactly once, after the last of
// every Retain'd and Derive'd reference is released.
func TestMsgReleaseRunsOnce(t *testing.T) {
	m, released := lentMsg()
	m.Retain()
	d := m.Derive([]byte("envelope"), m.Frames()[0])
	require.Equal(t, [][]byte{[]byte("envelope"), []byte("lent")}, d.Frames())
	m.Release()
	m.Release()
	require.Zero(t, released.Load(), "d still holds the frames")
	d.Release()
	require.EqualValues(t, 1, released.Load())
}

// TestMsgOverReleasePanics: releasing past zero panics, as do Retain and
// Derive on a released message, and none of them hands the frames back a
// second time or early: an over-released parent cannot drop a derived
// message's hold.
func TestMsgOverReleasePanics(t *testing.T) {
	m, released := lentMsg()
	d := m.Derive(m.Frames()...)
	m.Release()
	require.Panics(t, m.Release)
	require.Panics(t, m.Retain)
	require.Panics(t, func() { m.Derive() })
	require.Zero(t, released.Load(), "d's hold was stolen")
	d.Release()
	require.EqualValues(t, 1, released.Load())
	require.Panics(t, d.Release)
	require.EqualValues(t, 1, released.Load())
}

// TestMsgConcurrentRelease: fan-out style, many derived messages released
// concurrently (run with -race) give the frames back exactly once.
func TestMsgConcurrentRelease(t *testing.T) {
	for range 100 {
		m, released := lentMsg()
		var wg sync.WaitGroup
		for range 8 {
			d := m.Derive(m.Frames()...)
			d.Retain()
			wg.Add(2)
			go func() { defer wg.Done(); d.Release() }()
			go func() { defer wg.Done(); d.Release() }()
		}
		m.Release()
		wg.Wait()
		require.EqualValues(t, 1, released.Load())
	}
}
