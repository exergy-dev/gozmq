package msg

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestMsgAppendFrame(t *testing.T) {
	m := FromFrames()
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
// its put func has run.
func lentMsg() (*Msg, *atomic.Int32) {
	released := new(atomic.Int32)
	buf := []byte("lent")
	return NewLent(func([]byte) { released.Add(1) }, [][]byte{buf}, buf), released
}

func TestMsgLent(t *testing.T) {
	require.False(t, FromBytes([]byte("x")).Lent())
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

var copySink *Msg

// TestFromFramesCopyAllocs pins the allocation count per size tier. The
// tier arithmetic rests on Msg being 64 bytes (Go size classes 96/128/192/
// 320 for one frame, 144/160/192/256 for two).
func TestFromFramesCopyAllocs(t *testing.T) {
	require.EqualValues(t, 64, unsafe.Sizeof(Msg{}), "tier sizes assume a 64-byte Msg")
	fr := func(ns ...int) [][]byte {
		out := make([][]byte, len(ns))
		for i, n := range ns {
			out[i] = bytes.Repeat([]byte{byte('a' + i)}, n)
		}
		return out
	}
	for _, tc := range []struct {
		name   string
		frames [][]byte
		allocs float64
	}{
		{"1x0", fr(0), 1},
		{"1x1", fr(1), 1},
		{"1x32", fr(32), 1},
		{"1x33", fr(33), 1},
		{"1x64", fr(64), 1},
		{"1x65", fr(65), 1},
		{"1x128", fr(128), 1},
		{"1x129", fr(129), 1},
		{"1x256", fr(256), 1},
		{"1x300", fr(300), 2},
		{"2x[0,32]", fr(0, 32), 1},
		{"2x[8,24]", fr(8, 24), 1},
		{"2x[8,32]", fr(8, 32), 1},
		{"2x[8,40]", fr(8, 40), 1},
		{"2x[8,64]", fr(8, 64), 1},
		{"2x[8,72]", fr(8, 72), 1},
		{"2x[8,136]", fr(8, 136), 1},
		{"2x[8,137]", fr(8, 137), 2},
		{"3x[8,8,8]", fr(8, 8, 8), 3},
	} {
		got := testing.AllocsPerRun(100, func() { copySink = FromFramesCopy(tc.frames...) })
		require.Equal(t, tc.allocs, got, tc.name)
	}
}

// TestFromFramesCopyContents: the frames equal the inputs, are independent
// of them, and each has cap == len so an append cannot reach a neighbour.
func TestFromFramesCopyContents(t *testing.T) {
	for _, lens := range [][]int{
		{}, {0}, {1}, {32}, {33}, {256}, {257}, {5000},
		{0, 0}, {0, 32}, {8, 32}, {8, 64}, {8, 137}, {3000, 3000},
		{1, 2, 3}, {0, 5, 0}, {8, 8, 8, 8, 300},
	} {
		in := make([][]byte, len(lens))
		want := make([][]byte, len(lens))
		for i, n := range lens {
			in[i] = bytes.Repeat([]byte{byte(0x41 + i)}, n)
			want[i] = bytes.Clone(in[i])
		}
		m := FromFramesCopy(in...)
		for i := range in { // mutate after: the message must not follow
			for j := range in[i] {
				in[i][j] = 0xFF
			}
		}
		require.Equal(t, len(lens), m.NumFrames(), "%v", lens)
		require.False(t, m.Lent(), "%v", lens)
		for i, f := range m.Frames() {
			require.Equal(t, want[i], f, "%v frame %d", lens, i)
			require.NotNil(t, f, "%v frame %d non-nil", lens, i)
			require.Equal(t, len(f), cap(f), "%v frame %d cap", lens, i)
		}
		// Appending to a frame must not overwrite its neighbour.
		if len(lens) > 1 {
			_ = append(m.Frames()[0], 0xEE)
			require.Equal(t, want[1], m.Frames()[1], "%v append leaked", lens)
		}
		m.Release() // no-op, must not panic
	}
}

// TestFromFramesCopyIsUsable: the co-allocated Msg behaves like any other
// (AppendFrame, Derive, TotalSize).
func TestFromFramesCopyIsUsable(t *testing.T) {
	m := FromFramesCopy([]byte("topic"), []byte("payload"))
	require.Equal(t, 12, m.TotalSize())
	m.AppendFrame([]byte("more"))
	require.Equal(t, 3, m.NumFrames())
	d := m.Derive(m.Frames()[1])
	require.Equal(t, "payload", string(d.Bytes()))
	s := FromFramesCopy([]byte("x"))
	s.AppendFrame([]byte("y"))
	require.Equal(t, [][]byte{[]byte("x"), []byte("y")}, s.Frames())
}

// TestMsgNewLent: put runs once per buffer after the last reference goes,
// Derive shares the lease, and the one-buffer message is one allocation.
func TestMsgNewLent(t *testing.T) {
	var got [][]byte
	put := func(b []byte) { got = append(got, b) }
	a, b := []byte("aaaa"), []byte("bbbb")

	m := NewLent(put, [][]byte{a, b}, a[:2], b[:2])
	require.True(t, m.Lent())
	d := m.Derive(m.Frames()[1])
	m.Release()
	require.Empty(t, got, "the derived message still holds the frames")
	d.Release()
	require.Equal(t, [][]byte{a, b}, got)

	got = nil
	one := NewLent(put, [][]byte{a}, a)
	one.Release()
	require.Equal(t, [][]byte{a}, got)
	require.Panics(t, func() { NewLent(nil, [][]byte{a}, a) })

	allocs := testing.AllocsPerRun(100, func() {
		m := NewLent(put, [][]byte{a}, a)
		m.Release()
	})
	require.LessOrEqual(t, allocs, 1.0, "one-buffer lent message is one allocation")
}

// TestMsgLeaseSize pins the size class the one-buffer lent message lands
// in (see the lease comment): message plus lease must stay within 112 bytes.
func TestMsgLeaseSize(t *testing.T) {
	require.LessOrEqual(t, unsafe.Sizeof(struct {
		Msg
		lease lease
	}{}), uintptr(112))
}
