package zmtp

import (
	"bytes"
	"encoding/binary"
	"io"
	"runtime"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
)

func TestFrameRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		flags uint8
		body  []byte
	}{
		{name: "empty", flags: 0, body: []byte{}},
		{name: "single-byte", flags: 0, body: []byte{0xAA}},
		{name: "max-short", flags: 0, body: bytes.Repeat([]byte{0x55}, 255)},
		{name: "first-long", flags: 0, body: bytes.Repeat([]byte{0x33}, 256)},
		{name: "command-flag", flags: FlagCommand, body: []byte("READY")},
		{name: "more-flag", flags: FlagMore, body: []byte("part1")},
		{name: "long-forced", flags: FlagLong, body: []byte{0x01, 0x02, 0x03}},
		{name: "all-flags", flags: FlagMore | FlagLong | FlagCommand, body: bytes.Repeat([]byte{0xCC}, 1024)},
	}
	codec := NewFrameCodec(MaxBodyUnlimited)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			require.NoError(t, codec.WriteFrame(&buf, tc.flags, tc.body), "WriteFrame")
			gotFlags, gotBody, err := codec.ReadFrame(&buf, nil, nil)
			require.NoError(t, err, "ReadFrame")
			// LONG may be promoted by the encoder; the other bits must round-trip.
			wantFlags := tc.flags
			if len(tc.body) > 255 {
				wantFlags |= FlagLong
			}
			require.Equal(t, wantFlags, gotFlags, "flags")
			if len(tc.body) == 0 {
				require.Empty(t, gotBody, "body")
			} else {
				require.Equal(t, tc.body, gotBody, "body")
			}
			require.Equal(t, 0, buf.Len(), "trailing bytes")
		})
	}
}

func TestFrameWireFormat(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(MaxBodyUnlimited)
	t.Run("short", func(t *testing.T) {
		var buf bytes.Buffer
		require.NoError(t, codec.WriteFrame(&buf, FlagCommand, []byte{1, 2, 3}), "WriteFrame")
		want := []byte{FlagCommand, 0x03, 0x01, 0x02, 0x03}
		require.Equal(t, want, buf.Bytes())
	})
	t.Run("long", func(t *testing.T) {
		body := bytes.Repeat([]byte{0xAB}, 300)
		var buf bytes.Buffer
		require.NoError(t, codec.WriteFrame(&buf, 0, body), "WriteFrame")
		got := buf.Bytes()
		require.Equal(t, FlagLong, got[0], "flags")
		ln := binary.BigEndian.Uint64(got[1:9])
		require.Equal(t, uint64(len(body)), ln, "length")
		require.Equal(t, body, got[9:], "body")
	})
}

func TestFrameAppendHeaderMatchesWriteFrame(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(MaxBodyUnlimited)
	appender, ok := codec.(FrameHeaderAppender)
	require.True(t, ok, "codec does not implement FrameHeaderAppender")

	cases := []struct {
		name  string
		flags uint8
		body  []byte
	}{
		{name: "short", flags: FlagCommand, body: []byte("READY")},
		{name: "long", flags: FlagMore, body: bytes.Repeat([]byte{0xAB}, 300)},
		{name: "empty", flags: 0, body: nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var want bytes.Buffer
			require.NoError(t, codec.WriteFrame(&want, tc.flags, tc.body), "WriteFrame")

			scratch, hdrLen, err := appender.AppendFrameHeader(nil, tc.flags, tc.body)
			require.NoError(t, err, "AppendFrameHeader")
			require.Len(t, scratch, hdrLen, "returned scratch length vs header length")

			got := append(append([]byte(nil), scratch...), tc.body...)
			require.Equal(t, want.Bytes(), got, "wire mismatch")
		})
	}
}

func TestFrameReuseDst(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(MaxBodyUnlimited)
	body := []byte("hello-zmtp")
	var buf bytes.Buffer
	require.NoError(t, codec.WriteFrame(&buf, 0, body), "WriteFrame")
	dst := make([]byte, 0, 64)
	_, got, err := codec.ReadFrame(&buf, dst, nil)
	require.NoError(t, err, "ReadFrame")
	require.True(t, &got[:1][0] == &dst[:1][0], "ReadFrame did not reuse dst backing array")
	// Fresh allocation when capacity is insufficient.
	var buf2 bytes.Buffer
	big := bytes.Repeat([]byte{0xEF}, 200)
	require.NoError(t, codec.WriteFrame(&buf2, 0, big), "WriteFrame")
	small := make([]byte, 0, 8)
	_, got2, err := codec.ReadFrame(&buf2, small, nil)
	require.NoError(t, err, "ReadFrame")
	require.GreaterOrEqual(t, cap(got2), len(big), "expected fresh allocation")

	// A frame that does not fit dst is read into alloc's buffer.
	var buf3 bytes.Buffer
	require.NoError(t, codec.WriteFrame(&buf3, 0, big), "WriteFrame")
	pooled := make([]byte, 256)
	_, got3, err := codec.ReadFrame(&buf3, small, fixedPool{buf: pooled})
	require.NoError(t, err, "ReadFrame")
	require.Equal(t, big, got3)
	require.True(t, &got3[0] == &pooled[0], "ReadFrame did not read into alloc's buffer")
}

func TestFrameMaxBodyEnforced(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(16)

	t.Run("encode-rejects-oversize", func(t *testing.T) {
		var buf bytes.Buffer
		err := codec.WriteFrame(&buf, 0, bytes.Repeat([]byte{0x00}, 17))
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})

	t.Run("decode-rejects-oversize", func(t *testing.T) {
		// Hand-build a frame 17 bytes long that the strict codec must refuse.
		raw := append([]byte{0x00, 17}, bytes.Repeat([]byte{0xAA}, 17)...)
		_, _, err := codec.ReadFrame(bytes.NewReader(raw), nil, nil)
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})

	t.Run("decode-rejects-oversize-long", func(t *testing.T) {
		raw := []byte{FlagLong, 0, 0, 0, 0, 0, 0, 0, 17}
		raw = append(raw, bytes.Repeat([]byte{0xAA}, 17)...)
		_, _, err := codec.ReadFrame(bytes.NewReader(raw), nil, nil)
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})
}

func TestFrameRejectsReservedFlags(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(MaxBodyUnlimited)
	_, _, err := codec.ReadFrame(bytes.NewReader([]byte{0x80, 0}), nil, nil)
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, "decode")
	require.ErrorIs(t, codec.WriteFrame(io.Discard, 0xF0, nil), zmqerr.ErrMalformedFrame, "encode")
}

func TestFrameTruncated(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(MaxBodyUnlimited)

	t.Run("clean-eof", func(t *testing.T) {
		_, _, err := codec.ReadFrame(strings.NewReader(""), nil, nil)
		require.ErrorIs(t, err, io.EOF)
	})
	t.Run("missing-length", func(t *testing.T) {
		_, _, err := codec.ReadFrame(bytes.NewReader([]byte{0x00}), nil, nil)
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})
	t.Run("missing-body", func(t *testing.T) {
		_, _, err := codec.ReadFrame(bytes.NewReader([]byte{0x00, 0x05, 'h', 'i'}), nil, nil)
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})
	t.Run("missing-long-length", func(t *testing.T) {
		_, _, err := codec.ReadFrame(bytes.NewReader([]byte{FlagLong, 0, 0, 0}), nil, nil)
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})
}

func TestFrameNewFrameCodecPanicsOnNegative(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() {
		NewFrameCodec(-1)
	})
}

// FuzzFrameDecode feeds arbitrary byte streams through the decoder. The
// decoder must never panic and must, after a successful decode, accept its
// own re-encoded output. This is the load-bearing safety net for the framing
// layer per design §10.4.
func FuzzFrameDecode(f *testing.F) {
	// Seed with a couple of valid frames so the fuzzer has a starting point.
	f.Add([]byte{0x00, 0x00})
	f.Add([]byte{FlagCommand, 0x05, 'R', 'E', 'A', 'D', 'Y'})
	f.Add(append([]byte{FlagLong, 0, 0, 0, 0, 0, 0, 0, 0x04}, []byte{1, 2, 3, 4}...))
	f.Add([]byte{0xFF})     // reserved flag bits
	f.Add([]byte{FlagLong}) // truncated long header

	// A LONG header announcing ~64 MiB with two body bytes: must cost the
	// incremental threshold, not the announced size, per iteration.
	f.Add(append([]byte{FlagLong, 0, 0, 0, 0, 0x03, 0xFF, 0xFF, 0xFF}, 'x', 'y'))
	// Just over the incremental threshold so the slow path is in the corpus.
	f.Add(append(append([]byte{FlagLong}, binary.BigEndian.AppendUint64(nil, incrementalBodyThreshold+1)...),
		bytes.Repeat([]byte{0x5A}, incrementalBodyThreshold+1)...))

	// The production cap, so the fuzzer exercises the same bound sessions use.
	codec := NewFrameCodec(64 << 20)
	f.Fuzz(func(t *testing.T, data []byte) {
		flags, body, err := codec.ReadFrame(bytes.NewReader(data), nil, nil)
		if err != nil {
			return
		}
		// Invariant: a successful decode followed by encode + decode yields
		// the same flags and body.
		var buf bytes.Buffer
		require.NoError(t, codec.WriteFrame(&buf, flags, body), "re-encode failed for accepted frame")
		flags2, body2, err2 := codec.ReadFrame(&buf, nil, nil)
		require.NoError(t, err2, "re-decode failed")
		// LONG is always set by re-encode for the round-trip when forced; mask it.
		require.Equal(t, flags & ^FlagLong, flags2 & ^FlagLong, "flags drift")
		require.Equal(t, body, body2, "body drift")
	})
}

// fixedPool hands out one preallocated buffer.
type fixedPool struct{ buf []byte }

func (p fixedPool) Get(n int) []byte { return p.buf[:n] }
func (fixedPool) Put([]byte)         {}

// countingPool records Get/Put traffic and keeps the last buffer handed out
// for the final body so tests can check pointer identity.
type countingPool struct {
	gets, puts int
	finalN     int
	final      []byte
	live       int // buffers handed out and not yet returned
}

func (p *countingPool) Get(n int) []byte {
	p.gets++
	p.live++
	b := make([]byte, n)
	p.finalN, p.final = n, b
	return b
}

func (p *countingPool) Put([]byte) { p.puts++; p.live-- }

// TestFrameReadIncrementalRoundTrip: bodies above incrementalBodyThreshold
// take the incremental path and must arrive intact, with exactly one call
// to alloc for the final buffer, which is the slice returned (the session
// reader hands that slice back to the pool).
func TestFrameReadIncrementalRoundTrip(t *testing.T) {
	t.Parallel()
	const T = incrementalBodyThreshold
	codec := NewFrameCodec(MaxBodyUnlimited)
	for _, n := range []int{T + 1, 3 * T, 10*T + 17} {
		body := make([]byte, n)
		for i := range body {
			body[i] = byte(i * 7)
		}
		var buf bytes.Buffer
		require.NoError(t, codec.WriteFrame(&buf, FlagMore, body))
		wire := buf.Bytes()

		// Plain allocation.
		flags, got, err := codec.ReadFrame(bytes.NewReader(wire), make([]byte, 0, 16), nil)
		require.NoError(t, err, "n=%d", n)
		require.Equal(t, FlagMore|FlagLong, flags)
		require.Equal(t, body, got, "n=%d", n)

		// Pooled: the returned body is the last Get (of exactly n bytes) and
		// every intermediate went back through Put.
		pool := &countingPool{}
		_, got, err = codec.ReadFrame(iotest.OneByteReader(bytes.NewReader(wire)), nil, pool)
		require.NoError(t, err, "n=%d", n)
		require.Equal(t, body, got, "n=%d", n)
		require.Equal(t, n, pool.finalN, "final Get size for n=%d", n)
		require.Same(t, &pool.final[0], &got[0], "returned body is not the pooled buffer")
		require.Equal(t, 1, pool.live, "n=%d: intermediates not returned to the pool (gets=%d puts=%d)", n, pool.gets, pool.puts)
	}
}

// stallingReader serves a prefix, then blocks until released and fails.
type stallingReader struct {
	data    []byte
	stalled chan struct{} // closed once the prefix is consumed
	release chan struct{}
}

func (r *stallingReader) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		if len(r.data) == 0 {
			close(r.stalled)
		}
		return n, nil
	}
	<-r.release
	return 0, io.ErrUnexpectedEOF
}

// TestFrameReadStalledLargeHeaderBoundsAllocation: a peer that announces a
// 64 MiB body and then stalls must cost memory proportional to what it sent,
// never the announced size, and must never draw the announced size from the
// pool. Before incremental reading each such frame pinned 64 MiB.
func TestFrameReadStalledLargeHeaderBoundsAllocation(t *testing.T) {
	const T = incrementalBodyThreshold
	codec := NewFrameCodec(64 << 20)
	for _, k := range []int{0, 1, 100 << 10, 1 << 20} {
		hdr := append([]byte{FlagLong}, binary.BigEndian.AppendUint64(nil, 64<<20-1)...)
		r := &stallingReader{
			data:    append(hdr, make([]byte, k)...),
			stalled: make(chan struct{}),
			release: make(chan struct{}),
		}
		pool := &countingPool{}

		var before runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)

		done := make(chan error, 1)
		go func() {
			_, _, err := codec.ReadFrame(r, nil, pool)
			done <- err
		}()
		select {
		case <-r.stalled:
		case <-time.After(10 * time.Second):
			t.Fatalf("k=%d: reader never consumed the prefix", k)
		}

		var during runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&during)
		growth := int64(during.HeapAlloc) - int64(before.HeapAlloc)
		t.Logf("k=%d: HeapAlloc growth while stalled: %d KiB", k, growth>>10)
		// Peak = the buffer just filled plus the incrementalGrowth-times larger
		// one it is copied into, so (incrementalGrowth+1) x received, plus the
		// first chunk.
		require.Less(t, growth, int64((incrementalGrowth+1)*k+2*T), "k=%d: allocation not proportional to bytes received", k)
		close(r.release)
		err := <-done
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, "k=%d", k)
		// pool fields are read only after done, which the reader goroutine
		// signals after its last Get/Put.
		require.NotEqual(t, 64<<20-1, pool.finalN, "k=%d: the pool was asked for the announced size", k)
		require.Zero(t, pool.live, "k=%d: buffers not returned to the pool after a failed read", k)
	}
}
