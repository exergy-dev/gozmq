package zmtp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
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
	codec := NewFrameCodec(0)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			require.NoError(t, writeFrame(codec, &buf, tc.flags, tc.body), "WriteFrame")
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
	codec := NewFrameCodec(0)
	t.Run("short", func(t *testing.T) {
		var buf bytes.Buffer
		require.NoError(t, writeFrame(codec, &buf, FlagCommand, []byte{1, 2, 3}), "WriteFrame")
		want := []byte{FlagCommand, 0x03, 0x01, 0x02, 0x03}
		require.Equal(t, want, buf.Bytes())
	})
	t.Run("long", func(t *testing.T) {
		body := bytes.Repeat([]byte{0xAB}, 300)
		var buf bytes.Buffer
		require.NoError(t, writeFrame(codec, &buf, 0, body), "WriteFrame")
		got := buf.Bytes()
		require.Equal(t, FlagLong, got[0], "flags")
		ln := binary.BigEndian.Uint64(got[1:9])
		require.Equal(t, uint64(len(body)), ln, "length")
		require.Equal(t, body, got[9:], "body")
	})
}

func TestFrameAppendHeader(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(0)
	cases := []struct {
		name    string
		flags   uint8
		bodyLen int
		want    []byte
	}{
		{name: "short", flags: FlagCommand, bodyLen: 5, want: []byte{FlagCommand, 5}},
		{name: "long", flags: FlagMore, bodyLen: 300, want: []byte{FlagMore | FlagLong, 0, 0, 0, 0, 0, 0, 1, 44}},
		{name: "empty", flags: 0, bodyLen: 0, want: []byte{0, 0}},
		{name: "forced-long", flags: FlagLong, bodyLen: 1, want: []byte{FlagLong, 0, 0, 0, 0, 0, 0, 0, 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := codec.AppendFrameHeader([]byte{0xEE}, tc.flags, tc.bodyLen)
			require.NoError(t, err, "AppendFrameHeader")
			require.Equal(t, append([]byte{0xEE}, tc.want...), got, "appended to dst")
		})
	}
}

// writeFrame encodes one frame into w the way the session writer does.
func writeFrame(c FrameCodec, w *bytes.Buffer, flags uint8, body []byte) error {
	hdr, err := c.AppendFrameHeader(nil, flags, len(body))
	if err != nil {
		return err
	}
	w.Write(hdr)
	w.Write(body)
	return nil
}

func TestFrameReuseDst(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(0)
	body := []byte("hello-zmtp")
	var buf bytes.Buffer
	require.NoError(t, writeFrame(codec, &buf, 0, body), "WriteFrame")
	dst := make([]byte, 0, 64)
	_, got, err := codec.ReadFrame(&buf, dst, nil)
	require.NoError(t, err, "ReadFrame")
	require.True(t, &got[:1][0] == &dst[:1][0], "ReadFrame did not reuse dst backing array")
	// Fresh allocation when capacity is insufficient.
	var buf2 bytes.Buffer
	big := bytes.Repeat([]byte{0xEF}, 200)
	require.NoError(t, writeFrame(codec, &buf2, 0, big), "WriteFrame")
	small := make([]byte, 0, 8)
	_, got2, err := codec.ReadFrame(&buf2, small, nil)
	require.NoError(t, err, "ReadFrame")
	require.GreaterOrEqual(t, cap(got2), len(big), "expected fresh allocation")

	// A frame that does not fit dst is read into alloc's buffer.
	var buf3 bytes.Buffer
	require.NoError(t, writeFrame(codec, &buf3, 0, big), "WriteFrame")
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
		err := writeFrame(codec, &buf, 0, bytes.Repeat([]byte{0x00}, 17))
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
	codec := NewFrameCodec(0)
	_, _, err := codec.ReadFrame(bytes.NewReader([]byte{0x80, 0}), nil, nil)
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, "decode")
	_, err = codec.AppendFrameHeader(nil, 0xF0, 0)
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, "encode")
}

func TestFrameTruncated(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(0)

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
// layer.
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
		require.NoError(t, writeFrame(codec, &buf, flags, body), "re-encode failed for accepted frame")
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
// take the incremental path and must arrive intact, with exactly one Get
// for the final buffer, which is the slice returned (the session reader
// hands that slice back to the pool), and every intermediate Put back.
// Over a bufio.Reader whose buffer holds the commit point there is no
// intermediate at all (the Peek proof); otherwise a frame uses one
// intermediate until commit exceeds the threshold, then grows.
func TestFrameReadIncrementalRoundTrip(t *testing.T) {
	t.Parallel()
	const T = incrementalBodyThreshold
	const G = incrementalGrowth
	const bufSize = 16 << 10
	plain := func(r io.Reader) io.Reader { return r }
	buffered := func(r io.Reader) io.Reader { return bufio.NewReaderSize(r, bufSize) }
	codec := NewFrameCodec(0)
	for _, tc := range []struct {
		name string
		n    int
		wrap func(io.Reader) io.Reader
		gets int // pool Gets: the final buffer plus intermediates
	}{
		{"T+1/onebyte", T + 1, func(r io.Reader) io.Reader { return iotest.OneByteReader(r) }, 2},
		{"3T/plain", 3 * T, plain, 2},
		{"10T+17/plain", 10*T + 17, plain, 2},
		{"T+1/bufio", T + 1, buffered, 1},                 // commit 8 KiB+1 <= bufio: Peek path
		{"256KiB/bufio", 256 << 10, buffered, 1},          // commit 16 KiB == bufio: Peek path
		{"1MiB/bufio", 1 << 20, buffered, 2},              // commit 64 KiB > bufio: one intermediate
		{"4MiB+5/plain", 4<<20 + 5, plain, 3},             // commit > T: T then grow to commit
		{"G*bufSize+1/bufio", G*bufSize + 1, buffered, 2}, // commit = bufio+1: just misses Peek
	} {
		require.Equal(t, 256<<10, G*bufSize, "test assumes a 16 KiB bufio proves 256 KiB")
		body := make([]byte, tc.n)
		for i := range body {
			body[i] = byte(i * 7)
		}
		var buf bytes.Buffer
		require.NoError(t, writeFrame(codec, &buf, FlagMore, body))
		wire := buf.Bytes()

		// Plain allocation.
		flags, got, err := codec.ReadFrame(tc.wrap(bytes.NewReader(wire)), make([]byte, 0, 16), nil)
		require.NoError(t, err, tc.name)
		require.Equal(t, FlagMore|FlagLong, flags, tc.name)
		require.Equal(t, body, got, tc.name)

		// Pooled: the returned body is the last Get (of exactly n bytes) and
		// every intermediate went back through Put.
		pool := &countingPool{}
		_, got, err = codec.ReadFrame(tc.wrap(bytes.NewReader(wire)), nil, pool)
		require.NoError(t, err, tc.name)
		require.Equal(t, body, got, tc.name)
		require.Equal(t, tc.n, pool.finalN, "%s: final Get size", tc.name)
		require.Same(t, &pool.final[0], &got[0], "%s: returned body is not the pooled buffer", tc.name)
		require.Equal(t, tc.gets, pool.gets, "%s: Get calls", tc.name)
		require.Equal(t, 1, pool.live, "%s: intermediates not returned to the pool (gets=%d puts=%d)", tc.name, pool.gets, pool.puts)
	}
}

// scratchPool is a countingPool whose intermediates go through the
// ScratchPool methods, as the session's unpooled reader supplies them.
type scratchPool struct {
	countingPool
	scratchGets, scratchPuts int
	scratchLive              int
	scratchSizes             []int
}

func (p *scratchPool) GetScratch(n int) []byte {
	p.scratchGets++
	p.scratchLive++
	p.scratchSizes = append(p.scratchSizes, n)
	return make([]byte, n)
}

func (p *scratchPool) PutScratch([]byte) { p.scratchPuts++; p.scratchLive-- }

// TestFrameReadIncrementalScratchPool: when the pool implements
// ScratchPool, every intermediate comes from GetScratch and goes back
// through PutScratch, the final body is the only Get, and Put is never
// called on a successful read. On a failed read the final body is Put back
// and no scratch is left out.
func TestFrameReadIncrementalScratchPool(t *testing.T) {
	t.Parallel()
	const T = incrementalBodyThreshold
	const G = incrementalGrowth
	codec := NewFrameCodec(0)
	for _, n := range []int{1 << 20, 4<<20 + 5} {
		body := bytes.Repeat([]byte{0xA5}, n)
		var buf bytes.Buffer
		require.NoError(t, writeFrame(codec, &buf, 0, body))
		wire := buf.Bytes()
		commit := (n + G - 1) / G

		pool := &scratchPool{}
		_, got, err := codec.ReadFrame(bytes.NewReader(wire), nil, pool)
		require.NoError(t, err)
		require.Equal(t, body, got)
		require.Equal(t, 1, pool.gets, "n=%d: only the body comes from Get", n)
		require.Zero(t, pool.puts, "n=%d: Put on a successful read", n)
		require.Zero(t, pool.scratchLive, "n=%d: scratch not returned", n)
		require.Equal(t, min(commit, T), pool.scratchSizes[0], "n=%d: first intermediate", n)
		require.LessOrEqual(t, pool.scratchSizes[len(pool.scratchSizes)-1], commit, "n=%d: intermediate beyond the commit point", n)

		// Truncate just after the commit point: the body was allocated and
		// must go back through Put; the intermediate through PutScratch.
		pool = &scratchPool{}
		_, _, err = codec.ReadFrame(bytes.NewReader(wire[:9+commit+1]), nil, pool)
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
		require.Equal(t, 1, pool.gets, "n=%d", n)
		require.Equal(t, 1, pool.puts, "n=%d: failed body not returned", n)
		require.Zero(t, pool.scratchLive, "n=%d: scratch not returned on failure", n)

		// Truncate before the commit point: no body Get at all.
		pool = &scratchPool{}
		_, _, err = codec.ReadFrame(bytes.NewReader(wire[:9+commit/2]), nil, pool)
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
		require.Zero(t, pool.gets, "n=%d: body allocated before the commit point", n)
		require.Zero(t, pool.scratchLive, "n=%d", n)
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
// large body and then stalls must cost memory proportional to what it sent,
// never the announced size, and must never draw the announced size from the
// pool. Before incremental reading each such frame pinned the announced size.
// Over a bufio.Reader that can prove the commit point (the session's case for
// frames up to 256 KiB) nothing at all is allocated while the peer stalls.
func TestFrameReadStalledLargeHeaderBoundsAllocation(t *testing.T) {
	const T = incrementalBodyThreshold
	const G = incrementalGrowth
	codec := NewFrameCodec(64 << 20)
	type tc struct {
		name  string
		n     int
		k     int  // body bytes delivered before the stall
		bufio bool // wrap in the session-sized bufio.Reader
		bound int64
		gets  int // pool Gets expected while stalled (-1: don't care)
	}
	var cases []tc
	for _, k := range []int{0, 1, 100 << 10, 1 << 20} {
		// Peak = the buffer just filled plus the up-to-G-times larger one it
		// is copied into, so (G+1) x received, plus the first chunk.
		bound := int64((G+1)*k + 2*T)
		cases = append(cases,
			tc{fmt.Sprintf("64MiB/k=%d/raw", k), 64<<20 - 1, k, false, bound, -1},
			tc{fmt.Sprintf("64MiB/k=%d/bufio", k), 64<<20 - 1, k, true, bound, -1},
		)
	}
	// 256 KiB over bufio: commit (16 KiB) fits the buffer, so a peer that
	// sends less than that pins nothing but the bufio buffer it already had.
	cases = append(cases, tc{"256KiB/k=8KiB/bufio", 256 << 10, 8 << 10, true, 32 << 10, 0})
	for _, c := range cases {
		hdr := append([]byte{FlagLong}, binary.BigEndian.AppendUint64(nil, uint64(c.n))...)
		sr := &stallingReader{
			data:    append(hdr, make([]byte, c.k)...),
			stalled: make(chan struct{}),
			release: make(chan struct{}),
		}
		var r io.Reader = sr
		if c.bufio {
			r = bufio.NewReaderSize(sr, 16<<10)
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
		case <-sr.stalled:
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: reader never consumed the prefix", c.name)
		}

		var during runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&during)
		growth := int64(during.HeapAlloc) - int64(before.HeapAlloc)
		t.Logf("%s: HeapAlloc growth while stalled: %d KiB", c.name, growth>>10)
		require.Less(t, growth, c.bound, "%s: allocation not proportional to bytes received", c.name)
		close(sr.release)
		err := <-done
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, c.name)
		// pool fields are read only after done, which the reader goroutine
		// signals after its last Get/Put.
		require.NotEqual(t, c.n, pool.finalN, "%s: the pool was asked for the announced size", c.name)
		if c.gets >= 0 {
			require.Equal(t, c.gets, pool.gets, "%s: pool Gets", c.name)
		}
		require.Zero(t, pool.live, "%s: buffers not returned to the pool after a failed read", c.name)
	}
}
