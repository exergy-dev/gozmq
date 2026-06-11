package zmtp

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/zmqerr"
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
		{name: "command-flag", flags: iface.FlagCommand, body: []byte("READY")},
		{name: "more-flag", flags: iface.FlagMore, body: []byte("part1")},
		{name: "long-forced", flags: iface.FlagLong, body: []byte{0x01, 0x02, 0x03}},
		{name: "all-flags", flags: iface.FlagMore | iface.FlagLong | iface.FlagCommand, body: bytes.Repeat([]byte{0xCC}, 1024)},
	}
	codec := NewFrameCodec(MaxBodyUnlimited)
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			require.NoError(t, codec.WriteFrame(&buf, tc.flags, tc.body), "WriteFrame")
			gotFlags, gotBody, err := codec.ReadFrame(&buf, nil)
			require.NoError(t, err, "ReadFrame")
			// LONG may be promoted by the encoder; the other bits must round-trip.
			wantFlags := tc.flags
			if len(tc.body) > 255 {
				wantFlags |= iface.FlagLong
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
		require.NoError(t, codec.WriteFrame(&buf, iface.FlagCommand, []byte{1, 2, 3}), "WriteFrame")
		want := []byte{iface.FlagCommand, 0x03, 0x01, 0x02, 0x03}
		require.Equal(t, want, buf.Bytes())
	})
	t.Run("long", func(t *testing.T) {
		body := bytes.Repeat([]byte{0xAB}, 300)
		var buf bytes.Buffer
		require.NoError(t, codec.WriteFrame(&buf, 0, body), "WriteFrame")
		got := buf.Bytes()
		require.Equal(t, iface.FlagLong, got[0], "flags")
		ln := binary.BigEndian.Uint64(got[1:9])
		require.Equal(t, uint64(len(body)), ln, "length")
		require.Equal(t, body, got[9:], "body")
	})
}

func TestFrameAppendHeaderMatchesWriteFrame(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(MaxBodyUnlimited)
	appender, ok := codec.(iface.FrameHeaderAppender)
	require.True(t, ok, "codec does not implement FrameHeaderAppender")

	cases := []struct {
		name  string
		flags uint8
		body  []byte
	}{
		{name: "short", flags: iface.FlagCommand, body: []byte("READY")},
		{name: "long", flags: iface.FlagMore, body: bytes.Repeat([]byte{0xAB}, 300)},
		{name: "empty", flags: 0, body: nil},
	}

	for _, tc := range cases {
		tc := tc
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
	_, got, err := codec.ReadFrame(&buf, dst)
	require.NoError(t, err, "ReadFrame")
	require.True(t, &got[:1][0] == &dst[:1][0], "ReadFrame did not reuse dst backing array")
	// Fresh allocation when capacity is insufficient.
	var buf2 bytes.Buffer
	big := bytes.Repeat([]byte{0xEF}, 200)
	require.NoError(t, codec.WriteFrame(&buf2, 0, big), "WriteFrame")
	small := make([]byte, 0, 8)
	_, got2, err := codec.ReadFrame(&buf2, small)
	require.NoError(t, err, "ReadFrame")
	require.GreaterOrEqual(t, cap(got2), len(big), "expected fresh allocation")
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
		_, _, err := codec.ReadFrame(bytes.NewReader(raw), nil)
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})

	t.Run("decode-rejects-oversize-long", func(t *testing.T) {
		raw := []byte{iface.FlagLong, 0, 0, 0, 0, 0, 0, 0, 17}
		raw = append(raw, bytes.Repeat([]byte{0xAA}, 17)...)
		_, _, err := codec.ReadFrame(bytes.NewReader(raw), nil)
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})
}

func TestFrameRejectsReservedFlags(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(MaxBodyUnlimited)
	_, _, err := codec.ReadFrame(bytes.NewReader([]byte{0x80, 0}), nil)
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, "decode")
	require.ErrorIs(t, codec.WriteFrame(io.Discard, 0xF0, nil), zmqerr.ErrMalformedFrame, "encode")
}

func TestFrameTruncated(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(MaxBodyUnlimited)

	t.Run("clean-eof", func(t *testing.T) {
		_, _, err := codec.ReadFrame(strings.NewReader(""), nil)
		require.ErrorIs(t, err, io.EOF)
	})
	t.Run("missing-length", func(t *testing.T) {
		_, _, err := codec.ReadFrame(bytes.NewReader([]byte{0x00}), nil)
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})
	t.Run("missing-body", func(t *testing.T) {
		_, _, err := codec.ReadFrame(bytes.NewReader([]byte{0x00, 0x05, 'h', 'i'}), nil)
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})
	t.Run("missing-long-length", func(t *testing.T) {
		_, _, err := codec.ReadFrame(bytes.NewReader([]byte{iface.FlagLong, 0, 0, 0}), nil)
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
	f.Add([]byte{iface.FlagCommand, 0x05, 'R', 'E', 'A', 'D', 'Y'})
	f.Add(append([]byte{iface.FlagLong, 0, 0, 0, 0, 0, 0, 0, 0x04}, []byte{1, 2, 3, 4}...))
	f.Add([]byte{0xFF})           // reserved flag bits
	f.Add([]byte{iface.FlagLong}) // truncated long header

	codec := NewFrameCodec(1 << 20)
	f.Fuzz(func(t *testing.T, data []byte) {
		flags, body, err := codec.ReadFrame(bytes.NewReader(data), nil)
		if err != nil {
			return
		}
		// Invariant: a successful decode followed by encode + decode yields
		// the same flags and body.
		var buf bytes.Buffer
		require.NoError(t, codec.WriteFrame(&buf, flags, body), "re-encode failed for accepted frame")
		flags2, body2, err2 := codec.ReadFrame(&buf, nil)
		require.NoError(t, err2, "re-decode failed")
		// LONG is always set by re-encode for the round-trip when forced; mask it.
		require.Equal(t, flags & ^iface.FlagLong, flags2 & ^iface.FlagLong, "flags drift")
		require.Equal(t, body, body2, "body drift")
	})
}
