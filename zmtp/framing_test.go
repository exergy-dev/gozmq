package zmtp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/rkm/gozmq/internal/iface"
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
			if err := codec.WriteFrame(&buf, tc.flags, tc.body); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}
			gotFlags, gotBody, err := codec.ReadFrame(&buf, nil)
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			// LONG may be promoted by the encoder; the other bits must round-trip.
			wantFlags := tc.flags
			if len(tc.body) > 255 {
				wantFlags |= iface.FlagLong
			}
			if gotFlags != wantFlags {
				t.Fatalf("flags: got 0x%02x want 0x%02x", gotFlags, wantFlags)
			}
			if !bytes.Equal(gotBody, tc.body) {
				t.Fatalf("body mismatch: got %x want %x", gotBody, tc.body)
			}
			if buf.Len() != 0 {
				t.Fatalf("trailing bytes: %d", buf.Len())
			}
		})
	}
}

func TestFrameWireFormat(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(MaxBodyUnlimited)
	t.Run("short", func(t *testing.T) {
		var buf bytes.Buffer
		if err := codec.WriteFrame(&buf, iface.FlagCommand, []byte{1, 2, 3}); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
		want := []byte{iface.FlagCommand, 0x03, 0x01, 0x02, 0x03}
		if !bytes.Equal(buf.Bytes(), want) {
			t.Fatalf("bytes: got %x want %x", buf.Bytes(), want)
		}
	})
	t.Run("long", func(t *testing.T) {
		body := bytes.Repeat([]byte{0xAB}, 300)
		var buf bytes.Buffer
		if err := codec.WriteFrame(&buf, 0, body); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
		got := buf.Bytes()
		if got[0] != iface.FlagLong {
			t.Fatalf("flags: got 0x%02x want 0x%02x", got[0], iface.FlagLong)
		}
		ln := binary.BigEndian.Uint64(got[1:9])
		if ln != uint64(len(body)) {
			t.Fatalf("length: got %d want %d", ln, len(body))
		}
		if !bytes.Equal(got[9:], body) {
			t.Fatalf("body mismatch")
		}
	})
}

func TestFrameReuseDst(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(MaxBodyUnlimited)
	body := []byte("hello-zmtp")
	var buf bytes.Buffer
	if err := codec.WriteFrame(&buf, 0, body); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	dst := make([]byte, 0, 64)
	_, got, err := codec.ReadFrame(&buf, dst)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if &got[:1][0] != &dst[:1][0] {
		t.Fatalf("ReadFrame did not reuse dst backing array")
	}
	// Fresh allocation when capacity is insufficient.
	var buf2 bytes.Buffer
	big := bytes.Repeat([]byte{0xEF}, 200)
	if err := codec.WriteFrame(&buf2, 0, big); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	small := make([]byte, 0, 8)
	_, got2, err := codec.ReadFrame(&buf2, small)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if cap(got2) < len(big) {
		t.Fatalf("expected fresh allocation; got cap=%d", cap(got2))
	}
}

func TestFrameMaxBodyEnforced(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(16)

	t.Run("encode-rejects-oversize", func(t *testing.T) {
		var buf bytes.Buffer
		err := codec.WriteFrame(&buf, 0, bytes.Repeat([]byte{0x00}, 17))
		if !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("err: got %v want ErrMalformedFrame", err)
		}
	})

	t.Run("decode-rejects-oversize", func(t *testing.T) {
		// Hand-build a frame 17 bytes long that the strict codec must refuse.
		raw := append([]byte{0x00, 17}, bytes.Repeat([]byte{0xAA}, 17)...)
		_, _, err := codec.ReadFrame(bytes.NewReader(raw), nil)
		if !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("err: got %v want ErrMalformedFrame", err)
		}
	})

	t.Run("decode-rejects-oversize-long", func(t *testing.T) {
		raw := []byte{iface.FlagLong, 0, 0, 0, 0, 0, 0, 0, 17}
		raw = append(raw, bytes.Repeat([]byte{0xAA}, 17)...)
		_, _, err := codec.ReadFrame(bytes.NewReader(raw), nil)
		if !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("err: got %v want ErrMalformedFrame", err)
		}
	})
}

func TestFrameRejectsReservedFlags(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(MaxBodyUnlimited)
	_, _, err := codec.ReadFrame(bytes.NewReader([]byte{0x80, 0}), nil)
	if !errors.Is(err, zmqerr.ErrMalformedFrame) {
		t.Fatalf("decode err: got %v want ErrMalformedFrame", err)
	}
	if err := codec.WriteFrame(io.Discard, 0xF0, nil); !errors.Is(err, zmqerr.ErrMalformedFrame) {
		t.Fatalf("encode err: got %v want ErrMalformedFrame", err)
	}
}

func TestFrameTruncated(t *testing.T) {
	t.Parallel()
	codec := NewFrameCodec(MaxBodyUnlimited)

	t.Run("clean-eof", func(t *testing.T) {
		_, _, err := codec.ReadFrame(strings.NewReader(""), nil)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("err: got %v want io.EOF", err)
		}
	})
	t.Run("missing-length", func(t *testing.T) {
		_, _, err := codec.ReadFrame(bytes.NewReader([]byte{0x00}), nil)
		if !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("err: got %v want ErrMalformedFrame", err)
		}
	})
	t.Run("missing-body", func(t *testing.T) {
		_, _, err := codec.ReadFrame(bytes.NewReader([]byte{0x00, 0x05, 'h', 'i'}), nil)
		if !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("err: got %v want ErrMalformedFrame", err)
		}
	})
	t.Run("missing-long-length", func(t *testing.T) {
		_, _, err := codec.ReadFrame(bytes.NewReader([]byte{iface.FlagLong, 0, 0, 0}), nil)
		if !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("err: got %v want ErrMalformedFrame", err)
		}
	})
}

func TestFrameNewFrameCodecPanicsOnNegative(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic")
		}
	}()
	NewFrameCodec(-1)
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
		if werr := codec.WriteFrame(&buf, flags, body); werr != nil {
			t.Fatalf("re-encode failed for accepted frame: %v", werr)
		}
		flags2, body2, err2 := codec.ReadFrame(&buf, nil)
		if err2 != nil {
			t.Fatalf("re-decode failed: %v", err2)
		}
		// LONG is always set by re-encode for the round-trip when forced; mask it.
		if flags2 & ^iface.FlagLong != flags & ^iface.FlagLong {
			t.Fatalf("flags drift: %02x → %02x", flags, flags2)
		}
		if !bytes.Equal(body, body2) {
			t.Fatalf("body drift: %x → %x", body, body2)
		}
	})
}
