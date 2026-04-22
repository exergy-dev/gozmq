package zmtp

import (
	"bytes"
	"errors"
	"testing"

	"github.com/rkm/gozmq/internal/zmqerr"
)

func TestGreetingRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   Greeting
		want Greeting
	}{
		{
			name: "null-client",
			in:   Greeting{Version: [2]byte{0x03, 0x01}, Mechanism: "NULL", AsServer: false},
			want: Greeting{Version: [2]byte{0x03, 0x01}, Mechanism: "NULL", AsServer: false, HeartbeatCapable: true},
		},
		{
			name: "plain-server",
			in:   Greeting{Version: [2]byte{0x03, 0x01}, Mechanism: "PLAIN", AsServer: true},
			want: Greeting{Version: [2]byte{0x03, 0x01}, Mechanism: "PLAIN", AsServer: true, HeartbeatCapable: true},
		},
		{
			name: "curve-client",
			in:   Greeting{Version: [2]byte{0x03, 0x01}, Mechanism: "CURVE", AsServer: false},
			want: Greeting{Version: [2]byte{0x03, 0x01}, Mechanism: "CURVE", AsServer: false, HeartbeatCapable: true},
		},
		{
			name: "zmtp-30-no-heartbeat",
			in:   Greeting{Version: [2]byte{0x03, 0x00}, Mechanism: "NULL", AsServer: false},
			want: Greeting{Version: [2]byte{0x03, 0x00}, Mechanism: "NULL", AsServer: false, HeartbeatCapable: false},
		},
		{
			name: "max-mechanism",
			in:   Greeting{Version: [2]byte{0x03, 0x01}, Mechanism: "TWENTY-CHAR-MECH-ABC"},
			want: Greeting{Version: [2]byte{0x03, 0x01}, Mechanism: "TWENTY-CHAR-MECH-ABC", HeartbeatCapable: true},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			if err := WriteGreeting(&buf, tc.in); err != nil {
				t.Fatalf("WriteGreeting: %v", err)
			}
			if buf.Len() != GreetingSize {
				t.Fatalf("encoded size %d want %d", buf.Len(), GreetingSize)
			}
			got, err := ReadGreeting(&buf)
			if err != nil {
				t.Fatalf("ReadGreeting: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %+v want %+v", got, tc.want)
			}
		})
	}
}

func TestGreetingWireFormat(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	if err := WriteGreeting(&buf, Greeting{Version: [2]byte{0x03, 0x01}, Mechanism: "NULL", AsServer: true}); err != nil {
		t.Fatalf("WriteGreeting: %v", err)
	}
	b := buf.Bytes()
	if b[0] != signaturePrefix {
		t.Fatalf("sig prefix: got 0x%02x", b[0])
	}
	for i := 1; i < 9; i++ {
		if b[i] != 0 {
			t.Fatalf("padding[%d] non-zero: 0x%02x", i, b[i])
		}
	}
	if b[9] != signatureTerminator {
		t.Fatalf("sig terminator: got 0x%02x", b[9])
	}
	if b[10] != 0x03 || b[11] != 0x01 {
		t.Fatalf("version: %02x %02x", b[10], b[11])
	}
	if string(b[12:16]) != "NULL" {
		t.Fatalf("mechanism: %q", b[12:16])
	}
	for i := 16; i < 32; i++ {
		if b[i] != 0 {
			t.Fatalf("mechanism padding[%d] non-zero: 0x%02x", i, b[i])
		}
	}
	if b[32] != 1 {
		t.Fatalf("as-server: 0x%02x", b[32])
	}
	for i := 33; i < GreetingSize; i++ {
		if b[i] != 0 {
			t.Fatalf("filler[%d] non-zero: 0x%02x", i, b[i])
		}
	}
}

func TestGreetingRejects(t *testing.T) {
	t.Parallel()
	good := func() []byte {
		var buf bytes.Buffer
		if err := WriteGreeting(&buf, Greeting{Version: [2]byte{0x03, 0x01}, Mechanism: "NULL"}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		return buf.Bytes()
	}
	cases := []struct {
		name    string
		mutate  func([]byte)
		wantErr error
	}{
		{
			name:    "bad-sig-prefix",
			mutate:  func(b []byte) { b[0] = 0x00 },
			wantErr: zmqerr.ErrMalformedFrame,
		},
		{
			name:    "bad-sig-terminator",
			mutate:  func(b []byte) { b[9] = 0x00 },
			wantErr: zmqerr.ErrMalformedFrame,
		},
		{
			name:    "bad-major",
			mutate:  func(b []byte) { b[10] = 0x02 },
			wantErr: zmqerr.ErrProtocolVersion,
		},
		{
			name:    "bad-minor",
			mutate:  func(b []byte) { b[11] = 0xFF },
			wantErr: zmqerr.ErrProtocolVersion,
		},
		{
			name:    "bad-as-server",
			mutate:  func(b []byte) { b[32] = 0x05 },
			wantErr: zmqerr.ErrMalformedFrame,
		},
		{
			name:    "non-printable-mechanism",
			mutate:  func(b []byte) { b[12] = 0x01 },
			wantErr: zmqerr.ErrMalformedFrame,
		},
		{
			name:    "padding-non-zero",
			mutate:  func(b []byte) { b[20] = 0x42 }, // after "NULL" + 4 NULs of padding
			wantErr: zmqerr.ErrMalformedFrame,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := good()
			tc.mutate(b)
			_, err := ReadGreeting(bytes.NewReader(b))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err: got %v want %v", err, tc.wantErr)
			}
		})
	}
}

func TestGreetingTruncated(t *testing.T) {
	t.Parallel()
	_, err := ReadGreeting(bytes.NewReader([]byte{0xFF, 0x00, 0x7F}))
	if !errors.Is(err, zmqerr.ErrMalformedFrame) {
		t.Fatalf("err: got %v want ErrMalformedFrame", err)
	}
}

func TestGreetingMechanismTooLong(t *testing.T) {
	t.Parallel()
	err := WriteGreeting(&bytes.Buffer{}, Greeting{Version: [2]byte{0x03, 0x01}, Mechanism: "0123456789012345678901"})
	if !errors.Is(err, zmqerr.ErrMalformedFrame) {
		t.Fatalf("err: got %v want ErrMalformedFrame", err)
	}
}

// FuzzGreeting feeds arbitrary 64-byte (or shorter) blobs through the
// greeting parser; success requires the parser to never panic and any
// accepted greeting to round-trip via WriteGreeting back to the same struct.
func FuzzGreeting(f *testing.F) {
	seed := func(g Greeting) []byte {
		var buf bytes.Buffer
		_ = WriteGreeting(&buf, g)
		return buf.Bytes()
	}
	f.Add(seed(Greeting{Version: [2]byte{0x03, 0x01}, Mechanism: "NULL"}))
	f.Add(seed(Greeting{Version: [2]byte{0x03, 0x00}, Mechanism: "PLAIN", AsServer: true}))
	f.Add(make([]byte, 64))                                       // all zeros — bad signature
	f.Add(make([]byte, 10))                                       // truncated
	f.Add([]byte{0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0x7F, 0x09, 0x09}) // bad version

	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := ReadGreeting(bytes.NewReader(data))
		if err != nil {
			return
		}
		var buf bytes.Buffer
		if werr := WriteGreeting(&buf, got); werr != nil {
			t.Fatalf("re-encode of accepted greeting failed: %v", werr)
		}
		got2, err2 := ReadGreeting(&buf)
		if err2 != nil {
			t.Fatalf("re-decode of accepted greeting failed: %v", err2)
		}
		if got != got2 {
			t.Fatalf("round-trip drift: %+v → %+v", got, got2)
		}
	})
}
