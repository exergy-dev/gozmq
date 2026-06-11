package zmtp

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

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
			require.NoError(t, WriteGreeting(&buf, tc.in), "WriteGreeting")
			require.Equal(t, GreetingSize, buf.Len(), "encoded size")
			got, err := ReadGreeting(&buf)
			require.NoError(t, err, "ReadGreeting")
			require.Equal(t, tc.want, got)
		})
	}
}

func TestGreetingWireFormat(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	require.NoError(t, WriteGreeting(&buf, Greeting{Version: [2]byte{0x03, 0x01}, Mechanism: "NULL", AsServer: true}), "WriteGreeting")
	b := buf.Bytes()
	require.Equal(t, signaturePrefix, b[0], "sig prefix")
	for i := 1; i < 9; i++ {
		require.Equal(t, byte(0), b[i], "padding[%d] non-zero", i)
	}
	require.Equal(t, signatureTerminator, b[9], "sig terminator")
	require.Equal(t, byte(0x03), b[10], "version major")
	require.Equal(t, byte(0x01), b[11], "version minor")
	require.Equal(t, "NULL", string(b[12:16]), "mechanism")
	for i := 16; i < 32; i++ {
		require.Equal(t, byte(0), b[i], "mechanism padding[%d] non-zero", i)
	}
	require.Equal(t, byte(1), b[32], "as-server")
	for i := 33; i < GreetingSize; i++ {
		require.Equal(t, byte(0), b[i], "filler[%d] non-zero", i)
	}
}

func TestGreetingRejects(t *testing.T) {
	t.Parallel()
	good := func() []byte {
		var buf bytes.Buffer
		require.NoError(t, WriteGreeting(&buf, Greeting{Version: [2]byte{0x03, 0x01}, Mechanism: "NULL"}), "seed")
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
			require.ErrorIs(t, err, tc.wantErr)
		})
	}

	// Forward compatibility: any 3.x minor is accepted; minor >= 1 implies
	// heartbeat capability (treated as 3.1).
	t.Run("future-minor-accepted", func(t *testing.T) {
		t.Parallel()
		b := good()
		b[11] = 0x07
		g, err := ReadGreeting(bytes.NewReader(b))
		require.NoError(t, err)
		require.Equal(t, [2]byte{0x03, 0x07}, g.Version)
		require.True(t, g.HeartbeatCapable)
	})
}

func TestGreetingTruncated(t *testing.T) {
	t.Parallel()
	_, err := ReadGreeting(bytes.NewReader([]byte{0xFF, 0x00, 0x7F}))
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
}

func TestGreetingMechanismTooLong(t *testing.T) {
	t.Parallel()
	err := WriteGreeting(&bytes.Buffer{}, Greeting{Version: [2]byte{0x03, 0x01}, Mechanism: "0123456789012345678901"})
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
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
		require.NoError(t, WriteGreeting(&buf, got), "re-encode of accepted greeting failed")
		got2, err2 := ReadGreeting(&buf)
		require.NoError(t, err2, "re-decode of accepted greeting failed")
		require.Equal(t, got, got2, "round-trip drift")
	})
}
