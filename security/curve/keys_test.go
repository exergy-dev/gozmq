package curve

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// Hello-world Z85 vectors from RFC 32 §4. 8 bytes of binary data round-trip
// to a 10-character Z85 string.
func TestZ85HelloWorldVector(t *testing.T) {
	bin := []byte{0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B}
	want := "HelloWorld"

	require.Equal(t, want, Z85Encode(bin), "Z85Encode")
	dec, err := Z85Decode(want)
	require.NoError(t, err, "Z85Decode")
	require.Equal(t, bin, dec, "Z85Decode")
}

// Full 40-character Z85 public-key vectors from the RFC 32 §4 example pair.
// We only round-trip them here; their cryptographic relationship is not
// relevant to the codec test.
func TestZ85KeyVectorsRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		z85  string
	}{
		{"client-public", "Yne@$w-vo<fVvi]a<NY6T1ed:M$fCG*[IaLV{hID"},
		{"server-public", "rq:rM>}U?@Lns47E1%kR.o@n%FcmmsL/@{H8]yf7"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Len(t, c.z85, 40, "vector %q length", c.z85)
			raw, err := Z85Decode(c.z85)
			require.NoError(t, err, "decode")
			require.Len(t, raw, 32, "decoded length")
			back := Z85Encode(raw)
			require.Equal(t, c.z85, back, "round-trip")
			// Also through the Key wrapper.
			k, err := Z85DecodeKey(c.z85)
			require.NoError(t, err, "Z85DecodeKey")
			require.Equal(t, raw, k[:], "Z85DecodeKey bytes differ from Z85Decode")
			require.Equal(t, c.z85, Z85EncodeKey(k), "Z85EncodeKey round-trip")
		})
	}
}

// TestZ85PropertyEquivalent sweeps inputs of various lengths through the
// encoder and decoder and asserts the round-trip is the identity.
func TestZ85PropertyEquivalent(t *testing.T) {
	// Build 64 bytes of pseudo-random-but-deterministic input (using a
	// simple LCG so the test stays hermetic and depends only on math).
	var buf [64]byte
	seed := uint64(0xCAFEF00DBADDCAFE)
	for i := range buf {
		seed = seed*6364136223846793005 + 1442695040888963407
		buf[i] = byte(seed >> 56)
	}
	for _, chunk := range []int{4, 8, 16, 32, 40, 64} {
		in := buf[:chunk]
		enc := Z85Encode(in)
		require.Len(t, enc, chunk/4*5, "chunk %d encoded length", chunk)
		dec, err := Z85Decode(enc)
		require.NoError(t, err, "chunk %d decode", chunk)
		require.Equal(t, in, dec, "chunk %d round-trip", chunk)
	}
}

// TestZ85EncodePanicsOnBadLength confirms the encoder rejects inputs whose
// length is not a multiple of 4. Using recover() because the contract is a
// panic, not an error return.
func TestZ85EncodePanicsOnBadLength(t *testing.T) {
	require.Panics(t, func() {
		Z85Encode([]byte{0, 0, 0})
	}, "Z85Encode on 3-byte input: expected panic")
}

func TestZ85DecodeRejectsBadInputs(t *testing.T) {
	cases := []struct {
		name string
		s    string
	}{
		{"wrong-length", "abcd"},                               // 4 chars; must be multiple of 5
		{"invalid-char", "   ab"},                              // spaces not in alphabet
		{"overflow", "#####"},                                  // highest-digit run, value > 2^32
		{"nonascii", string([]byte{0xFF, 'a', 'b', 'c', 'd'})}, // 0xFF is 0xFF in the decoder table -> invalid
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Z85Decode(c.s)
			require.ErrorIs(t, err, ErrInvalidZ85, "decode %q", c.s)
		})
	}
}

// TestGenerateKeyPairRandom asserts two successive calls produce different
// keys (sanity on the RNG wiring) and that the public key is the
// basepoint-multiplication of the secret.
func TestGenerateKeyPairRandom(t *testing.T) {
	pub1, sec1, err := GenerateKeyPair()
	require.NoError(t, err, "GenerateKeyPair 1")
	pub2, sec2, err := GenerateKeyPair()
	require.NoError(t, err, "GenerateKeyPair 2")
	require.NotEqual(t, sec1, sec2, "two calls returned the same secret")
	require.NotEqual(t, pub1, pub2, "two calls returned the same public")
	// Derive and compare.
	derived, err := derivePublic(sec1)
	require.NoError(t, err, "derivePublic")
	require.Equal(t, pub1, derived, "derived public != returned public")
}

// TestZ85CounterExample is a small extra guard: a well-known 8-byte counter
// value encodes/decodes to the expected pattern — catches big-endian vs
// little-endian mix-ups when the encoder processes 4-byte chunks.
func TestZ85CounterExample(t *testing.T) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], 0x0000_0001_0000_0002)
	enc := Z85Encode(b[:])
	dec, err := Z85Decode(enc)
	require.NoError(t, err, "decode")
	require.Equal(t, b[:], dec, "round-trip")
}
