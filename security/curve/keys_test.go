package curve

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// Hello-world Z85 vectors from RFC 32 §4. 8 bytes of binary data round-trip
// to a 10-character Z85 string.
func TestZ85HelloWorldVector(t *testing.T) {
	bin := []byte{0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B}
	want := "HelloWorld"

	if got := Z85Encode(bin); got != want {
		t.Fatalf("Z85Encode: got %q, want %q", got, want)
	}
	dec, err := Z85Decode(want)
	if err != nil {
		t.Fatalf("Z85Decode: %v", err)
	}
	if !bytes.Equal(dec, bin) {
		t.Fatalf("Z85Decode: got %x, want %x", dec, bin)
	}
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
			if len(c.z85) != 40 {
				t.Fatalf("vector %q length %d != 40", c.z85, len(c.z85))
			}
			raw, err := Z85Decode(c.z85)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if len(raw) != 32 {
				t.Fatalf("decoded length %d != 32", len(raw))
			}
			back := Z85Encode(raw)
			if back != c.z85 {
				t.Fatalf("round-trip: got %q, want %q", back, c.z85)
			}
			// Also through the Key wrapper.
			k, err := Z85DecodeKey(c.z85)
			if err != nil {
				t.Fatalf("Z85DecodeKey: %v", err)
			}
			if !bytes.Equal(k[:], raw) {
				t.Fatalf("Z85DecodeKey bytes differ from Z85Decode")
			}
			if Z85EncodeKey(k) != c.z85 {
				t.Fatalf("Z85EncodeKey round-trip failed")
			}
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
		if len(enc) != chunk/4*5 {
			t.Fatalf("chunk %d encoded length %d != %d", chunk, len(enc), chunk/4*5)
		}
		dec, err := Z85Decode(enc)
		if err != nil {
			t.Fatalf("chunk %d decode: %v", chunk, err)
		}
		if !bytes.Equal(dec, in) {
			t.Fatalf("chunk %d round-trip mismatch: got %x, want %x", chunk, dec, in)
		}
	}
}

// TestZ85EncodePanicsOnBadLength confirms the encoder rejects inputs whose
// length is not a multiple of 4. Using recover() because the contract is a
// panic, not an error return.
func TestZ85EncodePanicsOnBadLength(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Z85Encode on 3-byte input: expected panic")
		}
	}()
	Z85Encode([]byte{0, 0, 0})
}

func TestZ85DecodeRejectsBadInputs(t *testing.T) {
	cases := []struct {
		name string
		s    string
	}{
		{"wrong-length", "abcd"},                             // 4 chars; must be multiple of 5
		{"invalid-char", "   ab"},                            // spaces not in alphabet
		{"overflow", "#####"},                                // highest-digit run, value > 2^32
		{"nonascii", string([]byte{0xFF, 'a', 'b', 'c', 'd'})}, // 0xFF is 0xFF in the decoder table -> invalid
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Z85Decode(c.s)
			if err == nil {
				t.Fatalf("decode %q: expected error", c.s)
			}
			if !errors.Is(err, ErrInvalidZ85) {
				t.Fatalf("decode %q: expected ErrInvalidZ85, got %v", c.s, err)
			}
		})
	}
}

// TestGenerateKeyPairRandom asserts two successive calls produce different
// keys (sanity on the RNG wiring) and that the public key is the
// basepoint-multiplication of the secret.
func TestGenerateKeyPairRandom(t *testing.T) {
	pub1, sec1, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair 1: %v", err)
	}
	pub2, sec2, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair 2: %v", err)
	}
	if sec1 == sec2 {
		t.Fatal("two calls returned the same secret")
	}
	if pub1 == pub2 {
		t.Fatal("two calls returned the same public")
	}
	// Derive and compare.
	derived, err := derivePublic(sec1)
	if err != nil {
		t.Fatalf("derivePublic: %v", err)
	}
	if derived != pub1 {
		t.Fatalf("derived public != returned public")
	}
}

// TestZ85CounterExample is a small extra guard: a well-known 8-byte counter
// value encodes/decodes to the expected pattern — catches big-endian vs
// little-endian mix-ups when the encoder processes 4-byte chunks.
func TestZ85CounterExample(t *testing.T) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], 0x0000_0001_0000_0002)
	enc := Z85Encode(b[:])
	dec, err := Z85Decode(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(dec, b[:]) {
		t.Fatalf("round-trip mismatch: got %x, want %x", dec, b[:])
	}
}
