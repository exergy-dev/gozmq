package zmtp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/rkm/gozmq/internal/zmqerr"
)

// encodeProp is a test helper that writes the raw wire bytes for a single
// property, without going through the codec under test. Used to craft
// fixtures (duplicate names, truncated inputs, long values).
func encodeProp(name string, value []byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte(byte(len(name)))
	buf.WriteString(name)
	var vln [4]byte
	binary.BigEndian.PutUint32(vln[:], uint32(len(value)))
	buf.Write(vln[:])
	buf.Write(value)
	return buf.Bytes()
}

func TestMetadataEncodeDecode(t *testing.T) {
	cases := []struct {
		name  string
		props map[string]string
	}{
		{"empty", map[string]string{}},
		{"single", map[string]string{"Socket-Type": "REQ"}},
		{"multi", map[string]string{
			"Socket-Type": "DEALER",
			"Identity":    "frontend-1",
			"Resource":    "/api/v1",
		}},
		{"empty-value", map[string]string{"Identity": ""}},
		{"binary-value", map[string]string{"Identity": "\x00\x01\x02\xff"}},
		{"long-value", map[string]string{
			"Socket-Type": "ROUTER",
			"X-Big":       strings.Repeat("a", 1024),
		}},
	}

	codec := NewMetadataCodec()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire := codec.Encode(tc.props)
			got, err := codec.Decode(wire)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if len(got) != len(tc.props) {
				t.Fatalf("roundtrip: got %d props, want %d", len(got), len(tc.props))
			}
			for k, v := range tc.props {
				if gv, ok := got[k]; !ok || gv != v {
					t.Errorf("roundtrip: key %q: got %q, want %q", k, gv, v)
				}
			}
		})
	}
}

func TestMetadataEncodeLongValueOver255Bytes(t *testing.T) {
	// Values >255 bytes exercise the 4-byte value-length encoding path.
	codec := NewMetadataCodec()
	big := strings.Repeat("x", 1000)
	wire := codec.Encode(map[string]string{"Big": big})

	// Expect: 1 (name-len) + 3 ("Big") + 4 (value-len) + 1000 = 1008.
	if got := len(wire); got != 1008 {
		t.Fatalf("wire length: got %d, want 1008", got)
	}
	// The 4-byte big-endian value length should read 1000.
	vln := binary.BigEndian.Uint32(wire[1+3 : 1+3+4])
	if vln != 1000 {
		t.Fatalf("wire value-length: got %d, want 1000", vln)
	}

	got, err := codec.Decode(wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got["Big"] != big {
		t.Fatalf("roundtrip long value: mismatch")
	}
}

func TestMetadataDecodeCaseInsensitiveDuplicate(t *testing.T) {
	// Names are case-insensitive per ZMTP 3.1 §2.3.1 — "Socket-Type" and
	// "socket-type" in the same list must be rejected.
	wire := bytes.Join([][]byte{
		encodeProp("Socket-Type", []byte("REQ")),
		encodeProp("socket-type", []byte("REP")),
	}, nil)

	_, err := NewMetadataCodec().Decode(wire)
	if err == nil {
		t.Fatal("Decode: want error, got nil")
	}
	if !errors.Is(err, zmqerr.ErrMalformedFrame) {
		t.Fatalf("Decode: want ErrMalformedFrame, got %v", err)
	}
}

func TestMetadataDecodeTruncatedInputs(t *testing.T) {
	// Build a known-valid two-property wire image, then truncate at every
	// offset and verify each truncation is rejected with ErrMalformedFrame.
	// Offsets that happen to land on a property boundary are valid on their
	// own (partial-list is a well-formed list), so we skip those.
	prop1 := encodeProp("Socket-Type", []byte("ROUTER"))
	prop2 := encodeProp("Identity", []byte("peer-1"))
	good := append(append([]byte{}, prop1...), prop2...)

	boundaries := map[int]bool{
		0:                          true, // empty list
		len(prop1):                 true, // just prop1
		len(prop1) + len(prop2):    true, // full list
	}

	for i := 1; i < len(good); i++ {
		if boundaries[i] {
			continue
		}
		i := i
		t.Run(fmt.Sprintf("truncate-at-%d", i), func(t *testing.T) {
			_, err := NewMetadataCodec().Decode(good[:i])
			if err == nil {
				t.Fatal("Decode(truncated): want error, got nil")
			}
			if !errors.Is(err, zmqerr.ErrMalformedFrame) {
				t.Fatalf("Decode(truncated): want ErrMalformedFrame, got %v", err)
			}
		})
	}
}

func TestMetadataDecodeEmptyIsNotDuplicate(t *testing.T) {
	// Zero-length property list decodes to an empty map without error.
	got, err := NewMetadataCodec().Decode(nil)
	if err != nil {
		t.Fatalf("Decode(nil): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Decode(nil): got %d props, want 0", len(got))
	}
	got, err = NewMetadataCodec().Decode([]byte{})
	if err != nil {
		t.Fatalf("Decode(empty): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Decode(empty): got %d props, want 0", len(got))
	}
}

func TestMetadataDecodeZeroLengthName(t *testing.T) {
	// An explicit zero-length-name, zero-length-value property is legal
	// on the wire (ZMTP doesn't forbid it). It must decode to a map entry
	// with the empty-string key.
	wire := encodeProp("", []byte{})
	got, err := NewMetadataCodec().Decode(wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Decode: got %d props, want 1", len(got))
	}
	if v, ok := got[""]; !ok || v != "" {
		t.Fatalf("Decode: empty-name entry missing or wrong: %q/%v", v, ok)
	}
}

// TestMetadataRoundtripRandom is a testing/quick-style randomized roundtrip
// check. Generates property maps with random ASCII names and random-length
// values, encodes, decodes, and asserts equality.
func TestMetadataRoundtripRandom(t *testing.T) {
	codec := NewMetadataCodec()
	rng := rand.New(rand.NewSource(1))

	for iter := 0; iter < 200; iter++ {
		nprops := rng.Intn(8) // 0..7
		props := make(map[string]string, nprops)
		for i := 0; i < nprops; i++ {
			// Unique name per iteration; ASCII letters keep the
			// case-insensitive dedup predictable (we lowercase to check).
			name := fmt.Sprintf("Key%d-%d", iter, i)
			vlen := rng.Intn(600) // span the 1-byte/4-byte split
			value := make([]byte, vlen)
			for j := range value {
				value[j] = byte(rng.Intn(256))
			}
			props[name] = string(value)
		}
		wire := codec.Encode(props)
		got, err := codec.Decode(wire)
		if err != nil {
			t.Fatalf("iter %d: Decode: %v", iter, err)
		}
		if len(got) != len(props) {
			t.Fatalf("iter %d: prop count: got %d, want %d", iter, len(got), len(props))
		}
		for k, v := range props {
			if gv, ok := got[k]; !ok || gv != v {
				t.Fatalf("iter %d: key %q mismatch: got %q, want %q", iter, k, gv, v)
			}
		}
	}
}

// FuzzMetadata drives the property-list decoder with arbitrary bytes and
// asserts that (a) no decoder panics, and (b) any successful decode
// round-trips back to bytes that themselves decode to the same map.
func FuzzMetadata(f *testing.F) {
	f.Add([]byte{})
	f.Add(encodeProp("Socket-Type", []byte("REQ")))
	f.Add(bytes.Join([][]byte{
		encodeProp("Socket-Type", []byte("DEALER")),
		encodeProp("Identity", []byte("x")),
	}, nil))
	// Truncated input — valuable regression fixture.
	f.Add([]byte{0x03, 'a', 'b', 'c', 0x00, 0x00, 0x00})
	// Case-insensitive duplicate.
	f.Add(bytes.Join([][]byte{
		encodeProp("Socket-Type", []byte("REQ")),
		encodeProp("socket-type", []byte("REP")),
	}, nil))

	codec := NewMetadataCodec()
	f.Fuzz(func(t *testing.T, raw []byte) {
		props, err := codec.Decode(raw)
		if err != nil {
			// Every decode error must wrap ErrMalformedFrame.
			if !errors.Is(err, zmqerr.ErrMalformedFrame) {
				t.Fatalf("Decode error does not wrap ErrMalformedFrame: %v", err)
			}
			return
		}
		// Successful decode: re-encode and redecode; result must equal.
		wire := codec.Encode(props)
		props2, err := codec.Decode(wire)
		if err != nil {
			t.Fatalf("re-Decode of re-Encode failed: %v", err)
		}
		if len(props) != len(props2) {
			t.Fatalf("prop count: first=%d re=%d", len(props), len(props2))
		}
		for k, v := range props {
			if props2[k] != v {
				t.Fatalf("key %q: first=%q re=%q", k, v, props2[k])
			}
		}
	})
}
