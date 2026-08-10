package zmtp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
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

	codec := MetadataCodec
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire := codec.Encode(tc.props)
			got, err := codec.Decode(wire)
			require.NoError(t, err, "Decode")
			require.Len(t, got, len(tc.props), "roundtrip prop count")
			for k, v := range tc.props {
				assert.Equal(t, v, got[k], "roundtrip: key %q", k)
			}
		})
	}
}

func TestMetadataEncodeLongValueOver255Bytes(t *testing.T) {
	// Values >255 bytes exercise the 4-byte value-length encoding path.
	codec := MetadataCodec
	big := strings.Repeat("x", 1000)
	wire := codec.Encode(map[string]string{"Big": big})

	// Expect: 1 (name-len) + 3 ("Big") + 4 (value-len) + 1000 = 1008.
	require.Equal(t, 1008, len(wire), "wire length")
	// The 4-byte big-endian value length should read 1000.
	vln := binary.BigEndian.Uint32(wire[1+3 : 1+3+4])
	require.Equal(t, uint32(1000), vln, "wire value-length")

	got, err := codec.Decode(wire)
	require.NoError(t, err, "Decode")
	require.Equal(t, big, got["Big"], "roundtrip long value")
}

func TestMetadataDecodeCaseInsensitiveDuplicate(t *testing.T) {
	// Names are case-insensitive per ZMTP 3.1 §2.3.1 — "Socket-Type" and
	// "socket-type" in the same list must be rejected.
	wire := bytes.Join([][]byte{
		encodeProp("Socket-Type", []byte("REQ")),
		encodeProp("socket-type", []byte("REP")),
	}, nil)

	_, err := MetadataCodec.Decode(wire)
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
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
		0:                       true, // empty list
		len(prop1):              true, // just prop1
		len(prop1) + len(prop2): true, // full list
	}

	for i := 1; i < len(good); i++ {
		if boundaries[i] {
			continue
		}
		t.Run(fmt.Sprintf("truncate-at-%d", i), func(t *testing.T) {
			_, err := MetadataCodec.Decode(good[:i])
			require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, "Decode(truncated)")
		})
	}
}

func TestMetadataDecodeEmptyIsNotDuplicate(t *testing.T) {
	// Zero-length property list decodes to an empty map without error.
	got, err := MetadataCodec.Decode(nil)
	require.NoError(t, err, "Decode(nil)")
	require.Empty(t, got, "Decode(nil)")
	got, err = MetadataCodec.Decode([]byte{})
	require.NoError(t, err, "Decode(empty)")
	require.Empty(t, got, "Decode(empty)")
}

func TestMetadataDecodeZeroLengthName(t *testing.T) {
	// An explicit zero-length-name, zero-length-value property is legal
	// on the wire (ZMTP doesn't forbid it). It must decode to a map entry
	// with the empty-string key.
	wire := encodeProp("", []byte{})
	got, err := MetadataCodec.Decode(wire)
	require.NoError(t, err, "Decode")
	require.Len(t, got, 1, "Decode prop count")
	v, ok := got[""]
	require.True(t, ok, "Decode: empty-name entry missing")
	require.Equal(t, "", v, "Decode: empty-name entry value")
}

// TestMetadataRoundtripRandom is a testing/quick-style randomized roundtrip
// check. Generates property maps with random ASCII names and random-length
// values, encodes, decodes, and asserts equality.
func TestMetadataRoundtripRandom(t *testing.T) {
	codec := MetadataCodec
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
		require.NoError(t, err, "iter %d: Decode", iter)
		require.Len(t, got, len(props), "iter %d: prop count", iter)
		for k, v := range props {
			require.Equal(t, v, got[k], "iter %d: key %q", iter, k)
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

	codec := MetadataCodec
	f.Fuzz(func(t *testing.T, raw []byte) {
		props, err := codec.Decode(raw)
		if err != nil {
			// Every decode error must wrap ErrMalformedFrame.
			require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, "Decode error does not wrap ErrMalformedFrame")
			return
		}
		// Successful decode: re-encode and redecode; result must equal.
		wire := codec.Encode(props)
		props2, err := codec.Decode(wire)
		require.NoError(t, err, "re-Decode of re-Encode failed")
		require.Len(t, props2, len(props), "prop count mismatch")
		for k, v := range props {
			require.Equal(t, v, props2[k], "key %q", k)
		}
	})
}
