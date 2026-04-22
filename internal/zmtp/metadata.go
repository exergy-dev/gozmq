package zmtp

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// MetadataCodec is the package-level iface.MetadataCodec implementation for
// the ZMTP 3.1 READY property list (§2.3.1). Encoding preserves name case;
// decoding treats names case-insensitively for duplicate detection.
//
// Wire format per property:
//
//	[1 byte name-length][name][4 bytes big-endian value-length][value]
//
// Properties are concatenated; the list has no outer delimiter. An empty
// property list is the zero-length byte slice.
var MetadataCodec iface.MetadataCodec = metadataCodec{}

// NewMetadataCodec returns the package-level MetadataCodec. Kept as a factory
// so callers that prefer constructor-style instantiation can obtain it
// without reaching into package variables.
func NewMetadataCodec() iface.MetadataCodec { return MetadataCodec }

// metadataCodec implements iface.MetadataCodec. It is stateless; a single
// value is safe to share across sessions.
type metadataCodec struct{}

// Encode serializes props into the ZMTP 3.1 READY property-list wire form.
// The returned slice is freshly allocated and safe to retain. Property
// iteration order is unspecified (Go map order); peers must not depend on it.
//
// Properties whose name length exceeds 255 bytes, or whose value length
// exceeds 2^32-1 bytes, panic — these are programmer errors in this codebase
// since we control every caller. (ZMTP lets values reach 2^32-1, so the panic
// only fires on pathological input.)
func (metadataCodec) Encode(props map[string]string) []byte {
	// Pre-compute size so we allocate exactly once.
	total := 0
	for name, value := range props {
		if len(name) > 255 {
			panic(fmt.Sprintf("gozmq/zmtp: metadata name length %d exceeds 255", len(name)))
		}
		if uint64(len(value)) > 0xFFFFFFFF {
			panic(fmt.Sprintf("gozmq/zmtp: metadata value length %d exceeds 2^32-1", len(value)))
		}
		total += 1 + len(name) + 4 + len(value)
	}

	out := make([]byte, 0, total)
	for name, value := range props {
		out = append(out, byte(len(name)))
		out = append(out, name...)
		var vln [4]byte
		binary.BigEndian.PutUint32(vln[:], uint32(len(value)))
		out = append(out, vln[:]...)
		out = append(out, value...)
	}
	return out
}

// Decode parses a ZMTP 3.1 READY property list into a map. Names are kept
// in their on-wire case, but duplicate names (compared case-insensitively per
// ZMTP 3.1 §2.3.1) are rejected with an error wrapping zmqerr.ErrMalformedFrame.
// Truncated input — any short read of a length prefix or its payload —
// produces the same wrapped sentinel.
func (metadataCodec) Decode(raw []byte) (map[string]string, error) {
	out := make(map[string]string)
	seen := make(map[string]struct{}) // lowercase names

	for i := 0; i < len(raw); {
		// Name length (1 byte).
		if i+1 > len(raw) {
			return nil, fmt.Errorf("gozmq/zmtp: metadata truncated at name length: %w", zmqerr.ErrMalformedFrame)
		}
		nameLen := int(raw[i])
		i++

		if i+nameLen > len(raw) {
			return nil, fmt.Errorf("gozmq/zmtp: metadata truncated in name (want %d bytes, have %d): %w", nameLen, len(raw)-i, zmqerr.ErrMalformedFrame)
		}
		name := string(raw[i : i+nameLen])
		i += nameLen

		// Value length (4 bytes big-endian).
		if i+4 > len(raw) {
			return nil, fmt.Errorf("gozmq/zmtp: metadata truncated at value length for %q: %w", name, zmqerr.ErrMalformedFrame)
		}
		valueLen := binary.BigEndian.Uint32(raw[i : i+4])
		i += 4

		if uint64(i)+uint64(valueLen) > uint64(len(raw)) {
			return nil, fmt.Errorf("gozmq/zmtp: metadata truncated in value for %q (want %d bytes, have %d): %w", name, valueLen, len(raw)-i, zmqerr.ErrMalformedFrame)
		}
		value := string(raw[i : i+int(valueLen)])
		i += int(valueLen)

		lower := strings.ToLower(name)
		if _, dup := seen[lower]; dup {
			return nil, fmt.Errorf("gozmq/zmtp: duplicate metadata property %q: %w", name, zmqerr.ErrMalformedFrame)
		}
		seen[lower] = struct{}{}
		out[name] = value
	}
	return out, nil
}
