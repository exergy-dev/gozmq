package zmtp

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
)

// EncodeMetadata and DecodeMetadata are the ZMTP 3.1 READY property list
// (§2.3.1). Encoding preserves name case; decoding treats names
// case-insensitively for duplicate detection. Wire format per property:
//
//	[1 byte name-length][name][4 bytes big-endian value-length][value]
//
// Properties are concatenated with no outer delimiter; an empty list is the
// empty slice.

// EncodeMetadata serializes props into the ZMTP 3.1 READY property-list wire form.
// The returned slice is freshly allocated and safe to retain. Property
// iteration order is unspecified (Go map order); peers must not depend on it.
//
// Properties outside the limits ValidateMetadata checks panic: every caller
// in this codebase controls its input.
func EncodeMetadata(props map[string]string) []byte {
	if err := ValidateMetadata(props); err != nil {
		panic(err)
	}
	total := 0 // size the output once
	for name, value := range props {
		total += 1 + len(name) + 4 + len(value)
	}

	out := make([]byte, 0, total)
	for name, value := range props {
		out = append(out, byte(len(name)))
		out = append(out, name...)
		out = binary.BigEndian.AppendUint32(out, uint32(len(value)))
		out = append(out, value...)
	}
	return out
}

// ValidateMetadata reports whether props fits a property list (names up to
// 255 bytes, values up to 2^32-1). Callers whose properties come from
// outside (a ZAP handler's reply) check with it before EncodeMetadata.
func ValidateMetadata(props map[string]string) error {
	for name, value := range props {
		if len(name) > 255 {
			return fmt.Errorf("gozmq/zmtp: metadata name length %d exceeds 255", len(name))
		}
		if uint64(len(value)) > math.MaxUint32 {
			return fmt.Errorf("gozmq/zmtp: metadata %q value length %d exceeds 2^32-1", name, len(value))
		}
	}
	return nil
}

// DecodeMetadata parses a ZMTP 3.1 READY property list into a map. Names are kept
// in their on-wire case, but duplicate names (compared case-insensitively per
// ZMTP 3.1 §2.3.1) are rejected with an error wrapping zmqerr.ErrMalformedFrame.
// Truncated input — any short read of a length prefix or its payload —
// produces the same wrapped sentinel.
func DecodeMetadata(raw []byte) (map[string]string, error) {
	out := make(map[string]string)
	seen := make(map[string]struct{}) // lowercase names

	for i := 0; i < len(raw); {
		// Name length (1 byte). The loop guard guarantees raw[i] is in range.
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
