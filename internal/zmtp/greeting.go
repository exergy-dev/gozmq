package zmtp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/rkm/gozmq/internal/zmqerr"
)

// nonPrintableASCII reports whether r falls outside the printable ASCII range
// [0x20, 0x7E]. ZMTP restricts mechanism and command names to printable ASCII.
func nonPrintableASCII(r rune) bool { return r < 0x20 || r > 0x7E }

// Greeting layout constants per ZMTP 3.1 §2.1 (design.md §5.1).
const (
	GreetingSize       = 64
	greetingSigLen     = 10 // 0xFF + 8 padding bytes + 0x7F
	greetingMechLen    = 20
	greetingFillerLen  = 31
	GreetingVersionMaj = 0x03
	GreetingVersionMin = 0x01
)

// signaturePrefix and signatureTerminator bracket the 8-byte padding region of
// the greeting signature. They never change across ZMTP versions.
const (
	signaturePrefix     byte = 0xFF
	signatureTerminator byte = 0x7F
)

// Greeting is the parsed representation of the 64-byte ZMTP greeting block.
// Encoders set Version[0]=GreetingVersionMaj and Version[1]=GreetingVersionMin
// (or 0x00 for ZMTP 3.0 negotiation testing); decoders surface whatever the
// peer sent so the session layer can decide whether to disable heartbeats.
type Greeting struct {
	// Version is the major/minor ZMTP version reported by the peer. Only
	// 3.0 and 3.1 are accepted; everything else fails decode with
	// zmqerr.ErrProtocolVersion.
	Version [2]byte
	// Mechanism is the security-mechanism token (e.g. "NULL", "PLAIN",
	// "CURVE"), unpadded. The encoder pads to 20 bytes with NULs.
	Mechanism string
	// AsServer is the value of the as-server byte: true if this peer is the
	// security-mechanism server, false if it is the client.
	AsServer bool
	// HeartbeatCapable reports whether the peer advertised ZMTP 3.1 (true)
	// or only ZMTP 3.0 (false). The session layer must skip PING/PONG when
	// false. Set by ReadGreeting; ignored by WriteGreeting.
	HeartbeatCapable bool
}

// WriteGreeting encodes g and writes the resulting 64-byte block to w.
func WriteGreeting(w io.Writer, g Greeting) error {
	if len(g.Mechanism) > greetingMechLen {
		return fmt.Errorf("gozmq/zmtp: mechanism %q exceeds %d bytes: %w", g.Mechanism, greetingMechLen, zmqerr.ErrMalformedFrame)
	}
	if i := strings.IndexFunc(g.Mechanism, nonPrintableASCII); i >= 0 {
		return fmt.Errorf("gozmq/zmtp: mechanism %q contains non-printable byte 0x%02x: %w", g.Mechanism, g.Mechanism[i], zmqerr.ErrMalformedFrame)
	}

	var buf [GreetingSize]byte
	// Signature: 0xFF, 8 padding bytes (zero), 0x7F.
	buf[0] = signaturePrefix
	buf[9] = signatureTerminator
	// Version: caller-supplied so tests can produce 3.0 blobs. The default
	// (zero value) is rejected on read; producers normally pass {0x03, 0x01}.
	buf[10] = g.Version[0]
	buf[11] = g.Version[1]
	// Mechanism: ASCII, NUL-padded to 20 bytes.
	copy(buf[12:12+greetingMechLen], g.Mechanism)
	// As-server: byte at offset 32.
	if g.AsServer {
		buf[32] = 1
	}
	// Filler: 31 zero bytes — already zero from the array initialization.
	if _, err := w.Write(buf[:]); err != nil {
		return fmt.Errorf("gozmq/zmtp: write greeting: %w", err)
	}
	return nil
}

// ReadGreeting reads a 64-byte greeting from r and validates it. Returns
// zmqerr.ErrProtocolVersion (wrapped) for any unsupported major version, and
// zmqerr.ErrMalformedFrame (wrapped) for signature / mechanism / as-server
// errors.
func ReadGreeting(r io.Reader) (Greeting, error) {
	var buf [GreetingSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Greeting{}, fmt.Errorf("%w: greeting truncated: %w", zmqerr.ErrMalformedFrame, err)
		}
		return Greeting{}, fmt.Errorf("gozmq/zmtp: read greeting: %w", err)
	}

	if buf[0] != signaturePrefix {
		return Greeting{}, fmt.Errorf("gozmq/zmtp: bad signature prefix 0x%02x: %w", buf[0], zmqerr.ErrMalformedFrame)
	}
	if buf[9] != signatureTerminator {
		return Greeting{}, fmt.Errorf("gozmq/zmtp: bad signature terminator 0x%02x: %w", buf[9], zmqerr.ErrMalformedFrame)
	}

	major := buf[10]
	minor := buf[11]
	if major != GreetingVersionMaj {
		return Greeting{}, fmt.Errorf("gozmq/zmtp: unsupported major version 0x%02x: %w", major, zmqerr.ErrProtocolVersion)
	}
	// Accept any 3.x minor for forward compatibility (ZMTP revisions are
	// backward compatible within a major); minor >= 1 is treated as 3.1.
	// Matches the session-layer greeting decode and libzmq.

	mech, err := decodeMechanism(buf[12 : 12+greetingMechLen])
	if err != nil {
		return Greeting{}, err
	}

	asServerByte := buf[32]
	if asServerByte != 0 && asServerByte != 1 {
		return Greeting{}, fmt.Errorf("gozmq/zmtp: bad as-server byte 0x%02x: %w", asServerByte, zmqerr.ErrMalformedFrame)
	}
	// Filler bytes (33..63) are explicitly reserved as zero by the spec but
	// we tolerate non-zero values to interoperate with implementations that
	// stuff the filler with arbitrary bytes; this matches libzmq's behavior.

	return Greeting{
		Version:          [2]byte{major, minor},
		Mechanism:        mech,
		AsServer:         asServerByte == 1,
		HeartbeatCapable: minor >= 0x01,
	}, nil
}

// decodeMechanism parses a 20-byte NUL-padded ASCII mechanism token. The token
// is the prefix up to the first NUL; trailing non-NUL after a NUL byte is a
// protocol violation (per spec, padding must be NUL). All bytes must be
// printable ASCII.
func decodeMechanism(raw []byte) (string, error) {
	end := bytes.IndexByte(raw, 0)
	if end < 0 {
		end = len(raw)
	}
	for i := end; i < len(raw); i++ {
		if raw[i] != 0 {
			return "", fmt.Errorf("gozmq/zmtp: mechanism padding non-zero at offset %d (0x%02x): %w", i, raw[i], zmqerr.ErrMalformedFrame)
		}
	}
	if i := bytes.IndexFunc(raw[:end], nonPrintableASCII); i >= 0 {
		return "", fmt.Errorf("gozmq/zmtp: mechanism non-printable byte 0x%02x at offset %d: %w", raw[i], i, zmqerr.ErrMalformedFrame)
	}
	return string(raw[:end]), nil
}
