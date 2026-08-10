package zmtp

import (
	"bytes"
	"fmt"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
)

// Canonical ZMTP 3.1 command names used by the security layer. The spec
// mandates ASCII; the Command encoder/decoder enforces that mandate
// independent of these constants.
const (
	CmdReady = "READY"
	CmdError = "ERROR"
)

// MaxCommandNameLen is the inclusive upper bound on a command name length.
// The on-wire encoding uses a single-byte length prefix, so 255 is the spec's
// hard ceiling.
const MaxCommandNameLen = 255

// nonPrintableASCII reports whether r falls outside the printable ASCII range
// [0x20, 0x7E]. ZMTP restricts command names to printable ASCII.
func nonPrintableASCII(r rune) bool { return r < 0x20 || r > 0x7E }

// Command is a parsed ZMTP command frame. Body is the command-specific payload
// — for READY/ERROR/SUBSCRIBE/CANCEL/DISCONNECT the body is the property list
// or subscription topic; for PING/PONG it is TTL+context. Higher-level helpers
// own the schema-specific layout.
type Command struct {
	Name string
	Body []byte
}

// EncodeCommand serializes c as the body of a COMMAND-flagged ZMTP frame:
// [nameLen byte][name bytes][body bytes...]. The frame flags byte and length
// prefix belong to the framing layer, not this function.
func EncodeCommand(c Command) []byte {
	// An empty name is illegal per ZMTP 3.1 §2.2, but we emit the bytes the
	// caller asked for; the receiver enforces non-emptiness on decode.
	if len(c.Name) > MaxCommandNameLen {
		// Programmer error (caller should validate before encode); panic so
		// the bug surfaces in tests rather than producing invalid wire bytes.
		panic(fmt.Sprintf("gozmq/zmtp: command name %q exceeds %d bytes", c.Name, MaxCommandNameLen))
	}
	out := make([]byte, 1+len(c.Name)+len(c.Body))
	out[0] = byte(len(c.Name))
	copy(out[1:], c.Name)
	copy(out[1+len(c.Name):], c.Body)
	return out
}

// DecodeCommand parses the body of a COMMAND-flagged ZMTP frame back into a
// Command. The returned Command.Body aliases raw; callers that intend to
// retain it past the next read must copy.
func DecodeCommand(raw []byte) (Command, error) {
	if len(raw) < 1 {
		return Command{}, fmt.Errorf("gozmq/zmtp: command empty: %w", zmqerr.ErrMalformedFrame)
	}
	nameLen := int(raw[0])
	if nameLen == 0 {
		return Command{}, fmt.Errorf("gozmq/zmtp: command name length zero: %w", zmqerr.ErrMalformedFrame)
	}
	if 1+nameLen > len(raw) {
		return Command{}, fmt.Errorf("gozmq/zmtp: command name length %d exceeds body %d: %w", nameLen, len(raw)-1, zmqerr.ErrMalformedFrame)
	}
	name := raw[1 : 1+nameLen]
	// Spec restricts command names to printable ASCII.
	if i := bytes.IndexFunc(name, nonPrintableASCII); i >= 0 {
		return Command{}, fmt.Errorf("gozmq/zmtp: command name non-printable byte 0x%02x at offset %d: %w", name[i], i, zmqerr.ErrMalformedFrame)
	}
	return Command{
		Name: string(name),
		Body: raw[1+nameLen:],
	}, nil
}
