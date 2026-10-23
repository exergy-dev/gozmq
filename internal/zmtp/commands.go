package zmtp

import (
	"bytes"
	"fmt"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
)

// Canonical ZMTP 3.1 command names used by more than one package.
const (
	CmdReady = "READY"
	CmdError = "ERROR"
)

// MaxCommandNameLen is the inclusive upper bound on a command name length:
// the wire encoding has a single-byte length prefix.
const MaxCommandNameLen = 255

// nonPrintableASCII reports whether r falls outside the printable ASCII range
// [0x20, 0x7E], to which ZMTP restricts command names.
func nonPrintableASCII(r rune) bool { return r < 0x20 || r > 0x7E }

// Command is a parsed ZMTP command frame. Body is the command-specific
// payload (a property list for READY, a reason for ERROR, TTL plus context
// for PING, ...); the schema is the caller's.
type Command struct {
	Name string
	Body []byte
}

// AppendCommandName appends the command header, a one-byte length and the
// name, to dst. It panics on a name over MaxCommandNameLen: that is a
// programming error, and silently truncating it would put a malformed frame
// on the wire.
func AppendCommandName(dst []byte, name string) []byte {
	if len(name) > MaxCommandNameLen {
		panic(fmt.Sprintf("gozmq/zmtp: command name %q exceeds %d bytes", name, MaxCommandNameLen))
	}
	dst = append(dst, byte(len(name)))
	return append(dst, name...)
}

// EncodeCommand serializes c as the body of a COMMAND-flagged frame. The
// flags byte and length prefix belong to the framing layer.
func EncodeCommand(c Command) []byte {
	out := AppendCommandName(make([]byte, 0, 1+len(c.Name)+len(c.Body)), c.Name)
	return append(out, c.Body...)
}

// libzmqBrokenErrorPrefix is what libzmq's plain_server_t::produce_error
// writes instead of a well-formed ERROR command name: the C literal
// "\x5ERROR" was meant as 0x05 "ERROR", but \x5E swallows the 'E', so the
// bytes are 0x5E 'R' 'R' 'O' 'R', a name of declared length 94 with four
// bytes behind it. Confirmed against libzmq 4.3.4. A decoder that rejects it
// would report every bad-password handshake against a real libzmq server as
// a malformed frame instead of an authentication failure.
var libzmqBrokenErrorPrefix = []byte{0x5E, 'R', 'R', 'O', 'R'}

// DecodeCommand parses the body of a COMMAND-flagged frame. The returned
// Body aliases raw. It accepts exactly one malformed encoding, libzmq's
// broken ERROR name (see libzmqBrokenErrorPrefix), and only after the strict
// parse has failed on it.
func DecodeCommand(raw []byte) (Command, error) {
	if len(raw) < 1 {
		return Command{}, fmt.Errorf("gozmq/zmtp: command empty: %w", zmqerr.ErrMalformedFrame)
	}
	nameLen := int(raw[0])
	if nameLen == 0 {
		return Command{}, fmt.Errorf("gozmq/zmtp: command name length zero: %w", zmqerr.ErrMalformedFrame)
	}
	if 1+nameLen > len(raw) {
		if bytes.HasPrefix(raw, libzmqBrokenErrorPrefix) {
			return Command{Name: CmdError, Body: raw[len(libzmqBrokenErrorPrefix):]}, nil
		}
		return Command{}, fmt.Errorf("gozmq/zmtp: command name length %d exceeds body %d: %w", nameLen, len(raw)-1, zmqerr.ErrMalformedFrame)
	}
	name := raw[1 : 1+nameLen]
	if i := bytes.IndexFunc(name, nonPrintableASCII); i >= 0 {
		return Command{}, fmt.Errorf("gozmq/zmtp: command name non-printable byte 0x%02x at offset %d: %w", name[i], i, zmqerr.ErrMalformedFrame)
	}
	return Command{Name: string(name), Body: raw[1+nameLen:]}, nil
}

// ExpectCommand decodes raw and returns its body if it is the command name,
// else an error wrapping ErrMalformedFrame. Names compare byte-exact, as
// libzmq's memcmp does.
func ExpectCommand(raw []byte, name string) ([]byte, error) {
	cmd, err := DecodeCommand(raw)
	if err != nil {
		return nil, fmt.Errorf("gozmq/zmtp: %s: %w", name, err)
	}
	if cmd.Name != name {
		return nil, fmt.Errorf("gozmq/zmtp: expected %s, got %q: %w", name, cmd.Name, zmqerr.ErrMalformedFrame)
	}
	return cmd.Body, nil
}

// EncodeError builds an ERROR command body carrying reason, cut to the 255
// bytes its one-byte length allows.
func EncodeError(reason string) []byte {
	if len(reason) > 255 {
		reason = reason[:255]
	}
	body := AppendCommandName(make([]byte, 0, 1+len(CmdError)+1+len(reason)), CmdError)
	body = append(body, byte(len(reason)))
	return append(body, reason...)
}

// DecodeErrorReason parses an ERROR payload: a one-byte length and the reason.
func DecodeErrorReason(payload []byte) (string, error) {
	if len(payload) < 1 || 1+int(payload[0]) > len(payload) {
		return "", fmt.Errorf("gozmq/zmtp: ERROR reason truncated: %w", zmqerr.ErrMalformedFrame)
	}
	return string(payload[1 : 1+payload[0]]), nil
}

// DecodeError reports whether raw is an ERROR command. isError is false for
// any other command, including one too malformed to name; when it is true,
// reason is the peer's text, or err wraps ErrMalformedFrame if that text is
// truncated.
func DecodeError(raw []byte) (reason string, isError bool, err error) {
	cmd, perr := DecodeCommand(raw)
	if perr != nil || cmd.Name != CmdError {
		return "", false, nil //nolint:nilerr // not an ERROR; the caller's own parser reports malformed input
	}
	reason, err = DecodeErrorReason(cmd.Body)
	return reason, true, err
}

// SubscriptionFrame is the single-frame message carrying a subscription:
// 0x01 (subscribe) or 0x00 (cancel) followed by the topic prefix. ZMTP 3.0
// peers send it as data; 3.1 peers send SUBSCRIBE/CANCEL commands, which the
// session turns into this form so the engines see one shape.
func SubscriptionFrame(subscribe bool, prefix []byte) []byte {
	b := make([]byte, 1+len(prefix))
	if subscribe {
		b[0] = 1
	}
	copy(b[1:], prefix)
	return b
}
