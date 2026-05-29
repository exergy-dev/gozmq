package zmtp

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/rkm/gozmq/internal/zmqerr"
)

// Canonical ZMTP 3.1 command names. The spec mandates ASCII; the Command
// encoder/decoder enforces that mandate independent of these constants.
const (
	CmdReady      = "READY"
	CmdError      = "ERROR"
	CmdSubscribe  = "SUBSCRIBE"
	CmdCancel     = "CANCEL"
	CmdPing       = "PING"
	CmdPong       = "PONG"
	CmdDisconnect = "DISCONNECT"
)

// MaxCommandNameLen is the inclusive upper bound on a command name length.
// The on-wire encoding uses a single-byte length prefix, so 255 is the spec's
// hard ceiling.
const MaxCommandNameLen = 255

// PingPongMaxContext is the inclusive upper bound on the trailing context
// blob carried by PING and PONG (ZMTP 3.1 §3.5.1).
const PingPongMaxContext = 16

// pingPongTTLBytes is the length of the big-endian TTL prefix in PING/PONG
// bodies.
const pingPongTTLBytes = 2

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

// EncodePing builds a PING command per ZMTP 3.1 §3.5.1: 2-byte big-endian TTL
// (hundredths of a second) followed by an opaque context blob (≤16 bytes).
// Contexts longer than PingPongMaxContext are rejected with a malformed-frame
// error so callers cannot silently produce invalid PINGs.
// encodePingPong builds a PING or PONG command body (TTL + echoed context),
// shared by EncodePing/EncodePong since the wire shape is identical.
func encodePingPong(name string, ttl uint16, context []byte) (Command, error) {
	if len(context) > PingPongMaxContext {
		return Command{}, fmt.Errorf("gozmq/zmtp: %s context %d bytes exceeds %d: %w", name, len(context), PingPongMaxContext, zmqerr.ErrMalformedFrame)
	}
	body := make([]byte, pingPongTTLBytes+len(context))
	binary.BigEndian.PutUint16(body[:pingPongTTLBytes], ttl)
	copy(body[pingPongTTLBytes:], context)
	return Command{Name: name, Body: body}, nil
}

func EncodePing(ttl uint16, context []byte) (Command, error) {
	return encodePingPong(CmdPing, ttl, context)
}

// DecodePing parses a PING command. The returned context aliases c.Body; copy
// before retaining past the next frame read.
func DecodePing(c Command) (ttl uint16, context []byte, err error) {
	if c.Name != CmdPing {
		return 0, nil, fmt.Errorf("gozmq/zmtp: DecodePing on %q: %w", c.Name, zmqerr.ErrMalformedFrame)
	}
	return decodePingPongBody(c.Body)
}

// EncodePong builds a PONG. PONG carries the same TTL+context shape as PING;
// the responder echoes the PING context verbatim so the originator can match
// the round-trip. We do not enforce that the originator ever sets TTL on a
// PONG (the spec is silent on its semantics there) — encoding is symmetric
// with EncodePing.
func EncodePong(ttl uint16, context []byte) (Command, error) {
	return encodePingPong(CmdPong, ttl, context)
}

// DecodePong parses a PONG command. The returned context aliases c.Body.
func DecodePong(c Command) (ttl uint16, context []byte, err error) {
	if c.Name != CmdPong {
		return 0, nil, fmt.Errorf("gozmq/zmtp: DecodePong on %q: %w", c.Name, zmqerr.ErrMalformedFrame)
	}
	return decodePingPongBody(c.Body)
}

func decodePingPongBody(body []byte) (uint16, []byte, error) {
	if len(body) < pingPongTTLBytes {
		return 0, nil, fmt.Errorf("gozmq/zmtp: PING/PONG body %d bytes shorter than TTL: %w", len(body), zmqerr.ErrMalformedFrame)
	}
	if len(body) > pingPongTTLBytes+PingPongMaxContext {
		return 0, nil, fmt.Errorf("gozmq/zmtp: PING/PONG body %d bytes exceeds %d: %w", len(body), pingPongTTLBytes+PingPongMaxContext, zmqerr.ErrMalformedFrame)
	}
	ttl := binary.BigEndian.Uint16(body[:pingPongTTLBytes])
	return ttl, body[pingPongTTLBytes:], nil
}
