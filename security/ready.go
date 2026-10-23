package security

import (
	"fmt"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
)

// Every ZMTP mechanism ends its handshake by exchanging a READY command
// carrying the local metadata properties (Socket-Type, Identity, and any
// mechanism-specific additions). EncodeReady and DecodeReady are that wire
// format, factored out of the NULL, PLAIN, and CURVE implementations so an
// out-of-tree [Mech] does not have to reimplement it — without them the
// interface would be public but not practically usable.
//
// This is encoding only: no cryptography, no policy. A mechanism that wraps
// frame bodies (CURVE) still seals the encoded READY itself.

// EncodeMetadata returns the ZMTP 3.1 §5.2 property-list encoding of meta:
// for each entry, a one-byte name length, the name, a four-byte big-endian
// value length, and the value. Mechanisms that carry metadata in something
// other than a bare READY — PLAIN's INITIATE, CURVE's sealed boxes — encode
// with this and frame it themselves.
//
// Property names must be at most 255 bytes and values at most 2^32-1 bytes;
// longer entries panic, since they cannot be produced by any valid
// configuration.
func EncodeMetadata(meta map[string]string) []byte {
	return zmtp.MetadataCodec.Encode(meta)
}

// DecodeMetadata parses a ZMTP 3.1 §5.2 property list. It returns an error
// wrapping gozmq.ErrMalformedFrame on a truncated or malformed list.
func DecodeMetadata(b []byte) (map[string]string, error) {
	return zmtp.MetadataCodec.Decode(b)
}

// EncodeReady returns the wire bytes of a ZMTP READY command whose body is
// the given metadata properties. Pass the map from [MechBase.Metadata] to
// advertise exactly what gozmq expects the peer to see.
//
// The same size limits as [EncodeMetadata] apply.
func EncodeReady(meta map[string]string) []byte {
	return zmtp.EncodeCommand(zmtp.Command{
		Name: zmtp.CmdReady,
		Body: EncodeMetadata(meta),
	})
}

// DecodeReady parses the wire bytes of a peer's READY command and returns
// its metadata properties. It returns an error wrapping
// gozmq.ErrMalformedFrame when body is not a well-formed READY — a
// truncated command, a different command name, or malformed properties.
//
// Feed the result to [MechBase.MergePeerMetadata] so the session layer sees
// the peer's identity and socket type. (The session layer runs the
// Socket-Type compatibility check itself after the handshake.)
//
// A peer that rejects the handshake answers with ERROR instead of READY;
// check for that with [DecodeError] first.
func DecodeReady(body []byte) (map[string]string, error) {
	cmd, err := zmtp.DecodeCommand(body)
	if err != nil {
		return nil, fmt.Errorf("gozmq/security: parse READY: %w", err)
	}
	if cmd.Name != zmtp.CmdReady {
		return nil, fmt.Errorf("gozmq/security: expected READY command, got %q: %w", cmd.Name, zmqerr.ErrMalformedFrame)
	}
	return DecodeMetadata(cmd.Body)
}

// DecodeError reports whether body is a ZMTP ERROR command: the reply a peer
// sends in place of the expected handshake command when it rejects the
// handshake, e.g. after a ZAP denial. isError is false for any other
// command, including one too malformed to name. When isError is true, reason
// is the peer's reason text, or err wraps gozmq.ErrMalformedFrame if that
// text is truncated. A mechanism should report a decoded ERROR as an error
// wrapping gozmq.ErrAuthFailed.
//
// It tolerates the mis-encoded ERROR name libzmq emits.
func DecodeError(body []byte) (reason string, isError bool, err error) {
	return zmtp.DecodeError(body)
}
