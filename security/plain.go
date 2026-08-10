// PLAIN security mechanism (ZMTP 3.1 §3.3, RFC 24, design §8.2).
//
// The PLAIN handshake is a short lock-step exchange layered on top of the
// greeting:
//
//	Client -> Server : HELLO [username][password]
//	Server -> Client : WELCOME  (or ERROR on auth failure)
//	Client -> Server : INITIATE  metadata
//	Server -> Client : READY  metadata
//
// RFC 24 carries the client's metadata in INITIATE and the server's metadata
// in READY. The server completes the PLAIN handshake as soon as it emits
// READY; there is no trailing client READY exchange.
//
// As with NULL, NextHandshake produces command BODIES (name-length prefix +
// name + payload). The session layer wraps each body in a COMMAND-flagged
// ZMTP frame on the way out and strips the frame flags on the way in before
// handing the body back via NextHandshake.

package security

import (
	"bytes"
	"fmt"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
)

// Mechanism token transmitted in the greeting (ZMTP 3.1 §3.3.1).
const plainMechName = "PLAIN"

// PLAIN command names, per RFC 24 §4.
const (
	plainCmdHello    = "HELLO"
	plainCmdWelcome  = "WELCOME"
	plainCmdInitiate = "INITIATE"
	plainCmdError    = "ERROR"
	plainCmdReady    = "READY"
)

// plainState names the state of the handshake state machine. States progress
// monotonically; transitions are driven by NextHandshake.
type plainState uint8

const (
	// Client-side states.
	plainClientSendHello plainState = iota
	plainClientRecvWelcome
	plainClientSendInitiate
	plainClientRecvReady

	// Server-side states. plainServerSeed is the passive initial state:
	// the session driver calls NextHandshake(nil) first on every mechanism
	// to seed the loop; for the server side this must return (nil, false,
	// nil) so the loop reads HELLO from the peer before advancing.
	plainServerSeed
	plainServerRecvHello
	plainServerRecvInitiate
	plainServerSendReady

	// Terminal.
	plainDone
)

// plainMech implements Mech for the PLAIN mechanism.
//
// A single value is not safe for concurrent use; the session layer drives
// NextHandshake single-threaded.
type plainMech struct {
	MechBase
	state    plainState
	username string
	password string
	authFn   func(user, pass string) bool
}

// NewPlainClient returns a PLAIN mechanism configured as the initiator. It
// advertises localMeta in its outbound INITIATE. username and password are
// sent verbatim in HELLO, per RFC 24 §4 (length-prefixed, max 255 each).
// localMeta may be nil.
func NewPlainClient(username, password string, localMeta map[string]string) Mech {
	return &plainMech{
		MechBase: NewMechBase(RoleClient, localMeta),
		state:    plainClientSendHello,
		username: username,
		password: password,
	}
}

// NewPlainServer returns a PLAIN mechanism configured as the acceptor. The
// authenticate callback is invoked once on receipt of HELLO; returning false
// causes the mechanism to emit an ERROR command and return an error wrapping
// gozmq.ErrAuthFailed from the next NextHandshake call so the session layer
// tears the connection down. A nil authenticate callback is equivalent to one
// that always returns false.
//
// Implementations of authenticate should compare the supplied password using
// crypto/subtle.ConstantTimeCompare (or compare salted hashes) rather than
// ==, so a network peer cannot use response timing to probe the password
// byte-by-byte. The credentials are not retained by the mechanism after the
// authentication decision is made.
func NewPlainServer(authenticate func(user, pass string) bool, localMeta map[string]string) Mech {
	return &plainMech{
		MechBase: NewMechBase(RoleServer, localMeta),
		state:    plainServerSeed,
		authFn:   authenticate,
	}
}

// Name reports the greeting mechanism token, unpadded.
func (*plainMech) Name() string { return plainMechName }

// EncodeFrame is the identity codec for PLAIN — no per-frame rewriting.
func (*plainMech) EncodeFrame(flags uint8, plain []byte) (uint8, []byte, error) {
	return flags, plain, nil
}

// DecodeFrame is the identity codec for PLAIN — no per-frame rewriting.
func (*plainMech) DecodeFrame(flags uint8, wire []byte) (uint8, []byte, error) {
	return flags, wire, nil
}

// NextHandshake drives the PLAIN handshake state machine. See the package
// header comment for the state sequence.
func (m *plainMech) NextHandshake(in []byte) ([]byte, bool, error) {
	switch m.state {
	// ------------------------------------------------------------------ Client
	case plainClientSendHello:
		if in != nil {
			return nil, false, fmt.Errorf("gozmq/security/plain: unexpected input on client seed: %w", zmqerr.ErrMalformedFrame)
		}
		out, err := encodeHello(m.username, m.password)
		if err != nil {
			return nil, false, err
		}
		m.state = plainClientRecvWelcome
		return out, false, nil

	case plainClientRecvWelcome:
		if in == nil {
			return nil, false, fmt.Errorf("gozmq/security/plain: nil peer input awaiting WELCOME: %w", zmqerr.ErrMalformedFrame)
		}
		name, payload, err := parseCommand(in)
		if err != nil {
			return nil, false, fmt.Errorf("gozmq/security/plain: parse WELCOME/ERROR body %x: %w", in, err)
		}
		switch name {
		case plainCmdWelcome:
			// WELCOME has no payload per RFC 24 §4.
			if len(payload) != 0 {
				return nil, false, fmt.Errorf("gozmq/security/plain: WELCOME carries unexpected payload (%d bytes): %w", len(payload), zmqerr.ErrMalformedFrame)
			}
			m.state = plainClientSendInitiate
			return m.NextHandshake(nil)
		case plainCmdError:
			reason, perr := parseErrorReason(payload)
			if perr != nil {
				return nil, false, perr
			}
			return nil, false, fmt.Errorf("gozmq/security/plain: server rejected credentials: %q: %w", reason, zmqerr.ErrAuthFailed)
		default:
			return nil, false, fmt.Errorf("gozmq/security/plain: expected WELCOME or ERROR, got %q: %w", name, zmqerr.ErrMalformedFrame)
		}

	case plainClientSendInitiate:
		out := encodeCommand(plainCmdInitiate, zmtp.MetadataCodec.Encode(m.localMeta))
		m.state = plainClientRecvReady
		return out, false, nil

	case plainClientRecvReady:
		if in == nil {
			return nil, false, fmt.Errorf("gozmq/security/plain: nil peer input awaiting READY: %w", zmqerr.ErrMalformedFrame)
		}
		name, payload, err := parseCommand(in)
		if err != nil {
			return nil, false, fmt.Errorf("gozmq/security/plain: parse READY body %x: %w", in, err)
		}
		switch name {
		case plainCmdError:
			reason, perr := parseErrorReason(payload)
			if perr != nil {
				return nil, false, perr
			}
			return nil, false, fmt.Errorf("gozmq/security/plain: server rejected INITIATE: %q: %w", reason, zmqerr.ErrAuthFailed)
		case plainCmdReady:
			props, err := zmtp.MetadataCodec.Decode(payload)
			if err != nil {
				return nil, false, err
			}
			m.MergePeerMetadata(props)
			m.state = plainDone
			return nil, true, nil
		default:
			return nil, false, fmt.Errorf("gozmq/security/plain: expected READY or ERROR, got %q: %w", name, zmqerr.ErrMalformedFrame)
		}

	// ------------------------------------------------------------------ Server
	case plainServerSeed:
		// Passive seed — let the session driver loop read HELLO next.
		if in != nil {
			return nil, false, fmt.Errorf("gozmq/security/plain: unexpected input on server seed: %w", zmqerr.ErrMalformedFrame)
		}
		m.state = plainServerRecvHello
		return nil, false, nil

	case plainServerRecvHello:
		if in == nil {
			return nil, false, fmt.Errorf("gozmq/security/plain: nil peer input awaiting HELLO: %w", zmqerr.ErrMalformedFrame)
		}
		name, payload, err := parseCommand(in)
		if err != nil {
			return nil, false, err
		}
		if name != plainCmdHello {
			return nil, false, fmt.Errorf("gozmq/security/plain: expected HELLO, got %q: %w", name, zmqerr.ErrMalformedFrame)
		}
		user, pass, err := parseHello(payload)
		if err != nil {
			return nil, false, err
		}
		// The credentials stay in locals only and are dropped as soon as
		// the auth decision below is made.
		switch {
		case m.zapAuth != nil:
			if err := m.AuthorizeZAP(plainMechName, [][]byte{[]byte(user), []byte(pass)}); err != nil {
				out := encodeCommand(plainCmdError, encodeErrorReason("Invalid username or password"))
				m.state = plainDone
				return out, false, fmt.Errorf("gozmq/security/plain: ZAP rejected user %q: %w", user, err)
			}
		case m.authFn == nil || !m.authFn(user, pass):
			// Emit ERROR and tear down. The reason text is short and
			// opaque per RFC 24's guidance not to leak authentication
			// detail.
			out := encodeCommand(plainCmdError, encodeErrorReason("Invalid username or password"))
			m.state = plainDone
			return out, false, fmt.Errorf("gozmq/security/plain: authentication failed for user %q: %w", user, zmqerr.ErrAuthFailed)
		}
		// Success: emit WELCOME, await INITIATE.
		out := encodeCommand(plainCmdWelcome, nil)
		m.state = plainServerRecvInitiate
		return out, false, nil

	case plainServerRecvInitiate:
		if in == nil {
			return nil, false, fmt.Errorf("gozmq/security/plain: nil peer input awaiting INITIATE: %w", zmqerr.ErrMalformedFrame)
		}
		name, payload, err := parseCommand(in)
		if err != nil {
			return nil, false, err
		}
		if name != plainCmdInitiate {
			return nil, false, fmt.Errorf("gozmq/security/plain: expected INITIATE, got %q: %w", name, zmqerr.ErrMalformedFrame)
		}
		props, err := zmtp.MetadataCodec.Decode(payload)
		if err != nil {
			return nil, false, err
		}
		m.MergePeerMetadata(props)
		m.state = plainServerSendReady
		return m.NextHandshake(nil)

	case plainServerSendReady:
		out := encodeCommand(plainCmdReady, zmtp.MetadataCodec.Encode(m.localMeta))
		m.state = plainDone
		return out, true, nil

	case plainDone:
		return nil, true, nil
	}
	return nil, false, fmt.Errorf("gozmq/security/plain: unexpected state %d", m.state)
}

// ---------------------------------------------------------------------------
// Wire helpers (RFC 24 §4 command bodies).
// ---------------------------------------------------------------------------

// encodeCommand builds a ZMTP command BODY ([1 byte name-length][name][payload])
// via the canonical zmtp codec. The session layer wraps this in a COMMAND frame.
func encodeCommand(name string, payload []byte) []byte {
	return zmtp.EncodeCommand(zmtp.Command{Name: name, Body: payload})
}

// parseCommand splits a command body into (name, payload). Command names in
// ZMTP are exact (case-sensitive) ASCII tokens, so no normalization is
// applied.
func parseCommand(body []byte) (string, []byte, error) {
	if len(body) < 1 {
		return "", nil, fmt.Errorf("gozmq/security/plain: empty command body: %w", zmqerr.ErrMalformedFrame)
	}
	nameLen := int(body[0])
	if 1+nameLen > len(body) {
		if name, payload, ok := parseLibzmqBrokenError(body); ok {
			return name, payload, nil
		}
		return "", nil, fmt.Errorf("gozmq/security/plain: command name truncated (declared %d, have %d): %w", nameLen, len(body)-1, zmqerr.ErrMalformedFrame)
	}
	return string(body[1 : 1+nameLen]), body[1+nameLen:], nil
}

// libzmqBrokenErrorPrefix is the exact byte sequence libzmq emits instead of
// a well-formed ERROR command name.
//
// libzmq's plain_server_t::produce_error writes the literal "\x5ERROR",
// intending a 0x05 length byte followed by "ERROR". In C, \x hex escapes
// consume as many hex digits as follow, so \x5E swallows the 'E': the
// literal is really the four characters 0x5E 'R' 'R' 'O' 'R'. The result is
// a command whose declared name length is 94 with only four name bytes
// behind it.
//
// Confirmed against libzmq 4.3.4. A gozmq client that rejects this — which
// a strict reading of ZMTP 3.1 §5.1 requires — reports every bad-password
// handshake against a real libzmq server as a malformed frame instead of an
// authentication failure, which is both wrong and actively misleading.
var libzmqBrokenErrorPrefix = []byte{0x5E, 'R', 'R', 'O', 'R'}

// parseLibzmqBrokenError recognises that one malformed encoding and nothing
// else. It runs only after the strict parse has already failed, and matches
// the full five-byte prefix, so no other malformed name is accepted: a
// genuinely corrupt frame still fails.
func parseLibzmqBrokenError(body []byte) (name string, payload []byte, ok bool) {
	if !bytes.HasPrefix(body, libzmqBrokenErrorPrefix) {
		return "", nil, false
	}
	return plainCmdError, body[len(libzmqBrokenErrorPrefix):], true
}

// putShortString appends the RFC 24 short-string form — [1 byte length][bytes]
// — of s to dst. Strings are capped at 255 bytes by the single-byte prefix.
func putShortString(dst []byte, s, what string) ([]byte, error) {
	if len(s) > 255 {
		return nil, fmt.Errorf("gozmq/security/plain: %s length %d exceeds 255", what, len(s))
	}
	dst = append(dst, byte(len(s)))
	return append(dst, s...), nil
}

// getShortString parses one [1 byte length][bytes] short string from the
// front of b, returning the string and the remaining bytes.
func getShortString(b []byte, what string) (string, []byte, error) {
	if len(b) < 1 {
		return "", nil, fmt.Errorf("gozmq/security/plain: missing %s length: %w", what, zmqerr.ErrMalformedFrame)
	}
	n := int(b[0])
	if 1+n > len(b) {
		return "", nil, fmt.Errorf("gozmq/security/plain: %s truncated (declared %d, have %d): %w", what, n, len(b)-1, zmqerr.ErrMalformedFrame)
	}
	return string(b[1 : 1+n]), b[1+n:], nil
}

// encodeHello builds the HELLO payload per RFC 24 §4:
//
//	[1 byte username-length][username][1 byte password-length][password]
func encodeHello(user, pass string) ([]byte, error) {
	payload := make([]byte, 0, 1+len(user)+1+len(pass))
	payload, err := putShortString(payload, user, "username")
	if err != nil {
		return nil, err
	}
	payload, err = putShortString(payload, pass, "password")
	if err != nil {
		return nil, err
	}
	return encodeCommand(plainCmdHello, payload), nil
}

// parseHello parses the HELLO payload (NOT the full command body — the
// command-name prefix must already have been stripped by parseCommand).
func parseHello(payload []byte) (string, string, error) {
	user, rest, err := getShortString(payload, "HELLO username")
	if err != nil {
		return "", "", err
	}
	pass, rest, err := getShortString(rest, "HELLO password")
	if err != nil {
		return "", "", err
	}
	// Trailing bytes after the declared password are malformed.
	if len(rest) != 0 {
		return "", "", fmt.Errorf("gozmq/security/plain: HELLO has %d trailing bytes: %w", len(rest), zmqerr.ErrMalformedFrame)
	}
	return user, pass, nil
}

// encodeErrorReason builds an ERROR command payload: [1 byte length][reason].
func encodeErrorReason(reason string) []byte {
	if len(reason) > 255 {
		reason = reason[:255]
	}
	out, _ := putShortString(make([]byte, 0, 1+len(reason)), reason, "reason") // length pre-clamped; cannot fail
	return out
}

// parseErrorReason parses an ERROR payload: [1 byte length][reason].
func parseErrorReason(payload []byte) (string, error) {
	reason, _, err := getShortString(payload, "ERROR reason")
	return reason, err
}
