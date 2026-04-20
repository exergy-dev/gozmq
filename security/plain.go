// PLAIN security mechanism (ZMTP 3.1 §3.3, RFC 24, design §8.2).
//
// The PLAIN handshake is a short lock-step exchange layered on top of the
// greeting:
//
//	Client -> Server : HELLO [username][password]
//	Server -> Client : WELCOME  (or ERROR on auth failure)
//	Client -> Server : INITIATE  (no payload beyond the command name)
//	Server -> Client : READY  metadata
//	Client -> Server : READY  metadata  (if not yet sent)
//
// RFC 24 mandates an INITIATE command between WELCOME and the first READY so
// both peers agree that authentication succeeded before any metadata is
// exchanged. We follow that sequencing here so the wire is bit-for-bit
// interoperable with libzmq.
//
// As with NULL, NextHandshake produces command BODIES (name-length prefix +
// name + payload). The session layer wraps each body in a COMMAND-flagged
// ZMTP frame on the way out and strips the frame flags on the way in before
// handing the body back via NextHandshake.

package security

import (
	"fmt"
	"strings"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/rkm/gozmq/zmtp"
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
	plainClientSendReady

	// Server-side states. plainServerSeed is the passive initial state:
	// the session driver calls NextHandshake(nil) first on every mechanism
	// to seed the loop; for the server side this must return (nil, false,
	// nil) so the loop reads HELLO from the peer before advancing.
	plainServerSeed
	plainServerRecvHello
	plainServerRecvInitiate
	plainServerSendReady
	plainServerRecvReady

	// Terminal.
	plainDone
)

// plainMech implements iface.SecurityMech for the PLAIN mechanism.
//
// A single value is not safe for concurrent use; the session layer drives
// NextHandshake single-threaded.
type plainMech struct {
	role      iface.Role
	state     plainState
	username  string
	password  string
	authFn    func(user, pass string) bool
	localMeta map[string]string
	peerMeta  map[string]string
	metaCodec iface.MetadataCodec

	// readySent tracks whether our own READY has been emitted. The server
	// sends READY first (after validating INITIATE); the client sends READY
	// last, after receiving the server's READY.
	readySent bool
	// readyRecv tracks whether the peer's READY has been parsed.
	readyRecv bool
}

// NewPlainClient returns a PLAIN mechanism configured as the initiator. It
// advertises localMeta in its outbound READY. username and password are sent
// verbatim in HELLO, per RFC 24 §4 (length-prefixed, max 255 each). localMeta
// may be nil.
func NewPlainClient(username, password string, localMeta map[string]string) iface.SecurityMech {
	return &plainMech{
		role:      iface.RoleClient,
		state:     plainClientSendHello,
		username:  username,
		password:  password,
		localMeta: copyMeta(localMeta),
		metaCodec: zmtp.MetadataCodec,
	}
}

// NewPlainServer returns a PLAIN mechanism configured as the acceptor. The
// authenticate callback is invoked once on receipt of HELLO; returning false
// causes the mechanism to emit an ERROR command and return an error wrapping
// zmqerr.ErrAuthFailed from the next NextHandshake call so the session layer
// tears the connection down. A nil authenticate callback is equivalent to one
// that always returns false.
func NewPlainServer(authenticate func(user, pass string) bool, localMeta map[string]string) iface.SecurityMech {
	return &plainMech{
		role:      iface.RoleServer,
		state:     plainServerSeed,
		authFn:    authenticate,
		localMeta: copyMeta(localMeta),
		metaCodec: zmtp.MetadataCodec,
	}
}

// Name reports the greeting mechanism token, unpadded.
func (*plainMech) Name() string { return plainMechName }

// AsServer reports the value of the greeting as-server byte.
func (m *plainMech) AsServer() bool { return m.role == iface.RoleServer }

// EncodeFrame is the identity codec for PLAIN — no per-frame rewriting.
func (*plainMech) EncodeFrame(flags uint8, plain []byte) (uint8, []byte, error) {
	return flags, plain, nil
}

// DecodeFrame is the identity codec for PLAIN — no per-frame rewriting.
func (*plainMech) DecodeFrame(flags uint8, wire []byte) (uint8, []byte, error) {
	return flags, wire, nil
}

// Metadata returns a copy of the local metadata properties sent in READY.
func (m *plainMech) Metadata() map[string]string { return copyMeta(m.localMeta) }

// SetPeerMetadata stores the peer's parsed READY properties. PLAIN performs
// no additional validation; Socket-Type compatibility lives in the engine.
func (m *plainMech) SetPeerMetadata(props map[string]string) error {
	if props == nil {
		m.peerMeta = nil
		return nil
	}
	m.peerMeta = copyMeta(props)
	return nil
}

// PeerMetadata returns the parsed peer READY properties, or nil before the
// handshake completes. Exposed (like NULL's version) for testability; not part
// of iface.SecurityMech.
func (m *plainMech) PeerMetadata() map[string]string {
	if m.peerMeta == nil {
		return nil
	}
	return copyMeta(m.peerMeta)
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
			return nil, false, err
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
		// INITIATE is emitted with an empty payload; it just signals the
		// client has accepted the server's WELCOME.
		out := encodeCommand(plainCmdInitiate, nil)
		m.state = plainClientRecvReady
		return out, false, nil

	case plainClientRecvReady:
		if in == nil {
			return nil, false, fmt.Errorf("gozmq/security/plain: nil peer input awaiting READY: %w", zmqerr.ErrMalformedFrame)
		}
		name, payload, err := parseCommand(in)
		if err != nil {
			return nil, false, err
		}
		if name != plainCmdReady {
			return nil, false, fmt.Errorf("gozmq/security/plain: expected READY, got %q: %w", name, zmqerr.ErrMalformedFrame)
		}
		props, err := m.metaCodec.Decode(payload)
		if err != nil {
			return nil, false, err
		}
		m.peerMeta = props
		m.readyRecv = true
		m.state = plainClientSendReady
		return m.NextHandshake(nil)

	case plainClientSendReady:
		out := encodeCommand(plainCmdReady, m.metaCodec.Encode(m.localMeta))
		m.readySent = true
		m.state = plainDone
		return out, true, nil

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
		m.username = user
		m.password = pass
		if m.authFn == nil || !m.authFn(user, pass) {
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
		name, _, err := parseCommand(in)
		if err != nil {
			return nil, false, err
		}
		if name != plainCmdInitiate {
			return nil, false, fmt.Errorf("gozmq/security/plain: expected INITIATE, got %q: %w", name, zmqerr.ErrMalformedFrame)
		}
		m.state = plainServerSendReady
		return m.NextHandshake(nil)

	case plainServerSendReady:
		out := encodeCommand(plainCmdReady, m.metaCodec.Encode(m.localMeta))
		m.readySent = true
		m.state = plainServerRecvReady
		return out, false, nil

	case plainServerRecvReady:
		if in == nil {
			return nil, false, fmt.Errorf("gozmq/security/plain: nil peer input awaiting client READY: %w", zmqerr.ErrMalformedFrame)
		}
		name, payload, err := parseCommand(in)
		if err != nil {
			return nil, false, err
		}
		if name != plainCmdReady {
			return nil, false, fmt.Errorf("gozmq/security/plain: expected READY, got %q: %w", name, zmqerr.ErrMalformedFrame)
		}
		props, err := m.metaCodec.Decode(payload)
		if err != nil {
			return nil, false, err
		}
		m.peerMeta = props
		m.readyRecv = true
		m.state = plainDone
		return nil, true, nil

	case plainDone:
		return nil, true, nil
	}
	return nil, false, fmt.Errorf("gozmq/security/plain: unexpected state %d", m.state)
}

// ---------------------------------------------------------------------------
// Wire helpers (RFC 24 §4 command bodies).
// ---------------------------------------------------------------------------

// encodeCommand builds a ZMTP command BODY: [1 byte name-length][name][payload].
// The session layer wraps this in a COMMAND frame.
func encodeCommand(name string, payload []byte) []byte {
	if len(name) > 255 {
		// Programmer error: command names in ZMTP are short ASCII tokens.
		panic(fmt.Sprintf("gozmq/security/plain: command name %q exceeds 255 bytes", name))
	}
	out := make([]byte, 0, 1+len(name)+len(payload))
	out = append(out, byte(len(name)))
	out = append(out, name...)
	out = append(out, payload...)
	return out
}

// parseCommand splits a command body into (name, payload).
func parseCommand(body []byte) (string, []byte, error) {
	if len(body) < 1 {
		return "", nil, fmt.Errorf("gozmq/security/plain: empty command body: %w", zmqerr.ErrMalformedFrame)
	}
	nameLen := int(body[0])
	if 1+nameLen > len(body) {
		return "", nil, fmt.Errorf("gozmq/security/plain: command name truncated (declared %d, have %d): %w", nameLen, len(body)-1, zmqerr.ErrMalformedFrame)
	}
	// Command names in ZMTP are case-sensitive ASCII tokens, but libzmq
	// emits them uppercase; accept either to be robust.
	name := strings.ToUpper(string(body[1 : 1+nameLen]))
	return name, body[1+nameLen:], nil
}

// encodeHello builds the HELLO payload per RFC 24 §4:
//
//	[1 byte username-length][username][1 byte password-length][password]
//
// Usernames and passwords are capped at 255 bytes by the single-byte prefix.
func encodeHello(user, pass string) ([]byte, error) {
	if len(user) > 255 {
		return nil, fmt.Errorf("gozmq/security/plain: username length %d exceeds 255", len(user))
	}
	if len(pass) > 255 {
		return nil, fmt.Errorf("gozmq/security/plain: password length %d exceeds 255", len(pass))
	}
	payload := make([]byte, 0, 1+len(user)+1+len(pass))
	payload = append(payload, byte(len(user)))
	payload = append(payload, user...)
	payload = append(payload, byte(len(pass)))
	payload = append(payload, pass...)
	return encodeCommand(plainCmdHello, payload), nil
}

// parseHello parses the HELLO payload (NOT the full command body — the
// command-name prefix must already have been stripped by parseCommand).
func parseHello(payload []byte) (string, string, error) {
	if len(payload) < 1 {
		return "", "", fmt.Errorf("gozmq/security/plain: HELLO payload missing username length: %w", zmqerr.ErrMalformedFrame)
	}
	userLen := int(payload[0])
	if 1+userLen > len(payload) {
		return "", "", fmt.Errorf("gozmq/security/plain: HELLO username truncated (declared %d, have %d): %w", userLen, len(payload)-1, zmqerr.ErrMalformedFrame)
	}
	user := string(payload[1 : 1+userLen])
	rest := payload[1+userLen:]
	if len(rest) < 1 {
		return "", "", fmt.Errorf("gozmq/security/plain: HELLO payload missing password length: %w", zmqerr.ErrMalformedFrame)
	}
	passLen := int(rest[0])
	if 1+passLen > len(rest) {
		return "", "", fmt.Errorf("gozmq/security/plain: HELLO password truncated (declared %d, have %d): %w", passLen, len(rest)-1, zmqerr.ErrMalformedFrame)
	}
	pass := string(rest[1 : 1+passLen])
	// Trailing bytes after the declared password are malformed.
	if 1+passLen != len(rest) {
		return "", "", fmt.Errorf("gozmq/security/plain: HELLO has %d trailing bytes: %w", len(rest)-(1+passLen), zmqerr.ErrMalformedFrame)
	}
	return user, pass, nil
}

// encodeErrorReason builds an ERROR command payload: [1 byte length][reason].
func encodeErrorReason(reason string) []byte {
	if len(reason) > 255 {
		reason = reason[:255]
	}
	out := make([]byte, 0, 1+len(reason))
	out = append(out, byte(len(reason)))
	out = append(out, reason...)
	return out
}

// parseErrorReason parses an ERROR payload: [1 byte length][reason].
func parseErrorReason(payload []byte) (string, error) {
	if len(payload) < 1 {
		return "", fmt.Errorf("gozmq/security/plain: ERROR payload missing reason length: %w", zmqerr.ErrMalformedFrame)
	}
	reasonLen := int(payload[0])
	if 1+reasonLen > len(payload) {
		return "", fmt.Errorf("gozmq/security/plain: ERROR reason truncated (declared %d, have %d): %w", reasonLen, len(payload)-1, zmqerr.ErrMalformedFrame)
	}
	return string(payload[1 : 1+reasonLen]), nil
}

// copyMeta is a small helper used by both NewPlainClient/Server and
// Metadata/SetPeerMetadata to keep caller-visible maps isolated from stored
// state. Nil in, fresh empty map out (so constructors never hold nil for the
// local-metadata field, which simplifies the Encode path).
func copyMeta(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
