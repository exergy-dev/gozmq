// PLAIN security mechanism (ZMTP 3.1 §3.3, RFC 24).
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
	"fmt"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
)

// Mechanism token transmitted in the greeting (ZMTP 3.1 §3.3.1).
const (
	plainMechName = "PLAIN"
	plainName     = "plain" // in error text
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
	identityFraming
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

// NextHandshake drives the PLAIN handshake state machine. See the package
// header comment for the state sequence.
func (m *plainMech) NextHandshake(in []byte) ([]byte, bool, error) {
	switch m.state {
	// ------------------------------------------------------------------ Client
	case plainClientSendHello:
		if err := RequireNoInput(plainName, in, "client seed"); err != nil {
			return nil, false, err
		}
		out, err := encodeHello(m.username, m.password)
		if err != nil {
			return nil, false, err
		}
		m.state = plainClientRecvWelcome
		return out, false, nil

	case plainClientRecvWelcome:
		if err := RequireInput(plainName, in, "WELCOME"); err != nil {
			return nil, false, err
		}
		body, err := DecodeReply(in, zmtp.CmdWelcome)
		if err != nil {
			return nil, false, err
		}
		// WELCOME has no body per RFC 24 §4.
		if len(body) != 0 {
			return nil, false, Malformed(plainName, "WELCOME carries unexpected body (%d bytes)", len(body))
		}
		m.state = plainClientSendInitiate
		return m.NextHandshake(nil)

	case plainClientSendInitiate:
		out := zmtp.EncodeCommand(zmtp.Command{Name: zmtp.CmdInitiate, Body: zmtp.EncodeMetadata(m.localMeta)})
		m.state = plainClientRecvReady
		return out, false, nil

	case plainClientRecvReady:
		if err := RequireInput(plainName, in, "READY"); err != nil {
			return nil, false, err
		}
		body, err := DecodeReply(in, zmtp.CmdReady)
		if err != nil {
			return nil, false, err
		}
		props, err := zmtp.DecodeMetadata(body)
		if err != nil {
			return nil, false, err
		}
		m.MergePeerMetadata(props)
		m.state = plainDone
		return nil, true, nil

	// ------------------------------------------------------------------ Server
	case plainServerSeed:
		// Passive seed — let the session driver loop read HELLO next.
		if err := RequireNoInput(plainName, in, "server seed"); err != nil {
			return nil, false, err
		}
		m.state = plainServerRecvHello
		return nil, false, nil

	case plainServerRecvHello:
		if err := RequireInput(plainName, in, "HELLO"); err != nil {
			return nil, false, err
		}
		body, err := zmtp.ExpectCommand(in, zmtp.CmdHello)
		if err != nil {
			return nil, false, err
		}
		user, pass, err := parseHello(body)
		if err != nil {
			return nil, false, err
		}
		// The credentials stay in locals only and are dropped as soon as
		// the auth decision below is made.
		var authErr error
		switch {
		case m.zap != nil:
			if err := m.AuthorizeZAP(plainMechName, [][]byte{[]byte(user), []byte(pass)}); err != nil {
				authErr = fmt.Errorf("gozmq/security/plain: ZAP rejected user %q: %w", user, err)
			}
		case m.authFn == nil || !m.authFn(user, pass):
			authErr = AuthFailed(plainName, "authentication failed for user %q", user)
		}
		if authErr != nil {
			// Emit ERROR and tear down. The reason text is short and
			// opaque per RFC 24's guidance not to leak authentication
			// detail.
			m.state = plainDone
			return zmtp.EncodeError("Invalid username or password"), false, authErr
		}
		// Success: emit WELCOME, await INITIATE.
		m.state = plainServerRecvInitiate
		return zmtp.EncodeCommand(zmtp.Command{Name: zmtp.CmdWelcome}), false, nil

	case plainServerRecvInitiate:
		if err := RequireInput(plainName, in, "INITIATE"); err != nil {
			return nil, false, err
		}
		body, err := zmtp.ExpectCommand(in, zmtp.CmdInitiate)
		if err != nil {
			return nil, false, err
		}
		props, err := zmtp.DecodeMetadata(body)
		if err != nil {
			return nil, false, err
		}
		m.MergePeerMetadata(props)
		m.state = plainServerSendReady
		return m.NextHandshake(nil)

	case plainServerSendReady:
		m.state = plainDone
		return EncodeReady(m.localMeta), true, nil

	case plainDone:
		return nil, true, nil
	}
	return nil, false, fmt.Errorf("gozmq/security/plain: unexpected state %d", m.state)
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
	return zmtp.EncodeCommand(zmtp.Command{Name: zmtp.CmdHello, Body: payload}), nil
}

// parseHello parses the HELLO payload (NOT the full command body — the
// command-name prefix must already have been stripped).
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
