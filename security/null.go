// Package security hosts the ZMTP security mechanisms NULL and PLAIN plus the
// RFC 27 ZAP authorization codec. Each mechanism implements iface.SecurityMech
// so the session layer drives them through the same state-machine loop. CURVE
// lives in the security/curve subpackage because it also exposes key-pair
// primitives to callers.
//
// Closed set: ZMTP defines exactly three mechanisms — NULL, PLAIN, and CURVE —
// and gozmq implements all three here. (libzmq additionally ships GSSAPI,
// specified separately in RFC 38; gozmq does not implement it.)
// iface.SecurityMech lives under internal/, so security mechanisms cannot be
// implemented by code outside this module; this is intentional.
// Custom/third-party mechanisms are out of scope (they would not interoperate
// with any other ZMTP peer). Authorization policy, by contrast, IS pluggable
// via the ZAP handler (see WithZapHandler / ZapPolicy) — that is the supported
// extension point, not the mechanism set.
//
// This file implements NULL (ZMTP 3.1 §3.2, design §8.1). The handshake is
// a single READY exchange — no cryptographic state, no per-frame rewriting.
package security

import (
	"fmt"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
)

// nullMechName is the greeting mechanism token for NULL (ZMTP 3.1 §3.2.1).
const nullMechName = "NULL"

// nullMech implements iface.SecurityMech for the NULL mechanism. The
// handshake is a strict lock-step exchange of a single READY command in
// each direction; the mechanism is "done" once the peer's READY has been
// parsed.
//
// State tracking:
//
//	sentReady: we have produced our outbound READY on a prior NextHandshake call.
//	peerReady: we have parsed the peer's READY on a prior NextHandshake call.
//
// done == sentReady && peerReady.
type nullMech struct {
	MechBase
	sentReady bool
	peerReady bool
}

// NewNull returns a NULL security mechanism configured for role and
// advertising localMeta in its READY command. A nil localMeta is treated as
// an empty property list. The returned value is not safe for concurrent use;
// the session layer drives NextHandshake single-threaded.
func NewNull(role iface.Role, localMeta map[string]string) iface.SecurityMech {
	return &nullMech{MechBase: NewMechBase(role, localMeta)}
}

// Name reports the greeting mechanism token, unpadded.
func (*nullMech) Name() string { return nullMechName }

// NextHandshake advances the NULL handshake.
//
// Call pattern:
//
//	out, done, err := m.NextHandshake(nil)              // returns READY body, done=false
//	// session writes out, reads peer's READY body
//	_, done, err  = m.NextHandshake(peerReadyBody)      // returns nil, done=true
//
// The bytes returned on the seed call are the READY command BODY in ZMTP
// 3.1 §2.3 shape — a single name-length byte, the literal "READY", then the
// property-list bytes. The session layer is responsible for wrapping this
// in a COMMAND-flagged frame (flags bit 0x04 + length + this body).
func (m *nullMech) NextHandshake(in []byte) ([]byte, bool, error) {
	if m.role == iface.RoleServer {
		return m.nextServer(in)
	}
	// First direction: emit our READY command body.
	if !m.sentReady {
		if in != nil {
			return nil, false, fmt.Errorf("gozmq/security/null: unexpected input on seed step: %w", zmqerr.ErrMalformedFrame)
		}
		m.sentReady = true
		return m.encodeReady(), false, nil
	}

	// Second direction: parse the peer's READY command body.
	if !m.peerReady {
		if in == nil {
			return nil, false, fmt.Errorf("gozmq/security/null: nil peer input on handshake step 2: %w", zmqerr.ErrMalformedFrame)
		}
		props, err := m.decodeReady(in)
		if err != nil {
			return nil, false, err
		}
		m.MergePeerMetadata(props)
		m.peerReady = true
		return nil, true, nil
	}

	// Post-handshake: the session layer must not call us again.
	return nil, true, nil
}

func (m *nullMech) nextServer(in []byte) ([]byte, bool, error) {
	if !m.peerReady {
		if !m.sentReady && in == nil {
			// Passive seed: wait for the client's READY so authorization can
			// happen before the server emits its READY.
			return nil, false, nil
		}
		if in == nil {
			return nil, false, fmt.Errorf("gozmq/security/null: nil peer input on server handshake step: %w", zmqerr.ErrMalformedFrame)
		}
		props, err := m.decodeReady(in)
		if err != nil {
			return nil, false, err
		}
		if err := m.AuthorizeZAP(nullMechName, nil); err != nil {
			// Mirror PLAIN: emit a ZMTP ERROR command alongside the wrapped
			// error so the peer sees an auth-failure reason rather than an
			// abrupt close. The reason text is short and opaque so as not
			// to leak authorization detail.
			out := encodeCommand(zmtp.CmdError, encodeErrorReason("Authentication failure"))
			return out, false, fmt.Errorf("gozmq/security/null: ZAP rejected peer: %w", err)
		}
		m.MergePeerMetadata(props)
		m.peerReady = true
	}
	if !m.sentReady {
		m.sentReady = true
		return m.encodeReady(), true, nil
	}
	return nil, true, nil
}

// EncodeFrame is the identity codec for NULL — no per-frame rewriting.
func (*nullMech) EncodeFrame(flags uint8, plain []byte) (uint8, []byte, error) {
	return flags, plain, nil
}

// DecodeFrame is the identity codec for NULL — no per-frame rewriting.
func (*nullMech) DecodeFrame(flags uint8, wire []byte) (uint8, []byte, error) {
	return flags, wire, nil
}

// encodeReady builds a ZMTP READY command body (name + metadata property
// list). The session layer wraps this in a COMMAND frame.
func (m *nullMech) encodeReady() []byte {
	return zmtp.EncodeCommand(zmtp.Command{Name: zmtp.CmdReady, Body: zmtp.MetadataCodec.Encode(m.localMeta)})
}

// decodeReady parses a READY command body and returns its metadata. ZMTP
// command names are exact (case-sensitive) ASCII tokens, so the match is
// byte-exact.
func (m *nullMech) decodeReady(body []byte) (map[string]string, error) {
	cmd, err := zmtp.DecodeCommand(body)
	if err != nil {
		return nil, fmt.Errorf("gozmq/security/null: parse READY: %w", err)
	}
	if cmd.Name != zmtp.CmdReady {
		return nil, fmt.Errorf("gozmq/security/null: expected READY command, got %q: %w", cmd.Name, zmqerr.ErrMalformedFrame)
	}
	return zmtp.MetadataCodec.Decode(cmd.Body)
}
