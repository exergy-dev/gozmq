// Package security hosts the ZMTP security mechanisms NULL and PLAIN plus the
// RFC 27 ZAP authorization codec. Each mechanism implements iface.SecurityMech
// so the session layer drives them through the same state-machine loop. CURVE
// lives in the security/curve subpackage because it also exposes key-pair
// primitives to callers.
//
// This file implements NULL (ZMTP 3.1 §3.2, design §8.1). The handshake is
// a single READY exchange — no cryptographic state, no per-frame rewriting.
package security

import (
	"fmt"
	"maps"
	"strings"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/rkm/gozmq/internal/zmtp"
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
	role      iface.Role
	localMeta map[string]string
	peerMeta  map[string]string
	sentReady bool
	peerReady bool
	zapAuth   iface.ZAPAuthorizer
	zapDomain string
	zapAddr   string
	zapMeta   map[string]string
}

// NewNull returns a NULL security mechanism configured for role and
// advertising localMeta in its READY command. A nil localMeta is treated as
// an empty property list. The returned value is not safe for concurrent use;
// the session layer drives NextHandshake single-threaded.
func NewNull(role iface.Role, localMeta map[string]string) iface.SecurityMech {
	return &nullMech{
		role:      role,
		localMeta: maps.Clone(localMeta),
	}
}

// Name reports the greeting mechanism token, unpadded.
func (*nullMech) Name() string { return nullMechName }

// AsServer reports the value of the greeting as-server byte.
func (m *nullMech) AsServer() bool { return m.role == iface.RoleServer }

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
		m.peerMeta = props
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
		if m.zapAuth != nil {
			_, meta, err := m.zapAuth(m.zapDomain, m.zapAddr, nullMechName, nil)
			if err != nil {
				return nil, false, fmt.Errorf("gozmq/security/null: ZAP rejected peer: %w", err)
			}
			m.zapMeta = maps.Clone(meta)
		}
		m.peerMeta = zmtp.MergeMeta(props, m.zapMeta)
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

// Metadata returns a copy of the local metadata properties sent in READY.
// The copy guards against external mutation after construction.
func (m *nullMech) Metadata() map[string]string {
	return maps.Clone(m.localMeta)
}

// SetPeerMetadata stores the peer's parsed READY properties. NULL performs
// no validation here; Socket-Type compatibility checks live in the socket
// engine layer per design §5.3.
func (m *nullMech) SetPeerMetadata(props map[string]string) error {
	m.peerMeta = maps.Clone(props)
	return nil
}

// PeerMetadata returns the properties parsed from the peer's READY, or nil
// before the handshake completes. Not part of iface.SecurityMech but useful
// for tests and for the session layer during metadata forwarding.
func (m *nullMech) PeerMetadata() map[string]string {
	return maps.Clone(m.peerMeta)
}

// ConfigureZAP wires a server-side RFC 27 authorizer into the mechanism.
// Client-side NULL ignores the hook.
func (m *nullMech) ConfigureZAP(authorizer iface.ZAPAuthorizer, domain, address string) {
	if m.role != iface.RoleServer {
		return
	}
	m.zapAuth = authorizer
	m.zapDomain = domain
	m.zapAddr = address
}

// encodeReady builds a ZMTP READY command body (name + metadata property
// list). The session layer wraps this in a COMMAND frame.
func (m *nullMech) encodeReady() []byte {
	return zmtp.EncodeCommand(zmtp.Command{Name: zmtp.CmdReady, Body: zmtp.MetadataCodec.Encode(m.localMeta)})
}

// decodeReady parses a READY command body and returns its metadata. The name
// is matched case-insensitively since libzmq emits it uppercase.
func (m *nullMech) decodeReady(body []byte) (map[string]string, error) {
	cmd, err := zmtp.DecodeCommand(body)
	if err != nil {
		return nil, fmt.Errorf("gozmq/security/null: parse READY: %w", err)
	}
	if !strings.EqualFold(cmd.Name, zmtp.CmdReady) {
		return nil, fmt.Errorf("gozmq/security/null: expected READY command, got %q: %w", cmd.Name, zmqerr.ErrMalformedFrame)
	}
	return zmtp.MetadataCodec.Decode(cmd.Body)
}

