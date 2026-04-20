// Package security hosts the three ZMTP security mechanisms: NULL, PLAIN,
// and CURVE. Each mechanism implements iface.SecurityMech so the session
// layer drives all three through the same state-machine loop.
//
// This file implements NULL (ZMTP 3.1 §3.2, design §8.1). The handshake is
// a single READY exchange — no cryptographic state, no per-frame rewriting.
package security

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// nullMechName is the greeting mechanism token for NULL (ZMTP 3.1 §3.2.1).
const nullMechName = "NULL"

// readyCommandName is the ZMTP command name used by NULL's handshake. It is
// written in the command BODY we emit (1-byte length prefix + name), because
// this file must produce the READY command body without importing the zmtp
// package — zmtp's EncodeCommand helper lands in parallel in agent A1 and we
// stay decoupled.
const readyCommandName = "READY"

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
	metaCodec iface.MetadataCodec
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
	copied := make(map[string]string, len(localMeta))
	for k, v := range localMeta {
		copied[k] = v
	}
	return &nullMech{
		role:      role,
		localMeta: copied,
		metaCodec: nullMetaCodec{},
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
		out, err := m.encodeReady()
		if err != nil {
			return nil, false, err
		}
		m.sentReady = true
		return out, false, nil
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
			m.zapMeta = copyNullMeta(meta)
		}
		m.peerMeta = mergeNullMeta(props, m.zapMeta)
		m.peerReady = true
	}
	if !m.sentReady {
		out, err := m.encodeReady()
		if err != nil {
			return nil, false, err
		}
		m.sentReady = true
		return out, true, nil
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
	out := make(map[string]string, len(m.localMeta))
	for k, v := range m.localMeta {
		out[k] = v
	}
	return out
}

// SetPeerMetadata stores the peer's parsed READY properties. NULL performs
// no validation here; Socket-Type compatibility checks live in the socket
// engine layer per design §5.3.
func (m *nullMech) SetPeerMetadata(props map[string]string) error {
	if props == nil {
		m.peerMeta = nil
		return nil
	}
	out := make(map[string]string, len(props))
	for k, v := range props {
		out[k] = v
	}
	m.peerMeta = out
	return nil
}

// PeerMetadata returns the properties parsed from the peer's READY, or nil
// before the handshake completes. Not part of iface.SecurityMech but useful
// for tests and for the session layer during metadata forwarding.
func (m *nullMech) PeerMetadata() map[string]string {
	if m.peerMeta == nil {
		return nil
	}
	out := make(map[string]string, len(m.peerMeta))
	for k, v := range m.peerMeta {
		out[k] = v
	}
	return out
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

// encodeReady builds a ZMTP READY command BODY: a single-byte name length,
// the literal "READY", then the metadata property-list bytes. The session
// layer wraps this in a COMMAND frame (flags 0x04 + length + this body).
func (m *nullMech) encodeReady() ([]byte, error) {
	meta := m.metaCodec.Encode(m.localMeta)
	body := make([]byte, 0, 1+len(readyCommandName)+len(meta))
	body = append(body, byte(len(readyCommandName)))
	body = append(body, readyCommandName...)
	body = append(body, meta...)
	return body, nil
}

// decodeReady parses a READY command body: [1 byte name-length][name][metadata].
// The name must be "READY" (case-sensitive per ZMTP 3.1 §2.3 which defines
// command names as ASCII tokens — tolerant case match is applied for
// robustness since libzmq uppercases consistently).
func (m *nullMech) decodeReady(body []byte) (map[string]string, error) {
	if len(body) < 1 {
		return nil, fmt.Errorf("gozmq/security/null: READY command body empty: %w", zmqerr.ErrMalformedFrame)
	}
	nameLen := int(body[0])
	if 1+nameLen > len(body) {
		return nil, fmt.Errorf("gozmq/security/null: READY command name truncated: %w", zmqerr.ErrMalformedFrame)
	}
	name := string(body[1 : 1+nameLen])
	if !strings.EqualFold(name, readyCommandName) {
		return nil, fmt.Errorf("gozmq/security/null: expected READY command, got %q: %w", name, zmqerr.ErrMalformedFrame)
	}
	metaBytes := body[1+nameLen:]
	props, err := m.metaCodec.Decode(metaBytes)
	if err != nil {
		return nil, err // already wraps ErrMalformedFrame
	}
	return props, nil
}

// nullMetaCodec is a minimal inline copy of the ZMTP metadata codec. We
// deliberately avoid importing zmtp here so security/null.go doesn't depend
// on a package that's being implemented in parallel. Once agent A1 and A3's
// work merge the main agent can refactor this to use zmtp.MetadataCodec.
type nullMetaCodec struct{}

func (nullMetaCodec) Encode(props map[string]string) []byte {
	total := 0
	for name, value := range props {
		total += 1 + len(name) + 4 + len(value)
	}
	out := make([]byte, 0, total)
	for name, value := range props {
		if len(name) > 255 {
			panic(fmt.Sprintf("gozmq/security/null: metadata name length %d > 255", len(name)))
		}
		out = append(out, byte(len(name)))
		out = append(out, name...)
		var vln [4]byte
		binary.BigEndian.PutUint32(vln[:], uint32(len(value)))
		out = append(out, vln[:]...)
		out = append(out, value...)
	}
	return out
}

func (nullMetaCodec) Decode(raw []byte) (map[string]string, error) {
	out := make(map[string]string)
	seen := make(map[string]struct{})
	for i := 0; i < len(raw); {
		if i+1 > len(raw) {
			return nil, fmt.Errorf("gozmq/security/null: metadata truncated at name length: %w", zmqerr.ErrMalformedFrame)
		}
		nameLen := int(raw[i])
		i++
		if i+nameLen > len(raw) {
			return nil, fmt.Errorf("gozmq/security/null: metadata truncated in name: %w", zmqerr.ErrMalformedFrame)
		}
		name := string(raw[i : i+nameLen])
		i += nameLen
		if i+4 > len(raw) {
			return nil, fmt.Errorf("gozmq/security/null: metadata truncated at value length: %w", zmqerr.ErrMalformedFrame)
		}
		valueLen := binary.BigEndian.Uint32(raw[i : i+4])
		i += 4
		if uint64(i)+uint64(valueLen) > uint64(len(raw)) {
			return nil, fmt.Errorf("gozmq/security/null: metadata truncated in value: %w", zmqerr.ErrMalformedFrame)
		}
		value := string(raw[i : i+int(valueLen)])
		i += int(valueLen)
		lower := strings.ToLower(name)
		if _, dup := seen[lower]; dup {
			return nil, fmt.Errorf("gozmq/security/null: duplicate metadata property %q: %w", name, zmqerr.ErrMalformedFrame)
		}
		seen[lower] = struct{}{}
		out[name] = value
	}
	return out, nil
}

func copyNullMeta(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func mergeNullMeta(base, overlay map[string]string) map[string]string {
	if len(base) == 0 {
		return copyNullMeta(overlay)
	}
	if len(overlay) == 0 {
		return copyNullMeta(base)
	}
	out := copyNullMeta(base)
	for k, v := range overlay {
		out[k] = v
	}
	return out
}
