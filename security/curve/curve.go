// CurveZMQ (RFC 26) security mechanism — five-step handshake plus
// per-frame MESSAGE envelope. This file is the codec only: the session
// layer drives NextHandshake in lock-step exactly as for NULL and PLAIN.
//
// The handshake runs:
//
//	client → server: HELLO     (client ephemeral public, signature box)
//	server → client: WELCOME   (cookie box hiding server ephemeral)
//	client → server: INITIATE  (client long-term key + vouch + metadata)
//	server → client: READY     (server metadata)
//	both directions: MESSAGE   (every post-handshake frame body)
//
// Implementation notes:
//   - Cryptography is delegated entirely to golang.org/x/crypto/nacl/box and
//     nacl/secretbox. This file only shapes the envelopes.
//   - "Short" nonces are 16-byte prefix + 8-byte counter (per-direction,
//     starting at 1). See nonce.go.
//   - "Long" nonces are 8-byte prefix + 16 random bytes (WELCOME/COOKIE/
//     VOUCH). Random bytes come from crypto/rand unless the caller injects
//     a deterministic reader for testing.
//   - Metadata is encoded as the ZMTP property list (1-byte name length,
//     name, 4-byte big-endian value length, value) — identical to what
//     NULL/PLAIN use. We inline the codec to keep this public package
//     independent of internal implementation packages.

package curve

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/rkm/gozmq/internal/zmtp"

	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/nacl/box"
	"golang.org/x/crypto/nacl/secretbox"
)

// --------------------------------------------------------------------------
// Constants: command names + fixed sizes from RFC 26.
// --------------------------------------------------------------------------

const (
	cmdHello    = "HELLO"
	cmdWelcome  = "WELCOME"
	cmdInitiate = "INITIATE"
	cmdReady    = "READY"
	cmdMessage  = "MESSAGE"

	mechName = "CURVE"

	// crypto_box authentication tag overhead in bytes. NaCl's Poly1305 tag
	// is 16 bytes; golang.org/x/crypto/nacl/box does not expose this as a
	// named constant, so we pin it locally. If nacl ever changes, the
	// layout asserts below will surface the mismatch at construction.
	boxOverhead = 16

	// Fixed body sizes for HELLO and WELCOME (the only fixed-size
	// handshake commands). INITIATE and READY vary with metadata.
	helloBodySize   = 1 + len(cmdHello) + 2 + 72 + 32 + 8 + 80
	welcomeBodySize = 1 + len(cmdWelcome) + 16 + 144

	// Inner fixed sizes inside HELLO / WELCOME / INITIATE.
	signatureBoxSize = 64 + boxOverhead     // 80 bytes
	welcomeBoxSize   = 128 + boxOverhead    // 144 bytes
	cookieInnerSize  = 64 + boxOverhead     // 80 bytes (secretbox cipher)
	cookieSize       = 16 + cookieInnerSize // 96 bytes (nonce + cipher)
	vouchBoxSize     = 64 + boxOverhead     // 80 bytes
	vouchSize        = 16 + vouchBoxSize    // 96 bytes (nonce + cipher)

	// Protocol version this implementation emits. RFC 26 specifies 1.0.
	curveVersionMajor = 0x01
	curveVersionMinor = 0x00
)

// --------------------------------------------------------------------------
// Mechanism state machine.
// --------------------------------------------------------------------------

// curveState tracks which handshake command we've sent/consumed.
type curveState uint8

const (
	stateStart        curveState = iota // nothing sent/consumed
	stateHelloSent                      // client: HELLO sent, awaiting WELCOME
	stateWelcomeSent                    // server: WELCOME sent, awaiting INITIATE
	stateInitiateSent                   // client: INITIATE sent, awaiting READY
	stateDone                           // handshake complete
)

// curveMech implements iface.SecurityMech for CURVE. The same struct backs
// both client and server roles; role-specific behavior branches inside
// NextHandshake.
type curveMech struct {
	role      iface.Role
	localMeta map[string]string
	peerMeta  map[string]string
	zapAuth   iface.ZAPAuthorizer
	zapDomain string
	zapAddr   string
	zapMeta   map[string]string

	// Long-term keys.
	ourPublic  Key // C on client, S on server
	ourSecret  Key // c on client, s on server
	peerPublic Key // S on client (supplied at construction), C on server (discovered in INITIATE)

	// Ephemeral keys.
	ourEphPublic  Key // C' on client, S' on server
	ourEphSecret  Key
	peerEphPublic Key // S' on client (from WELCOME), C' on server (from HELLO)

	// Cookie encryption key: server-only, used to seal the WELCOME cookie
	// so the server stays stateless between HELLO and INITIATE.
	cookieKey [32]byte

	// Short-nonce counters, per direction. RFC 26 §3 specifies a single
	// monotonic counter per direction starting at 1 and spanning HELLO /
	// INITIATE / READY and every MESSAGE that follows — the counter does
	// NOT reset at the handshake → traffic boundary. libzmq rejects
	// MESSAGE frames whose counter regresses below the last handshake
	// counter it observed, which is why traffic-phase interop requires a
	// single unified counter here.
	ourShortNonce  uint64
	peerShortNonce uint64

	// Prefix strings chosen by role for MESSAGE encode/decode. The client
	// encodes with "CurveZMQMESSAGEC" and decodes with "CurveZMQMESSAGES";
	// the server is the mirror.
	encPrefix string
	decPrefix string

	state curveState

	// rand source for random nonces (long nonces, cookie keys, ephemeral
	// keys). Defaults to crypto/rand. Tests inject a deterministic
	// io.Reader via newWithRand.
	rand io.Reader
}

// NewCurveServer returns a CURVE mechanism configured as the server side.
// serverSecret is the server's long-term secret; the corresponding public
// key is derived from it at construction. The server accepts any client
// public key presented in INITIATE — authorization via ZAP is a separate
// hook layered above this mechanism.
//
// localMeta is the property list sent in the server's READY. A nil map is
// treated as an empty property list.
func NewCurveServer(serverSecret Key, localMeta map[string]string) (iface.SecurityMech, error) {
	m, err := newMech(iface.RoleServer, serverSecret, Key{}, Key{}, localMeta, rand.Reader)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// NewCurveClient returns a CURVE mechanism configured as the client side.
// serverPublic is the expected server long-term public key; clientPublic
// and clientSecret are this client's long-term keypair.
//
// localMeta is the property list sent in INITIATE. A nil map is treated as
// an empty property list.
func NewCurveClient(serverPublic, clientPublic, clientSecret Key, localMeta map[string]string) (iface.SecurityMech, error) {
	// Fail fast on a zero or low-order server key. Doing the check here
	// (rather than in parseWelcome, where the key is first used in
	// box.Open) surfaces a misconfigured socket during construction
	// instead of during the first handshake attempt — the operator sees
	// ErrAuthFailed immediately instead of after a round trip. The
	// ephemeral server key in WELCOME is validated separately at parse
	// time because the wire is its only source.
	if err := validatePeerPublicKey(serverPublic[:]); err != nil {
		return nil, err
	}
	m, err := newMech(iface.RoleClient, clientSecret, clientPublic, serverPublic, localMeta, rand.Reader)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// newMech is the shared constructor. ownSecret is always this side's
// long-term secret. ownPublicHint is the caller-supplied public key for
// symmetric double-check (zero-value is acceptable — we derive it).
// peerLongTerm is the peer long-term public key; zero for the server (learned
// from INITIATE) and the server's public key for the client.
func newMech(role iface.Role, ownSecret, ownPublicHint, peerLongTerm Key, localMeta map[string]string, r io.Reader) (*curveMech, error) {
	if r == nil {
		r = rand.Reader
	}

	// Derive our own long-term public key from the secret via
	// Curve25519 scalar-mult-base. We do this via box.Precompute-style
	// operations, but the simplest is to call curve25519.ScalarBaseMult
	// indirectly through box.GenerateKey semantics; the nacl/box API
	// doesn't expose a public-from-secret helper, so we use the x/crypto
	// curve25519 primitive directly below.
	ourPub, err := derivePublic(ownSecret)
	if err != nil {
		return nil, fmt.Errorf("gozmq/security/curve: derive public from secret: %w", err)
	}
	// If the caller supplied a public-key hint (client form), validate it.
	// The server form passes zero-value — skip the check.
	if ownPublicHint != (Key{}) && ownPublicHint != ourPub {
		return nil, fmt.Errorf("gozmq/security/curve: supplied public key does not match secret: %w", zmqerr.ErrAuthFailed)
	}

	// Generate our ephemeral keypair up front. Both client and server use
	// it: the client sends C' in HELLO; the server sends S' inside the
	// WELCOME box.
	ephPubPtr, ephSecPtr, err := box.GenerateKey(r)
	if err != nil {
		return nil, fmt.Errorf("gozmq/security/curve: generate ephemeral key: %w", err)
	}
	var ephPub, ephSec Key
	copy(ephPub[:], ephPubPtr[:])
	copy(ephSec[:], ephSecPtr[:])

	// Copy the supplied metadata so caller mutation can't race with the
	// mechanism.
	copied := make(map[string]string, len(localMeta))
	for k, v := range localMeta {
		copied[k] = v
	}

	m := &curveMech{
		role:           role,
		localMeta:      copied,
		ourPublic:      ourPub,
		ourSecret:      ownSecret,
		peerPublic:     peerLongTerm,
		ourEphPublic:   ephPub,
		ourEphSecret:   ephSec,
		ourShortNonce:  1,
		peerShortNonce: 1,
		state:          stateStart,
		rand:           r,
	}

	// Per-role MESSAGE prefix assignment.
	switch role {
	case iface.RoleClient:
		m.encPrefix = prefixMessageC
		m.decPrefix = prefixMessageS
	case iface.RoleServer:
		// Server-side cookie key is a fresh 32-byte random used only to
		// secretbox cookies across HELLO → INITIATE. It never leaves the
		// server. Generate lazily-but-now so any RNG error surfaces
		// during construction, not mid-handshake.
		if _, err := io.ReadFull(r, m.cookieKey[:]); err != nil {
			return nil, fmt.Errorf("gozmq/security/curve: generate cookie key: %w", err)
		}
		m.encPrefix = prefixMessageS
		m.decPrefix = prefixMessageC
	default:
		return nil, fmt.Errorf("gozmq/security/curve: unknown role %d", role)
	}

	return m, nil
}

// newWithRand is the test entry point — lets a unit test pin the ephemeral
// keys and long-nonce randomness by injecting a deterministic reader.
func newWithRand(role iface.Role, ownSecret, peerLongTerm Key, localMeta map[string]string, r io.Reader) (*curveMech, error) {
	return newMech(role, ownSecret, Key{}, peerLongTerm, localMeta, r)
}

// --------------------------------------------------------------------------
// iface.SecurityMech implementation.
// --------------------------------------------------------------------------

// Name is the greeting mechanism token.
func (*curveMech) Name() string { return mechName }

// AsServer reports the value of the greeting as-server byte.
func (m *curveMech) AsServer() bool { return m.role == iface.RoleServer }

// Metadata returns a copy of the local properties sent in INITIATE (client)
// or READY (server). Never returns the internal map.
func (m *curveMech) Metadata() map[string]string {
	out := make(map[string]string, len(m.localMeta))
	for k, v := range m.localMeta {
		out[k] = v
	}
	return out
}

// SetPeerMetadata stores the peer's parsed metadata. CURVE does not
// validate here — the session layer handles Socket-Type compatibility —
// but we copy to isolate state from caller mutation.
func (m *curveMech) SetPeerMetadata(props map[string]string) error {
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

// PeerMetadata returns the properties parsed from the peer's INITIATE
// (server side) or READY (client side), or nil before the handshake
// completes. Not part of iface.SecurityMech but exposed for tests.
func (m *curveMech) PeerMetadata() map[string]string {
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
// Client-side CURVE ignores the hook.
func (m *curveMech) ConfigureZAP(authorizer iface.ZAPAuthorizer, domain, address string) {
	if m.role != iface.RoleServer {
		return
	}
	m.zapAuth = authorizer
	m.zapDomain = domain
	m.zapAddr = address
}

// PeerLongTermPublic returns the peer's long-term public key. On the client
// side this is the server public key passed to NewCurveClient. On the
// server side this is the client public key discovered during the INITIATE
// step; it is the zero Key until then.
func (m *curveMech) PeerLongTermPublic() Key { return m.peerPublic }

// NextHandshake drives the CURVE handshake one step per call. The state
// machine is:
//
//	Client: Start -> HELLO out -> WELCOME in -> INITIATE out -> READY in -> Done
//	Server: Start -> HELLO in -> WELCOME out -> INITIATE in -> READY out -> Done
//
// "done=true" on a client fires after consuming READY; on a server after
// producing READY. The session layer writes any non-empty returned byte
// slice as a ZMTP COMMAND frame, and reads the next inbound COMMAND body
// into the next call's `in`.
func (m *curveMech) NextHandshake(in []byte) (out []byte, done bool, err error) {
	switch m.role {
	case iface.RoleClient:
		return m.nextClient(in)
	case iface.RoleServer:
		return m.nextServer(in)
	default:
		return nil, false, fmt.Errorf("gozmq/security/curve: invalid role %d", m.role)
	}
}

// nextClient steps the client side of the handshake.
func (m *curveMech) nextClient(in []byte) ([]byte, bool, error) {
	switch m.state {
	case stateStart:
		if in != nil {
			return nil, false, fmt.Errorf("gozmq/security/curve: client seed step expects nil input: %w", zmqerr.ErrMalformedFrame)
		}
		body, err := m.buildHello()
		if err != nil {
			return nil, false, err
		}
		m.state = stateHelloSent
		return body, false, nil

	case stateHelloSent:
		if in == nil {
			return nil, false, fmt.Errorf("gozmq/security/curve: client expected WELCOME, got nil: %w", zmqerr.ErrMalformedFrame)
		}
		cookie, err := m.parseWelcome(in)
		if err != nil {
			return nil, false, err
		}
		body, err := m.buildInitiate(cookie)
		if err != nil {
			return nil, false, err
		}
		m.state = stateInitiateSent
		return body, false, nil

	case stateInitiateSent:
		if in == nil {
			return nil, false, fmt.Errorf("gozmq/security/curve: client expected READY, got nil: %w", zmqerr.ErrMalformedFrame)
		}
		if err := m.parseReady(in); err != nil {
			return nil, false, err
		}
		m.state = stateDone
		return nil, true, nil

	case stateDone:
		return nil, true, nil

	default:
		return nil, false, fmt.Errorf("gozmq/security/curve: client state %d: %w", m.state, zmqerr.ErrMalformedFrame)
	}
}

// nextServer steps the server side of the handshake.
func (m *curveMech) nextServer(in []byte) ([]byte, bool, error) {
	switch m.state {
	case stateStart:
		if in == nil {
			// Server is passive — must wait for HELLO. Returning
			// (nil, false, nil) on seed lets the session layer issue
			// the first Read before calling us again.
			return nil, false, nil
		}
		if err := m.parseHello(in); err != nil {
			return nil, false, err
		}
		body, err := m.buildWelcome()
		if err != nil {
			return nil, false, err
		}
		m.state = stateWelcomeSent
		return body, false, nil

	case stateWelcomeSent:
		if in == nil {
			return nil, false, fmt.Errorf("gozmq/security/curve: server expected INITIATE, got nil: %w", zmqerr.ErrMalformedFrame)
		}
		if err := m.parseInitiate(in); err != nil {
			return nil, false, err
		}
		body, err := m.buildReady()
		if err != nil {
			return nil, false, err
		}
		m.state = stateDone
		return body, true, nil

	case stateDone:
		return nil, true, nil

	default:
		return nil, false, fmt.Errorf("gozmq/security/curve: server state %d: %w", m.state, zmqerr.ErrMalformedFrame)
	}
}

// --------------------------------------------------------------------------
// HELLO (client → server).
// --------------------------------------------------------------------------

// buildHello constructs the HELLO command body: [5]"HELLO" + version +
// padding + C' + short-nonce-counter + signature-box.
func (m *curveMech) buildHello() ([]byte, error) {
	counter := m.ourShortNonce
	m.ourShortNonce++

	// Signature box: 64 zero bytes encrypted with c' + S.
	nonce := shortNonce(prefixHello, counter)
	sigBox := box.Seal(nil, make([]byte, 64), (*[24]byte)(&nonce), (*[32]byte)(&m.peerPublic), (*[32]byte)(&m.ourEphSecret))
	if len(sigBox) != signatureBoxSize {
		return nil, fmt.Errorf("gozmq/security/curve: HELLO signature box length %d != %d", len(sigBox), signatureBoxSize)
	}

	body := make([]byte, 0, helloBodySize)
	body = append(body, byte(len(cmdHello)))
	body = append(body, cmdHello...)
	body = append(body, curveVersionMajor, curveVersionMinor)
	body = append(body, make([]byte, 72)...)
	body = append(body, m.ourEphPublic[:]...)
	body = appendCounter(body, counter)
	body = append(body, sigBox...)
	if len(body) != helloBodySize {
		return nil, fmt.Errorf("gozmq/security/curve: HELLO body length %d != %d", len(body), helloBodySize)
	}
	return body, nil
}

// parseHello validates a HELLO body and stores the client ephemeral public.
// The signature box is opened to prove the client knows c' (the private
// half of the advertised ephemeral public). Any failure wraps
// zmqerr.ErrMalformedFrame or zmqerr.ErrAuthFailed.
func (m *curveMech) parseHello(body []byte) error {
	if len(body) != helloBodySize {
		return fmt.Errorf("gozmq/security/curve: HELLO length %d != %d: %w", len(body), helloBodySize, zmqerr.ErrMalformedFrame)
	}
	if err := expectCommandName(body, cmdHello); err != nil {
		return err
	}
	off := 1 + len(cmdHello)
	// Version.
	if body[off] != curveVersionMajor {
		return fmt.Errorf("gozmq/security/curve: HELLO version %d.%d unsupported: %w", body[off], body[off+1], zmqerr.ErrProtocolVersion)
	}
	off += 2
	// Skip 72-byte padding.
	off += 72
	// Client ephemeral public. Validate before using in box.Open:
	// a low-order or all-zero C' would make the shared-secret
	// computation collapse to a predictable small-subgroup point.
	if err := validatePeerPublicKey(body[off : off+32]); err != nil {
		return err
	}
	copy(m.peerEphPublic[:], body[off:off+32])
	off += 32
	// Short nonce counter.
	counter := binary.BigEndian.Uint64(body[off : off+8])
	off += 8
	if err := m.requireCounterAdvance(counter, "HELLO"); err != nil {
		return err
	}
	// Signature box.
	sig := body[off : off+signatureBoxSize]
	nonce := shortNonce(prefixHello, counter)
	plain, ok := box.Open(nil, sig, (*[24]byte)(&nonce), (*[32]byte)(&m.peerEphPublic), (*[32]byte)(&m.ourSecret))
	if !ok {
		return fmt.Errorf("gozmq/security/curve: HELLO signature box open failed: %w", zmqerr.ErrAuthFailed)
	}
	// Plaintext must be 64 zero bytes.
	if len(plain) != 64 {
		return fmt.Errorf("gozmq/security/curve: HELLO signature plaintext length %d != 64: %w", len(plain), zmqerr.ErrMalformedFrame)
	}
	for i, b := range plain {
		if b != 0 {
			return fmt.Errorf("gozmq/security/curve: HELLO signature plaintext nonzero at %d: %w", i, zmqerr.ErrAuthFailed)
		}
	}
	m.peerShortNonce = counter + 1
	return nil
}

// --------------------------------------------------------------------------
// WELCOME (server → client).
// --------------------------------------------------------------------------

// buildWelcome constructs the WELCOME command body on the server side.
// WELCOME's freshness comes from a random long-nonce (16 bytes), not a
// counter, because the server is intentionally stateless between HELLO and
// INITIATE — the cookie inside the welcome box lets the server restore its
// ephemeral secret when INITIATE arrives.
func (m *curveMech) buildWelcome() ([]byte, error) {
	// 16 random bytes for the WELCOME long nonce.
	var welNonceR [16]byte
	if _, err := io.ReadFull(m.rand, welNonceR[:]); err != nil {
		return nil, fmt.Errorf("gozmq/security/curve: welcome nonce rand: %w", err)
	}
	welNonce := longNonce(prefixWelcome, welNonceR[:])

	// 16 random bytes for the COOKIE secretbox nonce.
	var cookieNonceR [16]byte
	if _, err := io.ReadFull(m.rand, cookieNonceR[:]); err != nil {
		return nil, fmt.Errorf("gozmq/security/curve: cookie nonce rand: %w", err)
	}
	cookieNonce := longNonce(prefixCookie, cookieNonceR[:])

	// Cookie plaintext: C' (peer ephemeral from HELLO) + s' (our ephemeral secret).
	cookiePlain := make([]byte, 0, 64)
	cookiePlain = append(cookiePlain, m.peerEphPublic[:]...)
	cookiePlain = append(cookiePlain, m.ourEphSecret[:]...)
	// Encrypt with server-only cookieKey via secretbox.
	cookieCipher := secretbox.Seal(nil, cookiePlain, (*[24]byte)(&cookieNonce), (*[32]byte)(&m.cookieKey))
	if len(cookieCipher) != cookieInnerSize {
		return nil, fmt.Errorf("gozmq/security/curve: cookie cipher length %d != %d", len(cookieCipher), cookieInnerSize)
	}
	// Cookie wire form: 16-byte cookie nonce suffix || cookie cipher (80 bytes).
	cookie := make([]byte, 0, cookieSize)
	cookie = append(cookie, cookieNonceR[:]...)
	cookie = append(cookie, cookieCipher...)

	// Welcome plaintext: S' || cookie  (32 + 96 = 128 bytes).
	welPlain := make([]byte, 0, 128)
	welPlain = append(welPlain, m.ourEphPublic[:]...)
	welPlain = append(welPlain, cookie...)

	// Welcome box: box(welPlain, welNonce, C', s).
	welBox := box.Seal(nil, welPlain, (*[24]byte)(&welNonce), (*[32]byte)(&m.peerEphPublic), (*[32]byte)(&m.ourSecret))
	if len(welBox) != welcomeBoxSize {
		return nil, fmt.Errorf("gozmq/security/curve: welcome box length %d != %d", len(welBox), welcomeBoxSize)
	}

	// Assemble WELCOME body.
	body := make([]byte, 0, welcomeBodySize)
	body = append(body, byte(len(cmdWelcome)))
	body = append(body, cmdWelcome...)
	body = append(body, welNonceR[:]...)
	body = append(body, welBox...)
	if len(body) != welcomeBodySize {
		return nil, fmt.Errorf("gozmq/security/curve: WELCOME body length %d != %d", len(body), welcomeBodySize)
	}
	return body, nil
}

// parseWelcome decodes the WELCOME body, extracts and stores the server
// ephemeral public key S', and returns the 96-byte opaque cookie that the
// client must echo back in INITIATE.
func (m *curveMech) parseWelcome(body []byte) ([]byte, error) {
	if len(body) != welcomeBodySize {
		return nil, fmt.Errorf("gozmq/security/curve: WELCOME length %d != %d: %w", len(body), welcomeBodySize, zmqerr.ErrMalformedFrame)
	}
	if err := expectCommandName(body, cmdWelcome); err != nil {
		return nil, err
	}
	off := 1 + len(cmdWelcome)
	welNonceR := body[off : off+16]
	off += 16
	welBox := body[off : off+welcomeBoxSize]
	nonce := longNonce(prefixWelcome, welNonceR)
	plain, ok := box.Open(nil, welBox, (*[24]byte)(&nonce), (*[32]byte)(&m.peerPublic), (*[32]byte)(&m.ourEphSecret))
	if !ok {
		return nil, fmt.Errorf("gozmq/security/curve: WELCOME box open failed: %w", zmqerr.ErrAuthFailed)
	}
	if len(plain) != 128 {
		return nil, fmt.Errorf("gozmq/security/curve: WELCOME plaintext length %d != 128: %w", len(plain), zmqerr.ErrMalformedFrame)
	}
	// Validate the server's advertised ephemeral (S') before binding
	// into m.peerEphPublic. A malicious or broken server could inject
	// a low-order point here to drive the post-handshake MESSAGE
	// shared secret to a known small-subgroup value.
	if err := validatePeerPublicKey(plain[:32]); err != nil {
		return nil, err
	}
	copy(m.peerEphPublic[:], plain[:32])
	cookie := make([]byte, cookieSize)
	copy(cookie, plain[32:])
	return cookie, nil
}

// --------------------------------------------------------------------------
// INITIATE (client → server).
// --------------------------------------------------------------------------

// buildInitiate constructs the INITIATE command body on the client side.
// Layout: [8]"INITIATE" + cookie(96) + short-nonce(8) + initiate-box(variable).
func (m *curveMech) buildInitiate(cookie []byte) ([]byte, error) {
	if len(cookie) != cookieSize {
		return nil, fmt.Errorf("gozmq/security/curve: INITIATE cookie length %d != %d", len(cookie), cookieSize)
	}

	// Vouch box: plaintext C'||S (client ephemeral public || server long-
	// term public), encrypted c -> S' with nonce "VOUCH---"+random. Per
	// RFC 26 §2.3 the vouch binds the client's ephemeral key to the
	// server's LONG-TERM identity, not to its ephemeral key — libzmq
	// rejects C'||S' as "vouch S' mismatch".
	var vouchR [16]byte
	if _, err := io.ReadFull(m.rand, vouchR[:]); err != nil {
		return nil, fmt.Errorf("gozmq/security/curve: vouch nonce rand: %w", err)
	}
	vouchNonce := longNonce(prefixVouch, vouchR[:])
	vouchPlain := make([]byte, 0, 64)
	vouchPlain = append(vouchPlain, m.ourEphPublic[:]...)
	vouchPlain = append(vouchPlain, m.peerPublic[:]...)
	vouchBox := box.Seal(nil, vouchPlain, (*[24]byte)(&vouchNonce), (*[32]byte)(&m.peerEphPublic), (*[32]byte)(&m.ourSecret))
	if len(vouchBox) != vouchBoxSize {
		return nil, fmt.Errorf("gozmq/security/curve: vouch box length %d != %d", len(vouchBox), vouchBoxSize)
	}
	// Vouch wire form: 16-byte random nonce suffix || 80-byte cipher.
	vouch := make([]byte, 0, vouchSize)
	vouch = append(vouch, vouchR[:]...)
	vouch = append(vouch, vouchBox...)

	// Initiate box plaintext: C || vouch || metadata.
	meta := zmtp.MetadataCodec.Encode(m.localMeta)
	initPlain := make([]byte, 0, 32+vouchSize+len(meta))
	initPlain = append(initPlain, m.ourPublic[:]...)
	initPlain = append(initPlain, vouch...)
	initPlain = append(initPlain, meta...)

	// Initiate nonce: "CurveZMQINITIATE" + counter.
	counter := m.ourShortNonce
	m.ourShortNonce++
	initNonce := shortNonce(prefixInitiate, counter)
	initBox := box.Seal(nil, initPlain, (*[24]byte)(&initNonce), (*[32]byte)(&m.peerEphPublic), (*[32]byte)(&m.ourEphSecret))

	// Assemble INITIATE body.
	body := make([]byte, 0, 1+len(cmdInitiate)+cookieSize+8+len(initBox))
	body = append(body, byte(len(cmdInitiate)))
	body = append(body, cmdInitiate...)
	body = append(body, cookie...)
	body = appendCounter(body, counter)
	body = append(body, initBox...)
	return body, nil
}

// parseInitiate decodes the INITIATE body on the server side, opens the
// cookie to recover the paired ephemeral secret, opens the initiate box
// to extract C (client long-term public) and the metadata, and validates
// the vouch to confirm the advertised C controls c.
func (m *curveMech) parseInitiate(body []byte) error {
	minLen := 1 + len(cmdInitiate) + cookieSize + 8 + boxOverhead + 32 + vouchSize
	if len(body) < minLen {
		return fmt.Errorf("gozmq/security/curve: INITIATE length %d < min %d: %w", len(body), minLen, zmqerr.ErrMalformedFrame)
	}
	if err := expectCommandName(body, cmdInitiate); err != nil {
		return err
	}
	off := 1 + len(cmdInitiate)

	// Cookie — we decrypt it using our cookieKey to restore (C', s').
	cookieNonceR := body[off : off+16]
	cookieCipher := body[off+16 : off+cookieSize]
	off += cookieSize
	cookieNonce := longNonce(prefixCookie, cookieNonceR)
	cookiePlain, ok := secretbox.Open(nil, cookieCipher, (*[24]byte)(&cookieNonce), (*[32]byte)(&m.cookieKey))
	if !ok {
		return fmt.Errorf("gozmq/security/curve: INITIATE cookie open failed: %w", zmqerr.ErrAuthFailed)
	}
	if len(cookiePlain) != 64 {
		return fmt.Errorf("gozmq/security/curve: INITIATE cookie plaintext length %d != 64: %w", len(cookiePlain), zmqerr.ErrMalformedFrame)
	}
	// Restore (C', s') from the cookie. RFC 26 designs the cookie so the
	// server can be stateless between HELLO and INITIATE — the cookie is
	// authoritative. We also hold the values in memory from HELLO and
	// WELCOME; assert equality as a corruption check before trusting the
	// cookie.
	var cookieCprime Key
	var cookieSprime Key
	copy(cookieCprime[:], cookiePlain[:32])
	copy(cookieSprime[:], cookiePlain[32:])
	if cookieCprime != m.peerEphPublic {
		return fmt.Errorf("gozmq/security/curve: cookie C' mismatch: %w", zmqerr.ErrAuthFailed)
	}
	if cookieSprime != m.ourEphSecret {
		return fmt.Errorf("gozmq/security/curve: cookie s' mismatch: %w", zmqerr.ErrAuthFailed)
	}

	// Short-nonce counter for the initiate box.
	counter := binary.BigEndian.Uint64(body[off : off+8])
	off += 8
	if err := m.requireCounterAdvance(counter, "INITIATE"); err != nil {
		return err
	}
	initCipher := body[off:]

	initNonce := shortNonce(prefixInitiate, counter)
	initPlain, ok := box.Open(nil, initCipher, (*[24]byte)(&initNonce), (*[32]byte)(&m.peerEphPublic), (*[32]byte)(&m.ourEphSecret))
	if !ok {
		return fmt.Errorf("gozmq/security/curve: INITIATE box open failed: %w", zmqerr.ErrAuthFailed)
	}
	if len(initPlain) < 32+vouchSize {
		return fmt.Errorf("gozmq/security/curve: INITIATE plaintext too short: %w", zmqerr.ErrMalformedFrame)
	}

	// Extract C (client long-term public), vouch, and metadata.
	// Validate C before feeding it into box.Open for the vouch. A
	// client that presents a zero or low-order long-term key could
	// otherwise force the server into a small-subgroup ECDH, making
	// the vouch verification meaningless and every subsequent MESSAGE
	// key predictable.
	if err := validatePeerPublicKey(initPlain[:32]); err != nil {
		return err
	}
	var clientLongPub Key
	copy(clientLongPub[:], initPlain[:32])
	vouchR := initPlain[32:48]
	vouchBox := initPlain[48 : 48+vouchBoxSize]
	metaBytes := initPlain[32+vouchSize:]

	// Open vouch: encrypted with c (client long-term secret) -> S' (our ephemeral public).
	// We open using our ephemeral secret s' + clientLongPub.
	vouchNonce := longNonce(prefixVouch, vouchR)
	vouchPlain, ok := box.Open(nil, vouchBox, (*[24]byte)(&vouchNonce), (*[32]byte)(&clientLongPub), (*[32]byte)(&m.ourEphSecret))
	if !ok {
		return fmt.Errorf("gozmq/security/curve: INITIATE vouch open failed: %w", zmqerr.ErrAuthFailed)
	}
	if len(vouchPlain) != 64 {
		return fmt.Errorf("gozmq/security/curve: vouch plaintext length %d != 64: %w", len(vouchPlain), zmqerr.ErrMalformedFrame)
	}
	// Per RFC 26 §2.3 the vouch plaintext is C'||S — client ephemeral
	// public followed by server LONG-TERM public (ourPublic here, since
	// we are the server decoding a client's vouch).
	var vouchedCprime, vouchedS Key
	copy(vouchedCprime[:], vouchPlain[:32])
	copy(vouchedS[:], vouchPlain[32:])
	if vouchedCprime != m.peerEphPublic {
		return fmt.Errorf("gozmq/security/curve: vouch C' mismatch: %w", zmqerr.ErrAuthFailed)
	}
	if vouchedS != m.ourPublic {
		return fmt.Errorf("gozmq/security/curve: vouch S mismatch: %w", zmqerr.ErrAuthFailed)
	}

	// Client long-term public is now authenticated. Record it and parse
	// metadata.
	m.peerPublic = clientLongPub
	props, err := zmtp.MetadataCodec.Decode(metaBytes)
	if err != nil {
		return err
	}
	if m.zapAuth != nil {
		_, meta, err := m.zapAuth(m.zapDomain, m.zapAddr, mechName, [][]byte{clientLongPub[:]})
		if err != nil {
			return fmt.Errorf("gozmq/security/curve: ZAP rejected client key: %w", err)
		}
		m.zapMeta = copyMeta(meta)
	}
	m.peerMeta = mergeMeta(props, m.zapMeta)
	m.peerShortNonce = counter + 1
	return nil
}

// --------------------------------------------------------------------------
// READY (server → client).
// --------------------------------------------------------------------------

// buildReady constructs the READY command body. Layout:
// [5]"READY" + short-nonce(8) + ready-box(metadata ciphertext).
func (m *curveMech) buildReady() ([]byte, error) {
	meta := zmtp.MetadataCodec.Encode(m.localMeta)
	counter := m.ourShortNonce
	m.ourShortNonce++
	nonce := shortNonce(prefixReady, counter)
	readyBox := box.Seal(nil, meta, (*[24]byte)(&nonce), (*[32]byte)(&m.peerEphPublic), (*[32]byte)(&m.ourEphSecret))

	body := make([]byte, 0, 1+len(cmdReady)+8+len(readyBox))
	body = append(body, byte(len(cmdReady)))
	body = append(body, cmdReady...)
	body = appendCounter(body, counter)
	body = append(body, readyBox...)
	return body, nil
}

// parseReady decodes the READY body on the client side and stores the
// server's metadata.
func (m *curveMech) parseReady(body []byte) error {
	minLen := 1 + len(cmdReady) + 8 + boxOverhead
	if len(body) < minLen {
		return fmt.Errorf("gozmq/security/curve: READY length %d < min %d: %w", len(body), minLen, zmqerr.ErrMalformedFrame)
	}
	if err := expectCommandName(body, cmdReady); err != nil {
		return err
	}
	off := 1 + len(cmdReady)
	counter := binary.BigEndian.Uint64(body[off : off+8])
	off += 8
	if err := m.requireCounterAdvance(counter, "READY"); err != nil {
		return err
	}
	nonce := shortNonce(prefixReady, counter)
	plain, ok := box.Open(nil, body[off:], (*[24]byte)(&nonce), (*[32]byte)(&m.peerEphPublic), (*[32]byte)(&m.ourEphSecret))
	if !ok {
		return fmt.Errorf("gozmq/security/curve: READY box open failed: %w", zmqerr.ErrAuthFailed)
	}
	props, err := zmtp.MetadataCodec.Decode(plain)
	if err != nil {
		return err
	}
	m.peerMeta = props
	m.peerShortNonce = counter + 1
	return nil
}

// --------------------------------------------------------------------------
// MESSAGE (traffic phase).
// --------------------------------------------------------------------------

// EncodeFrame wraps one plaintext ZMTP frame body in a MESSAGE command body
// suitable for emission as a COMMAND-flagged ZMTP frame. The returned bytes
// contain the full MESSAGE command body:
//
//	[7]"MESSAGE" + short-nonce(8) + box(flags || plaintext)
//
// RFC 26 carries multipart state in the encrypted inner flags byte, not in
// the outer ZMTP frame flags. We therefore consume outer MORE here and
// return a wireFlags value with MORE cleared; outer COMMAND is preserved.
func (m *curveMech) EncodeFrame(flags uint8, plain []byte) (uint8, []byte, error) {
	if m.state != stateDone {
		return 0, nil, fmt.Errorf("gozmq/security/curve: EncodeFrame before handshake: %w", zmqerr.ErrFSMViolation)
	}
	// Guard against counter wrap BEFORE use. RFC 26 expects rekeying
	// long before the counter exhausts; we don't implement rekey, so
	// tear down instead of reusing (prefix, counter). The MaxUint64
	// value itself is treated as already-exhausted so the last usable
	// counter is MaxUint64-1 — no wrap can occur.
	if err := checkNonceCounter(m.ourShortNonce, "send"); err != nil {
		return 0, nil, err
	}
	counter := m.ourShortNonce
	m.ourShortNonce++
	nonce := shortNonce(m.encPrefix, counter)

	// Box plaintext: 1-byte flags || frame body. RFC 26 uses only the MORE
	// bit here; outer COMMAND remains visible to the session layer.
	inner := make([]byte, 1+len(plain))
	inner[0] = flags & iface.FlagMore
	copy(inner[1:], plain)
	cipher := box.Seal(nil, inner, (*[24]byte)(&nonce), (*[32]byte)(&m.peerEphPublic), (*[32]byte)(&m.ourEphSecret))

	body := make([]byte, 0, 1+len(cmdMessage)+8+len(cipher))
	body = append(body, byte(len(cmdMessage)))
	body = append(body, cmdMessage...)
	body = appendCounter(body, counter)
	body = append(body, cipher...)
	return flags &^ iface.FlagMore, body, nil
}

// DecodeFrame reverses EncodeFrame. Returns ErrMalformedFrame on MAC
// failure per the iface contract.
func (m *curveMech) DecodeFrame(flags uint8, wire []byte) (uint8, []byte, error) {
	if m.state != stateDone {
		return 0, nil, fmt.Errorf("gozmq/security/curve: DecodeFrame before handshake: %w", zmqerr.ErrFSMViolation)
	}
	minLen := 1 + len(cmdMessage) + 8 + boxOverhead + 1
	if len(wire) < minLen {
		return 0, nil, fmt.Errorf("gozmq/security/curve: MESSAGE length %d < min %d: %w", len(wire), minLen, zmqerr.ErrMalformedFrame)
	}
	if err := expectCommandName(wire, cmdMessage); err != nil {
		return 0, nil, err
	}
	off := 1 + len(cmdMessage)
	counter := binary.BigEndian.Uint64(wire[off : off+8])
	off += 8
	// Guard against peer-side nonce wrap. If the peer transmits
	// MaxUint64 as its counter, its next MESSAGE would wrap to 0 and
	// reuse a (prefix, counter) pair we have already accepted. That
	// is a protocol violation — the peer is supposed to rekey long
	// before this point — so tear down the session. This mirrors the
	// outbound guard in EncodeFrame. See checkNonceCounter for the
	// "MaxUint64 treated as exhausted" rationale.
	if err := checkNonceCounter(counter, "recv"); err != nil {
		return 0, nil, err
	}
	// Reject replays/duplicates before spending work on box.Open.
	if err := m.requireCounterAdvance(counter, "MESSAGE"); err != nil {
		return 0, nil, err
	}
	nonce := shortNonce(m.decPrefix, counter)
	plain, ok := box.Open(nil, wire[off:], (*[24]byte)(&nonce), (*[32]byte)(&m.peerEphPublic), (*[32]byte)(&m.ourEphSecret))
	if !ok {
		return 0, nil, fmt.Errorf("gozmq/security/curve: MESSAGE box open failed: %w", zmqerr.ErrMalformedFrame)
	}
	if len(plain) < 1 {
		return 0, nil, fmt.Errorf("gozmq/security/curve: MESSAGE plaintext empty: %w", zmqerr.ErrMalformedFrame)
	}
	if plain[0]&^iface.FlagMore != 0 {
		return 0, nil, fmt.Errorf("gozmq/security/curve: MESSAGE inner flags %#x invalid: %w", plain[0], zmqerr.ErrMalformedFrame)
	}
	// Strip the 1-byte inner flags prefix and restore MORE to the logical
	// outer flags seen by the session layer. plain is a fresh slice
	// produced by box.Open; slicing it (rather than make+copy) is safe —
	// the backing array has no other referents.
	m.peerShortNonce = counter + 1
	return (flags &^ iface.FlagMore) | (plain[0] & iface.FlagMore), plain[1:], nil
}

// --------------------------------------------------------------------------
// Helpers.
// --------------------------------------------------------------------------

// expectCommandName verifies that body starts with a 1-byte length prefix
// equal to len(name), followed by the literal name bytes. Matches libzmq's
// case-sensitive comparison (uppercase ASCII tokens per RFC 26).
func expectCommandName(body []byte, name string) error {
	if len(body) < 1+len(name) {
		return fmt.Errorf("gozmq/security/curve: %s command truncated in name: %w", name, zmqerr.ErrMalformedFrame)
	}
	if int(body[0]) != len(name) {
		return fmt.Errorf("gozmq/security/curve: %s name-length byte %d != %d: %w", name, body[0], len(name), zmqerr.ErrMalformedFrame)
	}
	got := string(body[1 : 1+len(name)])
	if !strings.EqualFold(got, name) {
		return fmt.Errorf("gozmq/security/curve: expected %s, got %q: %w", name, got, zmqerr.ErrMalformedFrame)
	}
	return nil
}

// checkNonceCounter returns an error wrapping zmqerr.ErrNonceExhausted
// if counter has reached math.MaxUint64, meaning the NEXT increment
// would wrap to 0 and reuse a (prefix, counter) nonce — catastrophic
// for NaCl's authenticated encryption. Callers must invoke this before
// incrementing ourShortNonce/peerShortNonce.
//
// Treating MaxUint64 itself as "already exhausted" (rather than the
// last usable value) gives us a strictly-less-than-2^64 budget, which
// is still 18 quintillion frames per direction — far beyond any
// practical session — and keeps the guard arithmetic free of wrap-
// around subtleties.
//
// direction is a short label ("send"/"recv") included in the wrapped
// error message for operator diagnosis; the sentinel is the same in
// both cases so errors.Is is unambiguous.
func checkNonceCounter(counter uint64, direction string) error {
	if counter == math.MaxUint64 {
		return fmt.Errorf("gozmq/security/curve: %s nonce counter exhausted: %w", direction, zmqerr.ErrNonceExhausted)
	}
	return nil
}

// requireCounterAdvance rejects an inbound short-nonce counter that does not
// advance peerShortNonce, i.e. one that repeats or precedes a counter we have
// already accepted. CurveZMQ (RFC 26 §3) leans on monotonic per-direction
// counters for replay defence because NaCl's AEAD has no built-in notion of
// "seen before"; without this check, an on-path attacker can re-inject a
// captured MESSAGE (or handshake command) and the box opens successfully.
//
// We require strictly greater than the LAST accepted counter, equivalently
// >= the next expected value stored in peerShortNonce. Gaps are tolerated —
// matching libzmq, which also allows the peer to skip counters (e.g. after
// an attacker-truncated sequence) rather than stalling the session.
func (m *curveMech) requireCounterAdvance(counter uint64, frame string) error {
	if counter < m.peerShortNonce {
		return fmt.Errorf("gozmq/security/curve: %s counter %d does not advance peer nonce %d: %w", frame, counter, m.peerShortNonce, zmqerr.ErrMalformedFrame)
	}
	return nil
}

// lowOrderPoints lists the known Curve25519 u-coordinates whose order
// divides 8 (so X25519 with these as the peer's public collapses to a
// predictable small-subgroup element regardless of our secret). RFC 7748
// §6.1 calls these out as points an implementation should reject when
// contributory behaviour is required; CurveZMQ relies on contributory
// ECDH through nacl/box, so we reject these before they reach box.Seal
// or box.Open.
//
// The eight values are the canonical list published by Daniel J.
// Bernstein and re-used by libsodium's crypto_scalarmult validation:
//
//  1. 0x00 × 32                    — zero u-coordinate (order 4 point)
//  2. 0x01 followed by 0x00 × 31   — order-1 point
//  3. 325606250916557431795983626356110631294008115727848805560023387167927233504  — order-8 point (little-endian below)
//  4. 39382357235489614581723060781553021112529911719440698176882885853963445705823 — order-8 point (second)
//  5. p - 1                         — order-2 point
//  6. p                             — equivalent to 0 mod p
//  7. p + 1                         — equivalent to 1 mod p
//  8. p + (order-8 point #1)        — wrap-around variants
//
// Hard-coded little-endian 32-byte encodings follow. They're also
// reachable via the constants used by x/crypto/curve25519 internally,
// but that package does not export them, so we mirror the list here.
var lowOrderPoints = [...][32]byte{
	// 0 (also p, p*2, ...).
	{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	},
	// 1 (order 1).
	{
		0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	},
	// Order-8 point A.
	{
		0xe0, 0xeb, 0x7a, 0x7c, 0x3b, 0x41, 0xb8, 0xae,
		0x16, 0x56, 0xe3, 0xfa, 0xf1, 0x9f, 0xc4, 0x6a,
		0xda, 0x09, 0x8d, 0xeb, 0x9c, 0x32, 0xb1, 0xfd,
		0x86, 0x62, 0x05, 0x16, 0x5f, 0x49, 0xb8, 0x00,
	},
	// Order-8 point B.
	{
		0x5f, 0x9c, 0x95, 0xbc, 0xa3, 0x50, 0x8c, 0x24,
		0xb1, 0xd0, 0xb1, 0x55, 0x9c, 0x83, 0xef, 0x5b,
		0x04, 0x44, 0x5c, 0xc4, 0x58, 0x1c, 0x8e, 0x86,
		0xd8, 0x22, 0x4e, 0xdd, 0xd0, 0x9f, 0x11, 0x57,
	},
	// p - 1 (order 2).
	{
		0xec, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f,
	},
	// p (≡ 0) — wrap-around of the zero point.
	{
		0xed, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f,
	},
	// p + 1 (≡ 1) — wrap-around of the order-1 point.
	{
		0xee, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f,
	},
	// p + order-8 point A — wrap-around.
	{
		0xcd, 0xeb, 0x7a, 0x7c, 0x3b, 0x41, 0xb8, 0xae,
		0x16, 0x56, 0xe3, 0xfa, 0xf1, 0x9f, 0xc4, 0x6a,
		0xda, 0x09, 0x8d, 0xeb, 0x9c, 0x32, 0xb1, 0xfd,
		0x86, 0x62, 0x05, 0x16, 0x5f, 0x49, 0xb8, 0x80,
	},
}

// validatePeerPublicKey rejects the all-zero key and Curve25519 low-order
// points before they are used in box.Open/box.Seal. Returns an error
// wrapping ErrAuthFailed on rejection.
//
// All comparisons are constant-time (crypto/subtle) to avoid timing
// oracles. The check is cheap — eight 32-byte compares — and runs once
// per handshake per peer key, not per MESSAGE, so there is no hot-path
// cost.
func validatePeerPublicKey(pk []byte) error {
	if len(pk) != KeySize {
		return fmt.Errorf("gozmq/security/curve: peer public key length %d != %d: %w", len(pk), KeySize, zmqerr.ErrAuthFailed)
	}
	var zero [KeySize]byte
	if subtle.ConstantTimeCompare(pk, zero[:]) == 1 {
		return fmt.Errorf("gozmq/security/curve: peer public key is zero or low-order: %w", zmqerr.ErrAuthFailed)
	}
	// Collapse the hit flag into a single bit without branching on pk.
	var hit int
	for i := range lowOrderPoints {
		hit |= subtle.ConstantTimeCompare(pk, lowOrderPoints[i][:])
	}
	if hit != 0 {
		return fmt.Errorf("gozmq/security/curve: peer public key is zero or low-order: %w", zmqerr.ErrAuthFailed)
	}
	return nil
}

// derivePublic computes the Curve25519 public key that pairs with secret.
// The nacl/box package exposes GenerateKey (fresh pair) but no
// public-from-secret helper, so we go through curve25519.X25519 against
// the base point. Returns a wrapped error rather than a plain one so the
// caller sees a package-qualified message.
func derivePublic(secret Key) (Key, error) {
	pubBytes, err := curve25519.X25519(secret[:], curve25519.Basepoint)
	if err != nil {
		return Key{}, err
	}
	var pub Key
	copy(pub[:], pubBytes)
	return pub, nil
}


func appendCounter(buf []byte, c uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], c)
	return append(buf, b[:]...)
}

func copyMeta(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func mergeMeta(base, overlay map[string]string) map[string]string {
	if len(base) == 0 {
		return copyMeta(overlay)
	}
	if len(overlay) == 0 {
		return copyMeta(base)
	}
	out := copyMeta(base)
	for k, v := range overlay {
		out[k] = v
	}
	return out
}
