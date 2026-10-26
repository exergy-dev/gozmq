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
//     name, 4-byte big-endian value length, value) via the same internal
//     zmtp codec NULL/PLAIN use; the shared mechanism plumbing (metadata
//     maps + ZAP hook) is embedded from the parent security package.

package curve

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
	"github.com/exergy-dev/gozmq/security"

	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/nacl/box"
	"golang.org/x/crypto/nacl/secretbox"
)

const (
	mechName  = "CURVE"
	mechLabel = "curve" // in error text

	// Fixed body sizes for HELLO and WELCOME (the only fixed-size
	// handshake commands). INITIATE and READY vary with metadata.
	helloBodySize   = 1 + len(zmtp.CmdHello) + 2 + 72 + 32 + 8 + 80
	welcomeBodySize = 1 + len(zmtp.CmdWelcome) + 16 + 144

	// Inner fixed sizes inside HELLO / WELCOME / INITIATE.
	signatureBoxSize = 64 + box.Overhead    // 80 bytes
	welcomeBoxSize   = 128 + box.Overhead   // 144 bytes
	cookieInnerSize  = 64 + box.Overhead    // 80 bytes (secretbox cipher)
	cookieSize       = 16 + cookieInnerSize // 96 bytes (nonce + cipher)
	vouchBoxSize     = 64 + box.Overhead    // 80 bytes
	vouchSize        = 16 + vouchBoxSize    // 96 bytes (nonce + cipher)

	// Protocol version this implementation emits. RFC 26 specifies 1.0.
	curveVersionMajor = 0x01
	curveVersionMinor = 0x00

	// Bits of the encrypted MESSAGE flags byte (libzmq curve_mechanism_base
	// flag_mask_more / flag_mask_command). A frame's MORE and COMMAND state
	// travel here, inside the box; the outer ZMTP frame carrying a MESSAGE
	// has neither flag set. EncodeFrame/DecodeFrame translate with masks
	// and a shift, relying on zmtp.FlagMore == innerFlagMore and
	// zmtp.FlagCommand == innerFlagCommand<<1 (pinned by a test).
	innerFlagMore    uint8 = 0x01
	innerFlagCommand uint8 = 0x02
)

// curveState tracks which handshake command we've sent/consumed.
type curveState uint8

const (
	stateStart        curveState = iota // nothing sent/consumed
	stateHelloSent                      // client: HELLO sent, awaiting WELCOME
	stateWelcomeSent                    // server: WELCOME sent, awaiting INITIATE
	stateInitiateSent                   // client: INITIATE sent, awaiting READY
	stateDone                           // handshake complete
)

// curveMech implements security.Mech for CURVE. The same struct backs
// both client and server roles; role-specific behavior branches inside
// NextHandshake. The embedded security.MechBase carries the metadata maps
// and the server-side ZAP hook shared with NULL/PLAIN.
type curveMech struct {
	security.MechBase

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

	// hsKey and shared are the two precomputed crypto_box keys, each
	// derived once and used for every box under it: hsKey is the long-term/
	// ephemeral pair of HELLO's signature box and the WELCOME box (client
	// (S, c'), server (C', s)); shared is the ephemeral pair of INITIATE,
	// READY and every MESSAGE (client (S', c'), server (C', s')). hsKey is
	// valid from construction (client) or parseHello (server); shared from
	// parseWelcome (client) or parseHello (server).
	hsKey  [32]byte
	shared [32]byte

	// Short-nonce counters, per direction. RFC 26 §3 specifies a single
	// monotonic counter per direction starting at 1 and spanning HELLO /
	// INITIATE / READY and every MESSAGE that follows — the counter does
	// NOT reset at the handshake → traffic boundary. libzmq rejects
	// MESSAGE frames whose counter regresses below the last handshake
	// counter it observed, which is why traffic-phase interop requires a
	// single unified counter here.
	ourShortNonce  uint64
	peerShortNonce uint64

	// MESSAGE nonce templates chosen by role: prefix with a zero counter.
	// The client encodes with "CurveZMQMESSAGEC" and decodes with
	// "CurveZMQMESSAGES"; the server is the mirror.
	encNonce [24]byte
	decNonce [24]byte

	state curveState

	// rand source for random nonces (long nonces, cookie keys, ephemeral
	// keys). Defaults to crypto/rand. newMech accepts a deterministic
	// io.Reader so tests can pin the ephemeral keys and long nonces.
	rand io.Reader

	// encScratch is the reusable inner-plaintext buffer (1 flags byte +
	// payload) for EncodeFrame. It is per-mechanism, NOT synchronized:
	// the concurrency contract (documented on EncodeFrame/DecodeFrame)
	// is one goroutine per direction, so EncodeFrame never runs
	// concurrently with itself. The buffer grows to the largest payload
	// sealed so far and is retained for the life of the session.
	encScratch []byte
}

// NewCurveServer returns a CURVE mechanism configured as the server side.
// serverSecret is the server's long-term secret; the corresponding public
// key is derived from it at construction. The server accepts any client
// public key presented in INITIATE — authorization via ZAP is a separate
// hook layered above this mechanism.
//
// localMeta is the property list sent in the server's READY. A nil map is
// treated as an empty property list.
func NewCurveServer(serverSecret Key, localMeta map[string]string) (security.Mech, error) {
	m, err := newMech(security.RoleServer, serverSecret, Key{}, Key{}, localMeta, rand.Reader)
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
func NewCurveClient(serverPublic, clientPublic, clientSecret Key, localMeta map[string]string) (security.Mech, error) {
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
	m, err := newMech(security.RoleClient, clientSecret, clientPublic, serverPublic, localMeta, rand.Reader)
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
func newMech(role security.Role, ownSecret, ownPublicHint, peerLongTerm Key, localMeta map[string]string, r io.Reader) (*curveMech, error) {
	if r == nil {
		r = rand.Reader
	}

	ourPub, err := derivePublic(ownSecret)
	if err != nil {
		return nil, fmt.Errorf("gozmq/security/curve: derive public from secret: %w", err)
	}
	// If the caller supplied a public-key hint (client form), validate it.
	// The server form passes zero-value — skip the check.
	if ownPublicHint != (Key{}) && ownPublicHint != ourPub {
		return nil, authFailed("supplied public key does not match secret")
	}

	// Generate our ephemeral keypair up front. Both client and server use
	// it: the client sends C' in HELLO; the server sends S' inside the
	// WELCOME box.
	ephPubPtr, ephSecPtr, err := box.GenerateKey(r)
	if err != nil {
		return nil, fmt.Errorf("gozmq/security/curve: generate ephemeral key: %w", err)
	}
	ephPub, ephSec := Key(*ephPubPtr), Key(*ephSecPtr)

	m := &curveMech{
		MechBase:       security.NewMechBase(role, localMeta),
		ourPublic:      ourPub,
		ourSecret:      ownSecret,
		peerPublic:     peerLongTerm,
		ourEphPublic:   ephPub,
		ourEphSecret:   ephSec,
		ourShortNonce:  1,
		peerShortNonce: 1,
		rand:           r,
	}

	// Per-role MESSAGE prefix assignment.
	if role == security.RoleClient {
		m.encNonce, m.decNonce = shortNonce(prefixMessageC, 0), shortNonce(prefixMessageS, 0)
		m.hsKey = precompute(&m.peerPublic, &m.ourEphSecret)
		return m, nil
	}
	m.encNonce, m.decNonce = shortNonce(prefixMessageS, 0), shortNonce(prefixMessageC, 0)
	// Server-side cookie key is a fresh 32-byte random used only to
	// secretbox cookies across HELLO → INITIATE. It never leaves the
	// server. Generate now so any RNG error surfaces during construction,
	// not mid-handshake.
	if _, err := io.ReadFull(r, m.cookieKey[:]); err != nil {
		return nil, fmt.Errorf("gozmq/security/curve: generate cookie key: %w", err)
	}
	return m, nil
}

// Name is the greeting mechanism token.
func (*curveMech) Name() string { return mechName }

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
	if m.Role() == security.RoleClient {
		return m.nextClient(in)
	}
	return m.nextServer(in)
}

// nextClient steps the client side of the handshake.
func (m *curveMech) nextClient(in []byte) ([]byte, bool, error) {
	switch m.state {
	case stateStart:
		if err := security.RequireNoInput(mechLabel, in, "client seed"); err != nil {
			return nil, false, err
		}
		body, err := m.buildHello()
		if err != nil {
			return nil, false, err
		}
		m.state = stateHelloSent
		return body, false, nil

	case stateHelloSent:
		if err := security.RequireInput(mechLabel, in, "WELCOME"); err != nil {
			return nil, false, err
		}
		welcome, err := security.DecodeReply(in, zmtp.CmdWelcome)
		if err != nil {
			return nil, false, err
		}
		cookie, err := m.parseWelcome(welcome)
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
		if err := security.RequireInput(mechLabel, in, "READY"); err != nil {
			return nil, false, err
		}
		ready, err := security.DecodeReply(in, zmtp.CmdReady)
		if err != nil {
			return nil, false, err
		}
		if err := m.parseReady(ready); err != nil {
			return nil, false, err
		}
		m.finishHandshake()
		return nil, true, nil

	case stateDone:
		return nil, true, nil

	default:
		return nil, false, malformed("client state %d", m.state)
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
		if err := security.RequireInput(mechLabel, in, "INITIATE"); err != nil {
			return nil, false, err
		}
		// parseInitiate may return an outbound ERROR command body alongside
		// the error (ZAP rejection) — mirror the PLAIN pattern and hand it
		// to the session layer so the peer sees an auth-failure reason.
		errOut, err := m.parseInitiate(in)
		if err != nil {
			return errOut, false, err
		}
		body := m.buildReady()
		m.finishHandshake()
		return body, true, nil

	case stateDone:
		return nil, true, nil

	default:
		return nil, false, malformed("server state %d", m.state)
	}
}

// finishHandshake transitions to stateDone. The traffic-phase key (shared)
// is already precomputed, so the ephemeral secret is no longer needed and
// is wiped (best-effort).
func (m *curveMech) finishHandshake() {
	clear(m.ourEphSecret[:])
	m.state = stateDone
}

// buildHello constructs the HELLO command body: [5]"HELLO" + version +
// padding + C' + short-nonce-counter + signature-box.
func (m *curveMech) buildHello() ([]byte, error) {
	body := zmtp.AppendCommandName(make([]byte, 0, helloBodySize), zmtp.CmdHello)
	body = append(body, curveVersionMajor, curveVersionMinor)
	body = append(body, make([]byte, 72)...)
	body = append(body, m.ourEphPublic[:]...)
	// Signature box: 64 zero bytes encrypted with c' + S.
	return m.appendShortBox(body, prefixHello, zeros64[:], &m.hsKey), nil
}

// parseHello validates a HELLO body and stores the client ephemeral public.
// The signature box is opened to prove the client knows c' (the private
// half of the advertised ephemeral public). Any failure wraps
// zmqerr.ErrMalformedFrame or zmqerr.ErrAuthFailed.
func (m *curveMech) parseHello(body []byte) error {
	if len(body) != helloBodySize {
		return malformed("HELLO length %d != %d", len(body), helloBodySize)
	}
	if _, err := zmtp.ExpectCommand(body, zmtp.CmdHello); err != nil {
		return err
	}
	off := 1 + len(zmtp.CmdHello)
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
	m.peerEphPublic = Key(body[off : off+32])
	off += 32
	m.hsKey = precompute(&m.peerEphPublic, &m.ourSecret)
	m.shared = precompute(&m.peerEphPublic, &m.ourEphSecret)
	// Short nonce counter and signature box.
	plain, err := m.openShort(prefixHello, "HELLO", body[off:], &m.hsKey)
	if err != nil {
		return err
	}
	if !bytes.Equal(plain, zeros64[:]) {
		return authFailed("HELLO signature plaintext is not 64 zero bytes")
	}
	return nil
}

// buildWelcome constructs the WELCOME command body on the server side.
// WELCOME's freshness comes from a random long-nonce (16 bytes), not a
// counter, because the server is intentionally stateless between HELLO and
// INITIATE — the cookie inside the welcome box lets the server restore its
// ephemeral secret when INITIATE arrives.
func (m *curveMech) buildWelcome() ([]byte, error) {
	welR, err := m.random16("welcome")
	if err != nil {
		return nil, err
	}
	// Cookie: secretbox(C' || s') under the server-only cookieKey and a
	// random long nonce, sent as nonce(16) || cipher(80).
	cookieNonceR, err := m.random16("cookie")
	if err != nil {
		return nil, err
	}
	cookieNonce := longNonce(prefixCookie, cookieNonceR[:])
	cookiePlain := slices.Concat(m.peerEphPublic[:], m.ourEphSecret[:])
	cookie := secretbox.Seal(cookieNonceR[:], cookiePlain, &cookieNonce, &m.cookieKey)
	clear(cookiePlain)

	// Welcome plaintext: S' || cookie (32 + 96 = 128 bytes), boxed under
	// (C', s).
	welPlain := slices.Concat(m.ourEphPublic[:], cookie)
	body := zmtp.AppendCommandName(make([]byte, 0, welcomeBodySize), zmtp.CmdWelcome)
	return sealLong(body, prefixWelcome, welR, welPlain, &m.hsKey), nil
}

// parseWelcome decodes the WELCOME body, extracts and stores the server
// ephemeral public key S', and returns the 96-byte opaque cookie that the
// client must echo back in INITIATE.
func (m *curveMech) parseWelcome(body []byte) ([]byte, error) {
	if len(body) != welcomeBodySize-1-len(zmtp.CmdWelcome) {
		return nil, malformed("WELCOME body length %d != %d", len(body), welcomeBodySize-1-len(zmtp.CmdWelcome))
	}
	plain, ok := openLong(prefixWelcome, body, &m.hsKey)
	if !ok {
		return nil, authFailed("WELCOME box open failed")
	}
	if len(plain) != 128 {
		return nil, malformed("WELCOME plaintext length %d != 128", len(plain))
	}
	// Validate the server's advertised ephemeral (S') before binding
	// into m.peerEphPublic. A malicious or broken server could inject
	// a low-order point here to drive the post-handshake MESSAGE
	// shared secret to a known small-subgroup value.
	if err := validatePeerPublicKey(plain[:32]); err != nil {
		return nil, err
	}
	m.peerEphPublic = Key(plain[:32])
	m.shared = precompute(&m.peerEphPublic, &m.ourEphSecret)
	return plain[32:], nil
}

// buildInitiate constructs the INITIATE command body on the client side.
// Layout: [8]"INITIATE" + cookie(96) + short-nonce(8) + initiate-box(variable).
func (m *curveMech) buildInitiate(cookie []byte) ([]byte, error) {
	if len(cookie) != cookieSize {
		return nil, malformed("INITIATE cookie length %d != %d", len(cookie), cookieSize)
	}

	// Vouch box: plaintext C'||S (client ephemeral public || server long-
	// term public), encrypted c -> S' with nonce "VOUCH---"+random. Per
	// RFC 26 §2.3 the vouch binds the client's ephemeral key to the
	// server's LONG-TERM identity, not to its ephemeral key — libzmq
	// rejects C'||S' as "vouch S' mismatch".
	vouchPlain := slices.Concat(m.ourEphPublic[:], m.peerPublic[:])
	vouchKey := precompute(&m.peerEphPublic, &m.ourSecret)

	// Initiate box plaintext: C || vouch || metadata.
	meta := zmtp.EncodeMetadata(m.Metadata())
	initPlain := append(make([]byte, 0, 32+vouchSize+len(meta)), m.ourPublic[:]...)
	vouchR, err := m.random16("vouch")
	if err != nil {
		return nil, err
	}
	initPlain = sealLong(initPlain, prefixVouch, vouchR, vouchPlain, &vouchKey)
	initPlain = append(initPlain, meta...)

	// Assemble INITIATE body: name, cookie, counter, box under "CurveZMQINITIATE" + counter.
	body := zmtp.AppendCommandName(make([]byte, 0, 1+len(zmtp.CmdInitiate)+cookieSize+8+len(initPlain)+box.Overhead), zmtp.CmdInitiate)
	body = append(body, cookie...)
	return m.appendShortBox(body, prefixInitiate, initPlain, &m.shared), nil
}

// parseInitiate decodes the INITIATE body on the server side, opens the
// cookie to recover the paired ephemeral secret, opens the initiate box
// to extract C (client long-term public) and the metadata, and validates
// the vouch to confirm the advertised C controls c.
//
// On a ZAP authorization rejection the first return value carries an
// encoded ZMTP ERROR command body for the session layer to emit before
// tearing down (mirroring PLAIN's auth-failure path); it is nil for every
// other outcome.
func (m *curveMech) parseInitiate(body []byte) ([]byte, error) {
	minLen := 1 + len(zmtp.CmdInitiate) + cookieSize + 8 + box.Overhead + 32 + vouchSize
	if len(body) < minLen {
		return nil, malformed("INITIATE length %d < min %d", len(body), minLen)
	}
	if _, err := zmtp.ExpectCommand(body, zmtp.CmdInitiate); err != nil {
		return nil, err
	}
	off := 1 + len(zmtp.CmdInitiate)

	// Cookie — we decrypt it using our cookieKey to restore (C', s').
	cookieNonceR := body[off : off+16]
	cookieCipher := body[off+16 : off+cookieSize]
	off += cookieSize
	cookieNonce := longNonce(prefixCookie, cookieNonceR)
	cookiePlain, ok := secretbox.Open(nil, cookieCipher, &cookieNonce, &m.cookieKey)
	if !ok {
		return nil, authFailed("INITIATE cookie open failed")
	}
	if len(cookiePlain) != 64 {
		return nil, malformed("INITIATE cookie plaintext length %d != 64", len(cookiePlain))
	}
	// Restore (C', s') from the cookie. RFC 26 designs the cookie so the
	// server can be stateless between HELLO and INITIATE — the cookie is
	// authoritative. We also hold the values in memory from HELLO and
	// WELCOME; assert equality as a corruption check before trusting the
	// cookie. The plaintext (which contains s') is wiped as soon as the
	// comparison is done.
	cprimeOK := Key(cookiePlain[:32]) == m.peerEphPublic
	sprimeOK := subtle.ConstantTimeCompare(cookiePlain[32:], m.ourEphSecret[:]) == 1
	clear(cookiePlain)
	if !cprimeOK {
		return nil, authFailed("cookie C' mismatch")
	}
	if !sprimeOK {
		return nil, authFailed("cookie s' mismatch")
	}

	// Short-nonce counter and initiate box.
	initPlain, err := m.openShort(prefixInitiate, "INITIATE", body[off:], &m.shared)
	if err != nil {
		return nil, err
	}
	if len(initPlain) < 32+vouchSize {
		return nil, malformed("INITIATE plaintext too short")
	}

	// Extract C (client long-term public), vouch, and metadata.
	// Validate C before feeding it into box.Open for the vouch. A
	// client that presents a zero or low-order long-term key could
	// otherwise force the server into a small-subgroup ECDH, making
	// the vouch verification meaningless and every subsequent MESSAGE
	// key predictable.
	if err := validatePeerPublicKey(initPlain[:32]); err != nil {
		return nil, err
	}
	clientLongPub := Key(initPlain[:32])
	metaBytes := initPlain[32+vouchSize:]

	// Open the vouch, sealed with c (client long-term secret) for S' (our
	// ephemeral public): the key is (C, s').
	vouchKey := precompute(&clientLongPub, &m.ourEphSecret)
	vouchPlain, ok := openLong(prefixVouch, initPlain[32:32+vouchSize], &vouchKey)
	if !ok {
		return nil, authFailed("INITIATE vouch open failed")
	}
	if len(vouchPlain) != 64 {
		return nil, malformed("vouch plaintext length %d != 64", len(vouchPlain))
	}
	// Per RFC 26 §2.3 the vouch plaintext is C'||S — client ephemeral
	// public followed by server LONG-TERM public (ourPublic here, since
	// we are the server decoding a client's vouch).
	if Key(vouchPlain[:32]) != m.peerEphPublic {
		return nil, authFailed("vouch C' mismatch")
	}
	if Key(vouchPlain[32:]) != m.ourPublic {
		return nil, authFailed("vouch S mismatch")
	}

	// Client long-term public is now authenticated. Record it and parse
	// metadata.
	m.peerPublic = clientLongPub
	props, err := zmtp.DecodeMetadata(metaBytes)
	if err != nil {
		return nil, err
	}
	if err := m.AuthorizeZAP(mechName, [][]byte{clientLongPub[:]}); err != nil {
		// Hand back an ERROR command body so the peer learns why it was
		// dropped. The reason is deliberately terse — authorization
		// detail stays on the server.
		return zmtp.EncodeError("Authentication failure"), fmt.Errorf("gozmq/security/curve: ZAP rejected client key: %w", err)
	}
	m.MergePeerMetadata(props)
	// The per-connection cookie key is no longer needed; drop it
	// (best-effort). The long-term secret is not wiped: the socket's
	// configuration keeps its own copy for every later connection.
	clear(m.cookieKey[:])
	return nil, nil
}

// buildReady constructs the READY command body. Layout:
// [5]"READY" + short-nonce(8) + ready-box(metadata ciphertext).
func (m *curveMech) buildReady() []byte {
	meta := zmtp.EncodeMetadata(m.Metadata())
	body := zmtp.AppendCommandName(make([]byte, 0, 1+len(zmtp.CmdReady)+8+len(meta)+box.Overhead), zmtp.CmdReady)
	return m.appendShortBox(body, prefixReady, meta, &m.shared)
}

// parseReady decodes the READY body on the client side and stores the
// server's metadata.
func (m *curveMech) parseReady(body []byte) error {
	if len(body) < 8+box.Overhead {
		return malformed("READY body length %d < min %d", len(body), 8+box.Overhead)
	}
	plain, err := m.openShort(prefixReady, "READY", body, &m.shared)
	if err != nil {
		return err
	}
	props, err := zmtp.DecodeMetadata(plain)
	if err != nil {
		return err
	}
	m.MergePeerMetadata(props)
	return nil
}

// EncodeFrame wraps one plaintext ZMTP frame body (data or command) in a
// MESSAGE command body, emitted as the body of a single ZMTP frame. The
// returned bytes contain the full MESSAGE command body:
//
//	[7]"MESSAGE" + short-nonce(8) + box(flags || plaintext)
//
// RFC 26 carries multipart state in the encrypted inner flags byte, not in
// the outer ZMTP frame flags, and libzmq does the same for the COMMAND
// flag: a traffic-phase command (PING, PONG, ...) is boxed with the inner
// command bit and sent as an ordinary MESSAGE frame. We therefore consume
// MORE and COMMAND here and return a wireFlags value with both cleared,
// matching libzmq's wire format.
//
// Concurrency contract: EncodeFrame must be called from at most one
// goroutine, and DecodeFrame from at most one goroutine (the two may be
// different — one per direction). The session layer satisfies this: the
// writer goroutine is the sole EncodeFrame caller and the reader goroutine
// the sole DecodeFrame caller. EncodeFrame relies on this to reuse the
// per-mechanism encScratch buffer; the nonce counters assume it too.
func (m *curveMech) EncodeFrame(flags uint8, plain []byte) (uint8, []byte, error) {
	wire, wireFlags, err := m.AppendEncodeFrame(make([]byte, 0, m.FrameOverhead()+len(plain)), flags, plain)
	return wireFlags, wire, err
}

// FrameOverhead reports the bytes a MESSAGE adds to a plaintext frame on
// the wire: the command name, the nonce counter, the inner flags byte and
// the box tag. The session sizes its frame codec by it so WithMaxMessageSize
// bounds the plaintext a caller sees, not the ciphertext.
func (m *curveMech) FrameOverhead() int { return 1 + len(zmtp.CmdMessage) + 8 + 1 + box.Overhead }

// AppendEncodeFrame implements security.FrameAppender: EncodeFrame sealed
// straight into dst.
func (m *curveMech) AppendEncodeFrame(dst []byte, flags uint8, plain []byte) ([]byte, uint8, error) {
	if m.state != stateDone {
		return dst, 0, fmt.Errorf("gozmq/security/curve: EncodeFrame before handshake: %w", zmqerr.ErrFSMViolation)
	}
	// Guard against counter wrap BEFORE use. RFC 26 expects rekeying
	// long before the counter exhausts; we don't implement rekey, so
	// tear down instead of reusing (prefix, counter). The MaxUint64
	// value itself is treated as already-exhausted so the last usable
	// counter is MaxUint64-1 — no wrap can occur.
	if err := checkNonceCounter(m.ourShortNonce, "send"); err != nil {
		return dst, 0, err
	}
	counter := m.nextCounter()
	nonce := withCounter(m.encNonce, counter)

	// Box plaintext: 1-byte flags || frame body. The shared key was
	// precomputed in finishHandshake, so sealing is pure XSalsa20/Poly1305,
	// no per-frame ECDH. The inner plaintext lives in the per-mechanism
	// scratch buffer (see the encScratch field and the concurrency contract
	// above); Seal appends the ciphertext to dst.
	if cap(m.encScratch) < 1+len(plain) {
		m.encScratch = make([]byte, 1+len(plain))
	}
	inner := m.encScratch[:1+len(plain)]
	inner[0] = flags&zmtp.FlagMore | (flags&zmtp.FlagCommand)>>1
	copy(inner[1:], plain)

	dst = zmtp.AppendCommandName(dst, zmtp.CmdMessage)
	dst = binary.BigEndian.AppendUint64(dst, counter)
	dst = box.SealAfterPrecomputation(dst, inner, &nonce, &m.shared)
	return dst, flags &^ (zmtp.FlagMore | zmtp.FlagCommand), nil
}

// DecodeFrame reverses EncodeFrame. Returns ErrMalformedFrame on MAC
// failure per the security.Mech contract.
func (m *curveMech) DecodeFrame(flags uint8, wire []byte) (uint8, []byte, error) {
	if m.state != stateDone {
		return 0, nil, fmt.Errorf("gozmq/security/curve: DecodeFrame before handshake: %w", zmqerr.ErrFSMViolation)
	}
	minLen := 1 + len(zmtp.CmdMessage) + 8 + box.Overhead + 1
	if len(wire) < minLen {
		return 0, nil, malformed("MESSAGE length %d < min %d", len(wire), minLen)
	}
	if !zmtp.HasCommandName(wire, zmtp.CmdMessage) {
		return 0, nil, malformed("not a MESSAGE command")
	}
	off := 1 + len(zmtp.CmdMessage)
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
	nonce := withCounter(m.decNonce, counter)
	plain, ok := box.OpenAfterPrecomputation(nil, wire[off:], &nonce, &m.shared)
	if !ok {
		return 0, nil, malformed("MESSAGE box open failed")
	}
	if len(plain) < 1 {
		return 0, nil, malformed("MESSAGE plaintext empty")
	}
	inner := plain[0]
	if inner&^(innerFlagMore|innerFlagCommand) != 0 {
		return 0, nil, malformed("MESSAGE inner flags %#x invalid", inner)
	}
	// Strip the 1-byte inner flags prefix and derive MORE and COMMAND for
	// the session layer from it alone. The outer wire flags are not covered
	// by the MAC, so they must not influence how authenticated data is
	// handled: an on-path attacker flipping the outer COMMAND bit would
	// otherwise turn a data frame into a command. libzmq likewise ignores
	// the outer flags of a MESSAGE.
	// plain is a fresh slice produced by box.Open; slicing it (rather than
	// make+copy) is safe — the backing array has no other referents.
	m.peerShortNonce = counter + 1
	return inner&innerFlagMore | (inner&innerFlagCommand)<<1, plain[1:], nil
}

// zeros64 is the HELLO signature plaintext.
var zeros64 [64]byte

// precompute is box.Precompute for the key pair (pub, sec).
func precompute(pub, sec *Key) (k [32]byte) {
	box.Precompute(&k, (*[32]byte)(pub), (*[32]byte)(sec))
	return k
}

// nextCounter returns the next outbound short-nonce counter and advances it.
func (m *curveMech) nextCounter() uint64 {
	c := m.ourShortNonce
	m.ourShortNonce++
	return c
}

// appendShortBox appends counter(8) || box(plain) to dst, the box under the
// short nonce (prefix, counter) with the next outbound counter and the
// precomputed key. The nonce shapes are defined in nonce.go.
func (m *curveMech) appendShortBox(dst []byte, prefix string, plain []byte, key *[32]byte) []byte {
	counter := m.nextCounter()
	nonce := shortNonce(prefix, counter)
	return box.SealAfterPrecomputation(binary.BigEndian.AppendUint64(dst, counter), plain, &nonce, key)
}

// openShort is the inverse of appendShortBox for a handshake command: wire is
// counter(8) || box. It rejects a counter that does not advance the peer's,
// opens the box and moves peerShortNonce past the counter. frame names the
// command in errors.
func (m *curveMech) openShort(prefix, frame string, wire []byte, key *[32]byte) ([]byte, error) {
	counter := binary.BigEndian.Uint64(wire)
	if err := m.requireCounterAdvance(counter, frame); err != nil {
		return nil, err
	}
	nonce := shortNonce(prefix, counter)
	plain, ok := box.OpenAfterPrecomputation(nil, wire[8:], &nonce, key)
	if !ok {
		return nil, authFailed("%s box open failed", frame)
	}
	m.peerShortNonce = counter + 1
	return plain, nil
}

// random16 draws the random half of a long nonce; what names it in the
// error.
func (m *curveMech) random16(what string) ([16]byte, error) {
	var r [16]byte
	if _, err := io.ReadFull(m.rand, r[:]); err != nil {
		return r, fmt.Errorf("gozmq/security/curve: %s nonce rand: %w", what, err)
	}
	return r, nil
}

// sealLong appends r || box(plain) to dst, the box under the long nonce
// (prefix, r) and the precomputed key: the WELCOME box and the INITIATE
// vouch, whose freshness is the random, not a counter.
func sealLong(dst []byte, prefix string, r [16]byte, plain []byte, key *[32]byte) []byte {
	nonce := longNonce(prefix, r[:])
	return box.SealAfterPrecomputation(append(dst, r[:]...), plain, &nonce, key)
}

// openLong is the inverse of sealLong: wire is random(16) || box. ok is
// false on MAC failure.
func openLong(prefix string, wire []byte, key *[32]byte) ([]byte, bool) {
	nonce := longNonce(prefix, wire[:16])
	return box.OpenAfterPrecomputation(nil, wire[16:], &nonce, key)
}

// malformed and authFailed build this package's errors wrapping
// zmqerr.ErrMalformedFrame and zmqerr.ErrAuthFailed.
func malformed(format string, args ...any) error {
	return security.Malformed(mechLabel, format, args...)
}

func authFailed(format string, args ...any) error {
	return security.AuthFailed(mechLabel, format, args...)
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
		return malformed("%s counter %d does not advance peer nonce %d", frame, counter, m.peerShortNonce)
	}
	return nil
}

// lowOrderProbe is an arbitrary fixed scalar for validatePeerPublicKey.
// X25519 clamps it to a multiple of 8, so the product is zero exactly for
// the small-order points, whatever the scalar.
var lowOrderProbe = [KeySize]byte{1}

// validatePeerPublicKey rejects the all-zero key and every Curve25519
// low-order point, in any encoding (including non-canonical ones and those
// with bit 255 set, which RFC 7748 masks off), before it is used in
// box.Open/box.Seal. nacl/box does not check: a low-order peer key makes the
// shared key a constant anyone can compute. Returns an error wrapping
// ErrAuthFailed on rejection.
//
// curve25519.X25519 returns an error precisely when its output is all zero,
// i.e. when the point has small order, and runs in constant time. It costs
// one scalar multiplication per handshake per peer key, never per MESSAGE.
func validatePeerPublicKey(pk []byte) error {
	if len(pk) != KeySize {
		return authFailed("peer public key length %d != %d", len(pk), KeySize)
	}
	if _, err := curve25519.X25519(lowOrderProbe[:], pk); err != nil {
		return authFailed("peer public key is zero or low-order")
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
	return Key(pubBytes), nil
}
