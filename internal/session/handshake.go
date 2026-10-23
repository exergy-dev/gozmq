package session

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
	"github.com/exergy-dev/gozmq/security"
)

// Greeting layout constants (ZMTP 3.1 §2.1).
const (
	greetingSize        = 64
	greetingMechOff     = 12
	greetingMechLen     = 20
	greetingAsServerOff = 32
	greetingVersionMaj  = 0x03
	greetingVersionMin  = 0x01
	greetingSigPrefix   = 0xFF
	greetingSigTerm     = 0x7F
)

// handshakeMaxFrame caps inbound handshake frames: the codec allocates a body
// from its length header, so a MaxMessageSize cap would let an unauthenticated
// peer pin that much per connection, while real handshake commands are a few
// hundred bytes.
const handshakeMaxFrame = 64 << 10

// handshakeCodec frames handshake commands; it is stateless, so one serves
// every session.
var handshakeCodec = zmtp.NewFrameCodec(handshakeMaxFrame)

// handshake drives greeting + security handshake on the reader goroutine
// under HandshakeTimeout, queueing outbound bytes on hsOut for the writer.
// On failure it returns an error wrapping zmqerr.ErrHandshakeTimeout (for
// timeouts) or the underlying cause, preferring the writer's error when a
// write failed, since the read error that follows is only its echo.
func (s *Session) handshake() error {
	err := s.runHandshake()
	if err != nil {
		if werr := s.hsWriteErr.Load(); werr != nil {
			err = fmt.Errorf("gozmq/session: write handshake: %w", *werr)
		}
		// net.Error timeouts (the SetDeadline path) report Timeout()=true.
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			err = fmt.Errorf("%w: %w", zmqerr.ErrHandshakeTimeout, err)
		}
	}
	return err
}

func (s *Session) runHandshake() error {
	if s.cfg.HandshakeTimeout > 0 {
		deadline := time.Now().Add(s.cfg.HandshakeTimeout)
		if err := s.cfg.Conn.SetDeadline(deadline); err != nil {
			return fmt.Errorf("gozmq/session: set handshake deadline: %w", err)
		}
	}
	if err := s.exchangeGreeting(); err != nil {
		return err
	}
	if err := s.runSecurityHandshake(); err != nil {
		return err
	}
	if err := s.finalizePeerMetadata(); err != nil {
		return err
	}
	// Traffic-phase I/O blocks indefinitely (the heartbeat polices
	// liveness). A failed handshake keeps the deadline so the writer's
	// last frames, such as an ERROR command, cannot hang.
	if s.cfg.HandshakeTimeout > 0 {
		_ = s.cfg.Conn.SetDeadline(time.Time{})
	}
	return nil
}

// exchangeGreeting queues our greeting, reads the peer's, validates it,
// and records peer-side capabilities (heartbeat support, mechanism name
// match) on the Session.
func (s *Session) exchangeGreeting() error {
	out, err := encodeGreeting(s.cfg.Mech.Name(), s.cfg.Mech.AsServer())
	if err != nil {
		return err
	}
	s.hsOut <- out

	var buf [greetingSize]byte
	if _, err := io.ReadFull(s.reader, buf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("%w: greeting truncated: %w", zmqerr.ErrMalformedFrame, err)
		}
		return fmt.Errorf("gozmq/session: read greeting: %w", err)
	}

	mech, peerMinor, peerAsServer, err := decodeGreeting(buf[:])
	if err != nil {
		return err
	}
	// Two servers (both ends WithCurveServer, or both WithPlainAuth) would
	// each wait for the other's first handshake command until
	// HandshakeTimeout. Fail at the greeting instead, so the
	// misconfiguration is reported at once.
	if peerAsServer && s.cfg.Mech.AsServer() {
		return fmt.Errorf("gozmq/session: both peers declare the %s server role (as-server=1): %w", mech, zmqerr.ErrMalformedFrame)
	}
	// ZMTP requires both peers to use the same mechanism and to close the
	// connection otherwise, as libzmq does. Leaving it to the mechanism's
	// parser would let a first message that happens to parse run the
	// handshake against a peer speaking something else.
	if mech != s.cfg.Mech.Name() {
		return fmt.Errorf("gozmq/session: security mechanism mismatch: peer %q, ours %q", mech, s.cfg.Mech.Name())
	}
	// Decode already rejected anything but ZMTP 3.x. Any minor >= 1 is
	// treated as 3.1 (heartbeats available); skip PING/PONG
	// if the peer is 3.0.
	if peerMinor >= 0x01 {
		s.peerHeartbeats = true
	}
	return nil
}

// runSecurityHandshake drives the lock-step NextHandshake loop. Each
// iteration: hand the previous inbound to the mechanism, queue its
// returned outbound (if any) for the writer, then read the peer's next
// message (unless the mechanism declared the handshake done).
//
// "Message" here is whatever NextHandshake returns — it's not framed
// in any particular way at the session level; for NULL/PLAIN it's a
// single READY command body. Reads and writes use a ZMTP codec capped at
// handshakeMaxFrame (see there), independent of the data-phase codec.
func (s *Session) runSecurityHandshake() error {
	var in []byte
	for {
		out, done, err := s.cfg.Mech.NextHandshake(in)
		// Mechanisms may return an encoded ERROR command alongside a
		// failure (e.g. ZAP rejection) so the peer learns the reason
		// instead of seeing an abrupt close; the writer sends it before
		// the conn closes.
		if len(out) > 0 {
			// Handshake frames are bounded by handshakeMaxFrame on both
			// sides, independent of the data cap: a small
			// WithMaxMessageSize must not break a CURVE INITIATE.
			frame, werr := handshakeCodec.AppendFrameHeader(nil, zmtp.FlagCommand, len(out))
			if werr != nil {
				return fmt.Errorf("gozmq/session: encode handshake frame: %w", werr)
			}
			s.hsOut <- append(frame, out...)
		}
		if err != nil {
			return fmt.Errorf("gozmq/session: security handshake: %w", err)
		}
		if done {
			return nil
		}

		// Read the peer's next handshake message.
		flags, body, rerr := handshakeCodec.ReadFrame(s.reader, nil, nil)
		if rerr != nil {
			return fmt.Errorf("gozmq/session: read handshake frame: %w", rerr)
		}
		if flags&zmtp.FlagCommand == 0 {
			return fmt.Errorf("%w: expected COMMAND frame in handshake, flags=0x%02x", zmqerr.ErrMalformedFrame, flags)
		}
		// Do NOT route through Mech.DecodeFrame here: that method is the
		// post-handshake MESSAGE-envelope codec. For CURVE it rejects
		// calls before stateDone; for NULL/PLAIN it's identity anyway.
		// Handshake command bodies are delivered raw to NextHandshake,
		// which owns any per-step cryptographic unwrapping.
		in = body
	}
}

func (s *Session) finalizePeerMetadata() error {
	props, err := peerMetadataFromMechanism(s.cfg.Mech)
	if err != nil {
		return err
	}
	if err := validatePeerIdentity(props); err != nil {
		return err
	}
	return validateSocketTypeCompatibility(s.cfg.Mech.Metadata(), props)
}

// validatePeerIdentity enforces ZMTP 3.1's rules on a peer-advertised
// Identity: at most 255 bytes and no leading zero byte (that range is
// reserved for identities the receiver synthesizes, so a peer that used it
// could collide with, and take over, another peer's ROUTER routing slot).
// libzmq refuses to set such a ZMQ_ROUTING_ID locally, so a conforming peer
// never sends one.
func validatePeerIdentity(props map[string]string) error {
	id, ok := props["Identity"]
	switch {
	case !ok || id == "":
		return nil
	case len(id) > 255:
		return fmt.Errorf("%w: peer Identity is %d bytes, max 255", zmqerr.ErrMalformedFrame, len(id))
	case id[0] == 0:
		return fmt.Errorf("%w: peer Identity starts with a zero byte", zmqerr.ErrMalformedFrame)
	}
	return nil
}

func peerMetadataFromMechanism(m security.Mech) (map[string]string, error) {
	props := m.PeerMetadata()
	if props == nil {
		return nil, fmt.Errorf("gozmq/session: mechanism %q finished handshake without peer metadata", m.Name())
	}
	return props, nil
}

func validateSocketTypeCompatibility(localMeta, peerMeta map[string]string) error {
	localType, ok := metadataValue(localMeta, "Socket-Type")
	if !ok {
		return nil
	}
	peerType, ok := metadataValue(peerMeta, "Socket-Type")
	if !ok {
		return nil
	}
	if localType == "STREAM" || peerType == "STREAM" {
		return fmt.Errorf("gozmq/session: STREAM sockets do not negotiate ZMTP sessions (local=%s peer=%s)", localType, peerType)
	}
	if !SocketTypesCompatible(localType, peerType) {
		return fmt.Errorf("gozmq/session: incompatible Socket-Type pairing local=%s peer=%s", localType, peerType)
	}
	return nil
}

func metadataValue(props map[string]string, name string) (string, bool) {
	for key, value := range props {
		if strings.EqualFold(key, name) {
			return strings.ToUpper(value), true
		}
	}
	return "", false
}

// SocketTypesCompatible reports whether two ZMTP Socket-Type values form a
// valid pairing per ZMTP 3.1. Inproc wiring, which has no handshake, uses it
// directly.
func SocketTypesCompatible(localType, peerType string) bool {
	switch localType {
	case "REQ":
		return peerType == "REP" || peerType == "ROUTER"
	case "REP":
		return peerType == "REQ" || peerType == "DEALER"
	case "DEALER":
		return peerType == "REP" || peerType == "DEALER" || peerType == "ROUTER"
	case "ROUTER":
		return peerType == "REQ" || peerType == "DEALER" || peerType == "ROUTER"
	case "PUB":
		return peerType == "SUB" || peerType == "XSUB"
	case "SUB":
		return peerType == "PUB" || peerType == "XPUB"
	case "XPUB":
		return peerType == "SUB" || peerType == "XSUB"
	case "XSUB":
		return peerType == "PUB" || peerType == "XPUB"
	case "PUSH":
		return peerType == "PULL"
	case "PULL":
		return peerType == "PUSH"
	case "PAIR":
		return peerType == "PAIR"
	default:
		return false
	}
}

// encodeGreeting builds the 64-byte ZMTP 3.1 greeting block.
func encodeGreeting(mech string, asServer bool) ([]byte, error) {
	if len(mech) > greetingMechLen {
		return nil, fmt.Errorf("gozmq/session: mechanism %q exceeds %d bytes: %w", mech, greetingMechLen, zmqerr.ErrMalformedFrame)
	}
	if i := strings.IndexFunc(mech, zmtp.NonPrintableASCII); i >= 0 {
		return nil, fmt.Errorf("gozmq/session: mechanism %q non-printable byte 0x%02x: %w", mech, mech[i], zmqerr.ErrMalformedFrame)
	}
	var buf [greetingSize]byte
	buf[0] = greetingSigPrefix
	buf[9] = greetingSigTerm
	buf[10] = greetingVersionMaj
	buf[11] = greetingVersionMin
	copy(buf[greetingMechOff:greetingMechOff+greetingMechLen], mech)
	if asServer {
		buf[greetingAsServerOff] = 1
	}
	return buf[:], nil
}

// decodeGreeting validates a 64-byte greeting block and returns the
// peer's mechanism, minor version and as-server flag.
func decodeGreeting(buf []byte) (mech string, minor byte, asServer bool, err error) {
	if len(buf) != greetingSize {
		return "", 0, false, fmt.Errorf("%w: greeting wrong size %d", zmqerr.ErrMalformedFrame, len(buf))
	}
	if buf[0] != greetingSigPrefix {
		return "", 0, false, fmt.Errorf("%w: bad signature prefix 0x%02x", zmqerr.ErrMalformedFrame, buf[0])
	}
	if buf[9] != greetingSigTerm {
		return "", 0, false, fmt.Errorf("%w: bad signature terminator 0x%02x", zmqerr.ErrMalformedFrame, buf[9])
	}
	major := buf[10]
	minor = buf[11]
	if major != greetingVersionMaj {
		return "", 0, false, fmt.Errorf("%w: unsupported major 0x%02x", zmqerr.ErrProtocolVersion, major)
	}
	// Any 3.x minor is acceptable: ZMTP guarantees backwards-compatible
	// minors, so we treat minor >= 1 as 3.1 (heartbeats) and never
	// reject a future 3.x peer.
	asServerByte := buf[greetingAsServerOff]
	if asServerByte != 0 && asServerByte != 1 {
		return "", 0, false, fmt.Errorf("%w: bad as-server byte 0x%02x", zmqerr.ErrMalformedFrame, asServerByte)
	}
	// Mechanism: NUL-terminated, NUL-padded, printable ASCII.
	raw := buf[greetingMechOff : greetingMechOff+greetingMechLen]
	end := bytes.IndexByte(raw, 0)
	if end < 0 {
		end = len(raw)
	}
	for i := end; i < len(raw); i++ {
		if raw[i] != 0 {
			return "", 0, false, fmt.Errorf("%w: mechanism padding non-zero at %d", zmqerr.ErrMalformedFrame, i)
		}
	}
	if i := bytes.IndexFunc(raw[:end], zmtp.NonPrintableASCII); i >= 0 {
		return "", 0, false, fmt.Errorf("%w: mechanism non-printable 0x%02x at %d", zmqerr.ErrMalformedFrame, raw[i], i)
	}
	return string(raw[:end]), minor, asServerByte == 1, nil
}
