package session

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// Greeting layout constants. Duplicated from zmtp/greeting.go on purpose:
// the session package is forbidden by the Wave-1 plan from importing zmtp
// (so A4 can land in parallel with A1). The byte layout is fixed by ZMTP
// 3.1 §2.1 and will not drift.
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

// doHandshake drives greeting + security handshake under HandshakeTimeout.
// On any failure it returns an error wrapping zmqerr.ErrHandshakeTimeout
// (for timeouts) or the underlying cause; callers stash the error and
// close handshaked.
func (s *sessionImpl) doHandshake() error {
	// Apply the deadline if configured. We clear the deadline once the
	// handshake completes so traffic-phase reads/writes block forever
	// (the heartbeat path manages its own timeout).
	if s.cfg.HandshakeTimeout > 0 {
		deadline := time.Now().Add(s.cfg.HandshakeTimeout)
		if err := s.cfg.Conn.SetDeadline(deadline); err != nil {
			return fmt.Errorf("gozmq/session: set handshake deadline: %w", err)
		}
		defer func() {
			// Reset to no-deadline. If this fails the caller is about
			// to bail anyway; ignore.
			_ = s.cfg.Conn.SetDeadline(time.Time{})
		}()
	}

	if err := s.exchangeGreeting(); err != nil {
		return s.classifyHandshakeErr(err)
	}

	if err := s.runSecurityHandshake(); err != nil {
		return s.classifyHandshakeErr(err)
	}

	if err := s.finalizePeerMetadata(); err != nil {
		return s.classifyHandshakeErr(err)
	}

	return nil
}

// classifyHandshakeErr promotes timeouts to ErrHandshakeTimeout while
// preserving the underlying cause for diagnostics.
func (s *sessionImpl) classifyHandshakeErr(err error) error {
	if err == nil {
		return nil
	}
	// net.Error timeouts (the SetDeadline path) report Timeout()=true.
	type timeouter interface{ Timeout() bool }
	var to timeouter
	if errors.As(err, &to) && to.Timeout() {
		return fmt.Errorf("%w: %w", zmqerr.ErrHandshakeTimeout, err)
	}
	return err
}

// exchangeGreeting writes our greeting, reads the peer's, validates it,
// and records peer-side capabilities (heartbeat support, mechanism name
// match) on the sessionImpl.
//
// The write happens concurrently with the read because some transports
// (notably synchronous in-memory ones used in tests, and in principle
// any transport whose send buffer is smaller than 64 bytes) would
// otherwise deadlock with both peers blocked in Write before either
// reaches Read. Real net.Conn implementations have OS buffers that
// absorb 64 bytes trivially, so the concurrency is harmless there.
func (s *sessionImpl) exchangeGreeting() error {
	out, err := encodeGreeting(s.cfg.Mech.Name(), s.cfg.Mech.AsServer())
	if err != nil {
		return err
	}
	writeErrCh := make(chan error, 1)
	go func() {
		_, werr := s.cfg.Conn.Write(out)
		writeErrCh <- werr
	}()

	var buf [greetingSize]byte
	if _, err := io.ReadFull(s.reader, buf[:]); err != nil {
		// Drain the write goroutine before returning so we don't leak
		// it on a read failure.
		<-writeErrCh
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("%w: greeting truncated: %w", zmqerr.ErrMalformedFrame, err)
		}
		return fmt.Errorf("gozmq/session: read greeting: %w", err)
	}
	if werr := <-writeErrCh; werr != nil {
		return fmt.Errorf("gozmq/session: write greeting: %w", werr)
	}

	mech, peerMinor, err := decodeGreeting(buf[:])
	if err != nil {
		return err
	}
	// We do not require strict mechanism agreement here — the security
	// layer's NextHandshake will fail naturally if the peer chose a
	// different mechanism. We only assert that the peer also speaks
	// ZMTP 3.x (decode would have rejected anything else) and record
	// the heartbeat capability. design §5.4: skip PING/PONG if peer is
	// 3.0.
	if peerMinor >= 0x01 {
		s.peerHeartbeats.Store(true)
	}
	// Mechanism mismatch is informational; the actual negotiation rule
	// is decided by NextHandshake. We log if there's a mismatch so it's
	// observable in tests.
	if mech != s.cfg.Mech.Name() {
		s.cfg.Logger.Debug(
			"gozmq/session: peer mechanism differs from ours",
			"peer", mech, "ours", s.cfg.Mech.Name(),
		)
	}
	return nil
}

// runSecurityHandshake drives the lock-step NextHandshake loop. Each
// iteration: hand the previous inbound to the mechanism, write its
// returned outbound (if any), then read the peer's next message (unless
// the mechanism declared the handshake done).
//
// To avoid Write-vs-Write deadlock on synchronous transports (where
// both peers are about to write before either is reading) we issue the
// outbound write in a goroutine and overlap it with the inbound read.
// Real net.Conn implementations don't need this, but it costs nothing
// and removes a fragile assumption from the design.
//
// "Message" here is whatever NextHandshake returns — it's not framed
// in any particular way at the session level; for NULL/PLAIN it's a
// single READY command body. We use the FrameCodec for the wire form
// so the codec sees every command exchange.
func (s *sessionImpl) runSecurityHandshake() error {
	var in []byte
	for {
		out, done, err := s.cfg.Mech.NextHandshake(in)
		if err != nil {
			return fmt.Errorf("gozmq/session: security handshake: %w", err)
		}

		// Kick off the outbound write (if any) in the background so we
		// can begin reading the peer's response immediately. For the
		// final step (done=true) we still need to wait for the write
		// to drain before returning — otherwise the writer goroutine
		// might race traffic-phase frames in front of our trailing
		// handshake bytes.
		writeErrCh := make(chan error, 1)
		if len(out) > 0 {
			go func() {
				werr := s.cfg.Codec.WriteFrame(s.cfg.Conn, iface.FlagCommand, out)
				writeErrCh <- werr
			}()
		} else {
			writeErrCh <- nil
		}

		if done {
			if werr := <-writeErrCh; werr != nil {
				return fmt.Errorf("gozmq/session: write handshake frame: %w", werr)
			}
			return nil
		}

		// Read the peer's next handshake message.
		flags, body, rerr := s.cfg.Codec.ReadFrame(s.reader, nil)
		// Always reap the writer before propagating either error.
		werr := <-writeErrCh
		if werr != nil {
			return fmt.Errorf("gozmq/session: write handshake frame: %w", werr)
		}
		if rerr != nil {
			return fmt.Errorf("gozmq/session: read handshake frame: %w", rerr)
		}
		if flags&iface.FlagCommand == 0 {
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

func (s *sessionImpl) finalizePeerMetadata() error {
	props, err := peerMetadataFromMechanism(s.cfg.Mech)
	if err != nil {
		return err
	}
	if err := s.cfg.Mech.SetPeerMetadata(props); err != nil {
		return fmt.Errorf("gozmq/session: set peer metadata: %w", err)
	}
	if err := validateSocketTypeCompatibility(s.cfg.Mech.Metadata(), props); err != nil {
		return err
	}
	return nil
}

func peerMetadataFromMechanism(m iface.SecurityMech) (map[string]string, error) {
	type peerMetaReader interface {
		PeerMetadata() map[string]string
	}
	pm, ok := m.(peerMetaReader)
	if !ok {
		return nil, fmt.Errorf("gozmq/session: mechanism %q does not expose peer metadata", m.Name())
	}
	props := pm.PeerMetadata()
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
	if !socketTypesCompatible(localType, peerType) {
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

func socketTypesCompatible(localType, peerType string) bool {
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
	for i := 0; i < len(mech); i++ {
		c := mech[i]
		if c < 0x20 || c > 0x7E {
			return nil, fmt.Errorf("gozmq/session: mechanism %q non-printable byte 0x%02x: %w", mech, c, zmqerr.ErrMalformedFrame)
		}
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
// peer's mechanism and minor version.
func decodeGreeting(buf []byte) (mech string, minor byte, err error) {
	if len(buf) != greetingSize {
		return "", 0, fmt.Errorf("%w: greeting wrong size %d", zmqerr.ErrMalformedFrame, len(buf))
	}
	if buf[0] != greetingSigPrefix {
		return "", 0, fmt.Errorf("%w: bad signature prefix 0x%02x", zmqerr.ErrMalformedFrame, buf[0])
	}
	if buf[9] != greetingSigTerm {
		return "", 0, fmt.Errorf("%w: bad signature terminator 0x%02x", zmqerr.ErrMalformedFrame, buf[9])
	}
	major := buf[10]
	minor = buf[11]
	if major != greetingVersionMaj {
		return "", 0, fmt.Errorf("%w: unsupported major 0x%02x", zmqerr.ErrProtocolVersion, major)
	}
	if minor != 0x00 && minor != 0x01 {
		return "", 0, fmt.Errorf("%w: unsupported minor 0x%02x", zmqerr.ErrProtocolVersion, minor)
	}
	asServer := buf[greetingAsServerOff]
	if asServer != 0 && asServer != 1 {
		return "", 0, fmt.Errorf("%w: bad as-server byte 0x%02x", zmqerr.ErrMalformedFrame, asServer)
	}
	// Mechanism: NUL-terminated, NUL-padded, printable ASCII.
	raw := buf[greetingMechOff : greetingMechOff+greetingMechLen]
	end := len(raw)
	for i, b := range raw {
		if b == 0 {
			end = i
			break
		}
	}
	for i := end; i < len(raw); i++ {
		if raw[i] != 0 {
			return "", 0, fmt.Errorf("%w: mechanism padding non-zero at %d", zmqerr.ErrMalformedFrame, i)
		}
	}
	for i := 0; i < end; i++ {
		c := raw[i]
		if c < 0x20 || c > 0x7E {
			return "", 0, fmt.Errorf("%w: mechanism non-printable 0x%02x at %d", zmqerr.ErrMalformedFrame, c, i)
		}
	}
	return string(raw[:end]), minor, nil
}
