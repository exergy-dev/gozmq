package session

import (
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
)

// TestDecodeGreeting_MinorVersions: any 3.x minor must be accepted —
// ZMTP guarantees backwards-compatible minors — while a non-3 major is
// still rejected. The session treats minor >= 1 as 3.1 (heartbeats).
func TestDecodeGreeting_MinorVersions(t *testing.T) {
	for _, minor := range []byte{0x00, 0x01, 0x02, 0x7F, 0xFF} {
		g, err := encodeGreeting("NULL", false)
		require.NoError(t, err, "encodeGreeting")
		g[11] = minor

		mech, gotMinor, err := decodeGreeting(g)
		require.NoError(t, err, "decodeGreeting(minor=0x%02x)", minor)
		require.Equal(t, "NULL", mech, "mechanism (minor=0x%02x)", minor)
		require.Equal(t, minor, gotMinor, "minor")
	}
}

func TestDecodeGreeting_RejectsWrongMajor(t *testing.T) {
	g, err := encodeGreeting("NULL", false)
	require.NoError(t, err, "encodeGreeting")
	g[10] = 0x02 // ZMTP 2.x peer

	_, _, err = decodeGreeting(g)
	require.ErrorIs(t, err, zmqerr.ErrProtocolVersion, "decodeGreeting(major=0x02)")
}

// TestHandshake_RejectsMechanismMismatch: RFC 23 requires both peers to
// announce the same mechanism in the greeting. A mismatch must fail the
// handshake on both sides instead of being logged and left to the
// mechanism's parser.
func TestHandshake_RejectsMechanismMismatch(t *testing.T) {
	connA, connB := newFakeConnPair()
	sA := New(Config{Conn: connA, Mech: newFakeMech("NULL", false, nil), Codec: fakeCodec{}, SendHWM: 1, RecvHWM: 1})
	sB := New(Config{Conn: connB, Mech: newFakeMech("PLAIN", true, nil), Codec: fakeCodec{}, SendHWM: 1, RecvHWM: 1})
	sA.Start()
	sB.Start()
	defer func() {
		_ = sA.Stop()
		_ = sB.Stop()
	}()
	for label, s := range map[string]iface.Session{"A": sA, "B": sB} {
		waitClosed(t, s.Handshaked(), 2*time.Second, label+": handshake did not finish")
		err := s.HandshakeErr()
		require.Error(t, err, label)
		require.ErrorContains(t, err, "mechanism mismatch", label)
		require.NotErrorIs(t, err, zmqerr.ErrAuthFailed, label)
		require.Nil(t, s.Pipe(), label)
	}
	require.Zero(t, sA.(*sessionImpl).cfg.Mech.(*fakeMech).setPeerCalls, "A reached the security handshake")
}

// TestHandshake_BoundsPreAuthFrameSize: a handshake frame announcing more
// than handshakeMaxFrame bytes is rejected from its header alone, before
// any body is buffered, whatever MaxMessageSize the session carries.
func TestHandshake_BoundsPreAuthFrameSize(t *testing.T) {
	connA, connB := newFakeConnPair()
	defer func() { _ = connB.Close() }()
	s := New(Config{
		Conn:           connA,
		Mech:           newFakeMech("NULL", true, nil),
		Codec:          zmtp.NewFrameCodec(64 << 20),
		MaxMessageSize: 64 << 20,
		SendHWM:        1,
		RecvHWM:        1,
	})
	s.Start()
	defer func() { _ = s.Stop() }()

	go func() { _, _ = io.Copy(io.Discard, connB) }()
	g, err := encodeGreeting("NULL", false)
	require.NoError(t, err)
	hdr := make([]byte, 9)
	hdr[0] = iface.FlagCommand | zmtp.FlagLong
	binary.BigEndian.PutUint64(hdr[1:], handshakeMaxFrame+1)
	_, err = connB.Write(append(g, hdr...))
	require.NoError(t, err)

	waitClosed(t, s.Handshaked(), 2*time.Second, "oversized handshake frame was not rejected")
	require.ErrorIs(t, s.HandshakeErr(), zmqerr.ErrMalformedFrame)
}
