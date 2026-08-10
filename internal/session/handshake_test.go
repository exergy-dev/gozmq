package session

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
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
