package security

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// TestNullHandshakeEndToEnd drives two NULL mechanisms — one server, one
// client — through their handshake by pumping bytes between them until both
// report done. Then it asserts that each side received the other's metadata.
func TestNullHandshakeEndToEnd(t *testing.T) {
	clientMeta := map[string]string{
		"Socket-Type": "DEALER",
		"Identity":    "client-1",
	}
	serverMeta := map[string]string{
		"Socket-Type": "ROUTER",
		"Identity":    "server-1",
		"Resource":    "/api",
	}

	client := NewNull(iface.RoleClient, clientMeta)
	server := NewNull(iface.RoleServer, serverMeta)

	require.False(t, client.AsServer(), "client.AsServer should be false")
	require.True(t, server.AsServer(), "server.AsServer should be true")
	require.Equal(t, "NULL", client.Name())
	require.Equal(t, "NULL", server.Name())

	// Step 1: the client emits READY; the server is passive on seed so it can
	// authorize the peer before sending its own READY.
	clientOut, clientDone, err := client.NextHandshake(nil)
	require.NoError(t, err, "client seed")
	require.False(t, clientDone, "client seed: done should be false")
	require.NotEmpty(t, clientOut, "client seed: out should not be empty")

	serverOut, serverDone, err := server.NextHandshake(nil)
	require.NoError(t, err, "server seed")
	require.False(t, serverDone, "server seed: done should be false")
	require.Empty(t, serverOut, "server seed: out should be empty")

	// Sanity-check the client's READY command shape: [5]"READY"[meta...].
	require.GreaterOrEqual(t, len(clientOut), 6, "READY body too short")
	require.Equal(t, byte(5), clientOut[0], "READY name-length byte")
	require.Equal(t, "READY", string(clientOut[1:6]), "READY name")

	// Step 2: the server ingests the client's READY and emits its own READY.
	serverOut, serverDone, err = server.NextHandshake(clientOut)
	require.NoError(t, err, "server ingest")
	require.True(t, serverDone, "server ingest: done should be true")
	require.NotEmpty(t, serverOut, "server ingest: READY out should not be empty")
	require.Equal(t, byte(5), serverOut[0], "server READY name-length byte")
	require.Equal(t, "READY", string(serverOut[1:6]), "server READY name")

	// Step 3: client ingests the server's READY body.
	_, clientDone, err = client.NextHandshake(serverOut)
	require.NoError(t, err, "client ingest")
	require.True(t, clientDone, "client ingest: done should be true")

	// Peer metadata round-tripped through the handshake. We cast to the
	// concrete type to access PeerMetadata; the public iface uses
	// SetPeerMetadata (session-layer side of the contract), so tests peek
	// at what the mechanism stored.
	clientImpl := client.(*nullMech)
	serverImpl := server.(*nullMech)

	require.Equal(t, serverMeta, clientImpl.PeerMetadata(), "client peer-metadata")
	require.Equal(t, clientMeta, serverImpl.PeerMetadata(), "server peer-metadata")

	// Metadata() returns a copy of the local properties.
	require.Equal(t, clientMeta, client.Metadata(), "client local-metadata")
	require.Equal(t, serverMeta, server.Metadata(), "server local-metadata")
}

// TestNullEncodeDecodeFrameIdentity verifies NULL applies no per-frame
// transformation — plaintext and flags are preserved exactly.
func TestNullEncodeDecodeFrameIdentity(t *testing.T) {
	m := NewNull(iface.RoleClient, nil)
	for _, tc := range []struct {
		flags uint8
		input [][]byte
	}{
		{flags: 0, input: [][]byte{nil, {}, {0x00}, []byte("hello, zmq"), make([]byte, 4096)}},
		{flags: iface.FlagMore | iface.FlagCommand, input: [][]byte{[]byte("flagged")}},
	} {
		for _, input := range tc.input {
			wireFlags, enc, err := m.EncodeFrame(tc.flags, input)
			require.NoError(t, err, "EncodeFrame(%d bytes)", len(input))
			require.Equal(t, tc.flags, wireFlags, "EncodeFrame flags")
			require.Equal(t, len(input), len(enc), "EncodeFrame length")
			plainFlags, dec, err := m.DecodeFrame(wireFlags, enc)
			require.NoError(t, err, "DecodeFrame(%d bytes)", len(enc))
			require.Equal(t, tc.flags, plainFlags, "DecodeFrame flags")
			require.Equal(t, input, dec, "DecodeFrame output")
		}
	}
}

// TestNullSetPeerMetadataStores checks SetPeerMetadata is a pure store
// (NULL does no validation) and that nil clears the state.
func TestNullSetPeerMetadataStores(t *testing.T) {
	m := NewNull(iface.RoleServer, nil).(*nullMech)
	props := map[string]string{"Socket-Type": "PAIR"}
	require.NoError(t, m.SetPeerMetadata(props))
	require.Equal(t, props, m.PeerMetadata())

	// Mutating the caller's map must not affect the stored copy.
	props["Socket-Type"] = "PUB"
	require.Equal(t, "PAIR", m.PeerMetadata()["Socket-Type"], "PeerMetadata not isolated from caller mutations")

	require.NoError(t, m.SetPeerMetadata(nil))
	require.Nil(t, m.PeerMetadata(), "PeerMetadata after nil-set should be nil")
}

// TestNullNextHandshakeRejectsMalformed covers the defensive error paths in
// NextHandshake: non-nil input on the seed step, and a malformed peer READY
// on the ingest step.
func TestNullNextHandshakeRejectsMalformed(t *testing.T) {
	t.Run("non-nil-on-seed", func(t *testing.T) {
		m := NewNull(iface.RoleClient, nil)
		_, _, err := m.NextHandshake([]byte{0x00})
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})

	t.Run("nil-on-ingest", func(t *testing.T) {
		m := NewNull(iface.RoleClient, nil)
		_, _, err := m.NextHandshake(nil)
		require.NoError(t, err, "seed")
		_, _, err = m.NextHandshake(nil)
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})

	t.Run("wrong-command-name", func(t *testing.T) {
		m := NewNull(iface.RoleClient, nil)
		_, _, err := m.NextHandshake(nil)
		require.NoError(t, err, "seed")
		// Fabricate a command that is not READY.
		body := []byte{0x05, 'E', 'R', 'R', 'O', 'R'}
		_, _, err = m.NextHandshake(body)
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})

	t.Run("truncated-name", func(t *testing.T) {
		m := NewNull(iface.RoleClient, nil)
		_, _, err := m.NextHandshake(nil)
		require.NoError(t, err, "seed")
		// Declares a 5-byte name but only supplies 2 bytes.
		body := []byte{0x05, 'R', 'E'}
		_, _, err = m.NextHandshake(body)
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})

	t.Run("empty-body", func(t *testing.T) {
		m := NewNull(iface.RoleClient, nil)
		_, _, err := m.NextHandshake(nil)
		require.NoError(t, err, "seed")
		_, _, err = m.NextHandshake([]byte{})
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})
}

// TestNullMetadataIsolated confirms Metadata returns a copy so external
// mutation can't leak into what the mechanism sends on the wire.
func TestNullMetadataIsolated(t *testing.T) {
	local := map[string]string{"Socket-Type": "REQ"}
	m := NewNull(iface.RoleClient, local)

	// Caller mutates the map they passed in — mechanism must be unaffected.
	local["Socket-Type"] = "REP"

	got := m.Metadata()
	require.Equal(t, "REQ", got["Socket-Type"], "Metadata not isolated from caller mutation")

	// Mutating the Metadata() result must not affect subsequent calls.
	got["Socket-Type"] = "PUB"
	again := m.Metadata()
	require.Equal(t, "REQ", again["Socket-Type"], "Metadata not isolated from returned-map mutation")
}