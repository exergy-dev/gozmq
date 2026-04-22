package security

import (
	"errors"
	"testing"

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

	if client.AsServer() {
		t.Fatal("client.AsServer = true, want false")
	}
	if !server.AsServer() {
		t.Fatal("server.AsServer = false, want true")
	}
	if client.Name() != "NULL" || server.Name() != "NULL" {
		t.Fatalf("Name: client=%q server=%q", client.Name(), server.Name())
	}

	// Step 1: the client emits READY; the server is passive on seed so it can
	// authorize the peer before sending its own READY.
	clientOut, clientDone, err := client.NextHandshake(nil)
	if err != nil {
		t.Fatalf("client seed: %v", err)
	}
	if clientDone {
		t.Fatal("client seed: done=true, want false")
	}
	if len(clientOut) == 0 {
		t.Fatal("client seed: empty out")
	}

	serverOut, serverDone, err := server.NextHandshake(nil)
	if err != nil {
		t.Fatalf("server seed: %v", err)
	}
	if serverDone {
		t.Fatal("server seed: done=true, want false")
	}
	if len(serverOut) != 0 {
		t.Fatalf("server seed: out=%x, want empty", serverOut)
	}

	// Sanity-check the client's READY command shape: [5]"READY"[meta...].
	if len(clientOut) < 6 {
		t.Fatalf("READY body too short: %d bytes", len(clientOut))
	}
	if clientOut[0] != 5 {
		t.Fatalf("READY name-length byte: got %d, want 5", clientOut[0])
	}
	if string(clientOut[1:6]) != "READY" {
		t.Fatalf("READY name: got %q, want %q", clientOut[1:6], "READY")
	}

	// Step 2: the server ingests the client's READY and emits its own READY.
	serverOut, serverDone, err = server.NextHandshake(clientOut)
	if err != nil {
		t.Fatalf("server ingest: %v", err)
	}
	if !serverDone {
		t.Fatal("server ingest: done=false, want true")
	}
	if len(serverOut) == 0 {
		t.Fatal("server ingest: empty READY out")
	}
	if serverOut[0] != 5 || string(serverOut[1:6]) != "READY" {
		t.Fatalf("server READY malformed: %x", serverOut)
	}

	// Step 3: client ingests the server's READY body.
	_, clientDone, err = client.NextHandshake(serverOut)
	if err != nil {
		t.Fatalf("client ingest: %v", err)
	}
	if !clientDone {
		t.Fatal("client ingest: done=false, want true")
	}

	// Peer metadata round-tripped through the handshake. We cast to the
	// concrete type to access PeerMetadata; the public iface uses
	// SetPeerMetadata (session-layer side of the contract), so tests peek
	// at what the mechanism stored.
	clientImpl := client.(*nullMech)
	serverImpl := server.(*nullMech)

	if !mapsEqual(clientImpl.PeerMetadata(), serverMeta) {
		t.Fatalf("client peer-metadata: got %v, want %v", clientImpl.PeerMetadata(), serverMeta)
	}
	if !mapsEqual(serverImpl.PeerMetadata(), clientMeta) {
		t.Fatalf("server peer-metadata: got %v, want %v", serverImpl.PeerMetadata(), clientMeta)
	}

	// Metadata() returns a copy of the local properties.
	if !mapsEqual(client.Metadata(), clientMeta) {
		t.Fatalf("client local-metadata: got %v, want %v", client.Metadata(), clientMeta)
	}
	if !mapsEqual(server.Metadata(), serverMeta) {
		t.Fatalf("server local-metadata: got %v, want %v", server.Metadata(), serverMeta)
	}
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
			if err != nil {
				t.Fatalf("EncodeFrame(%d bytes): %v", len(input), err)
			}
			if wireFlags != tc.flags {
				t.Fatalf("EncodeFrame flags: got %#x, want %#x", wireFlags, tc.flags)
			}
			if len(enc) != len(input) {
				t.Fatalf("EncodeFrame: length changed %d -> %d", len(input), len(enc))
			}
			plainFlags, dec, err := m.DecodeFrame(wireFlags, enc)
			if err != nil {
				t.Fatalf("DecodeFrame(%d bytes): %v", len(enc), err)
			}
			if plainFlags != tc.flags {
				t.Fatalf("DecodeFrame flags: got %#x, want %#x", plainFlags, tc.flags)
			}
			if len(dec) != len(input) {
				t.Fatalf("DecodeFrame: length changed %d -> %d", len(input), len(dec))
			}
			for i := range input {
				if dec[i] != input[i] {
					t.Fatalf("DecodeFrame: byte %d diverged", i)
				}
			}
		}
	}
}

// TestNullSetPeerMetadataStores checks SetPeerMetadata is a pure store
// (NULL does no validation) and that nil clears the state.
func TestNullSetPeerMetadataStores(t *testing.T) {
	m := NewNull(iface.RoleServer, nil).(*nullMech)
	props := map[string]string{"Socket-Type": "PAIR"}
	if err := m.SetPeerMetadata(props); err != nil {
		t.Fatalf("SetPeerMetadata: %v", err)
	}
	if !mapsEqual(m.PeerMetadata(), props) {
		t.Fatalf("PeerMetadata: got %v, want %v", m.PeerMetadata(), props)
	}

	// Mutating the caller's map must not affect the stored copy.
	props["Socket-Type"] = "PUB"
	if m.PeerMetadata()["Socket-Type"] != "PAIR" {
		t.Fatal("PeerMetadata not isolated from caller mutations")
	}

	if err := m.SetPeerMetadata(nil); err != nil {
		t.Fatalf("SetPeerMetadata(nil): %v", err)
	}
	if m.PeerMetadata() != nil {
		t.Fatalf("PeerMetadata after nil-set: got %v, want nil", m.PeerMetadata())
	}
}

// TestNullNextHandshakeRejectsMalformed covers the defensive error paths in
// NextHandshake: non-nil input on the seed step, and a malformed peer READY
// on the ingest step.
func TestNullNextHandshakeRejectsMalformed(t *testing.T) {
	t.Run("non-nil-on-seed", func(t *testing.T) {
		m := NewNull(iface.RoleClient, nil)
		_, _, err := m.NextHandshake([]byte{0x00})
		if err == nil {
			t.Fatal("seed with non-nil input: want error")
		}
		if !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("want ErrMalformedFrame, got %v", err)
		}
	})

	t.Run("nil-on-ingest", func(t *testing.T) {
		m := NewNull(iface.RoleClient, nil)
		_, _, err := m.NextHandshake(nil)
		if err != nil {
			t.Fatalf("seed: %v", err)
		}
		_, _, err = m.NextHandshake(nil)
		if err == nil {
			t.Fatal("ingest with nil input: want error")
		}
		if !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("want ErrMalformedFrame, got %v", err)
		}
	})

	t.Run("wrong-command-name", func(t *testing.T) {
		m := NewNull(iface.RoleClient, nil)
		_, _, err := m.NextHandshake(nil)
		if err != nil {
			t.Fatalf("seed: %v", err)
		}
		// Fabricate a command that is not READY.
		body := []byte{0x05, 'E', 'R', 'R', 'O', 'R'}
		_, _, err = m.NextHandshake(body)
		if err == nil {
			t.Fatal("ingest of ERROR command: want error")
		}
		if !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("want ErrMalformedFrame, got %v", err)
		}
	})

	t.Run("truncated-name", func(t *testing.T) {
		m := NewNull(iface.RoleClient, nil)
		_, _, err := m.NextHandshake(nil)
		if err != nil {
			t.Fatalf("seed: %v", err)
		}
		// Declares a 5-byte name but only supplies 2 bytes.
		body := []byte{0x05, 'R', 'E'}
		_, _, err = m.NextHandshake(body)
		if err == nil {
			t.Fatal("ingest truncated: want error")
		}
		if !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("want ErrMalformedFrame, got %v", err)
		}
	})

	t.Run("empty-body", func(t *testing.T) {
		m := NewNull(iface.RoleClient, nil)
		_, _, err := m.NextHandshake(nil)
		if err != nil {
			t.Fatalf("seed: %v", err)
		}
		_, _, err = m.NextHandshake([]byte{})
		if err == nil {
			t.Fatal("ingest empty body: want error")
		}
		if !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("want ErrMalformedFrame, got %v", err)
		}
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
	if got["Socket-Type"] != "REQ" {
		t.Fatalf("Metadata not isolated from caller mutation: got %q, want %q", got["Socket-Type"], "REQ")
	}

	// Mutating the Metadata() result must not affect subsequent calls.
	got["Socket-Type"] = "PUB"
	again := m.Metadata()
	if again["Socket-Type"] != "REQ" {
		t.Fatalf("Metadata not isolated from returned-map mutation: got %q, want %q", again["Socket-Type"], "REQ")
	}
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}
