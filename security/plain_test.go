package security

import (
	"errors"
	"testing"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// pumpPlain drives two PLAIN mechanisms against each other until both report
// done or both have errored. Messages flow lock-step: side A emits, side B
// ingests, side B emits, side A ingests, repeating. The first (seed) call is
// on the CLIENT — PLAIN starts client-first per RFC 24 §4.
//
// On NextHandshake errors the pump still delivers any non-nil `out` bytes the
// erroring side produced to its peer, because in the real session layer the
// writer emits the failure-path ERROR command before the session tears down,
// and the peer must observe it to surface its own ErrAuthFailed. The pump
// returns the pair (clientErr, serverErr); either may be nil on success.
func pumpPlain(t *testing.T, client, server iface.SecurityMech) (clientErr, serverErr error) {
	t.Helper()
	const maxSteps = 16

	var toServer, toClient []byte
	var clientDone, serverDone bool
	var cErr, sErr error

	// Seed both sides per the session-driver contract (every mechanism's
	// first NextHandshake call is with nil input). For PLAIN the server
	// is passive on seed; the client emits HELLO.
	if _, _, err := server.NextHandshake(nil); err != nil {
		return nil, err
	}
	out, done, err := client.NextHandshake(nil)
	if err != nil {
		return err, nil
	}
	clientDone = done
	toServer = out

	for step := 0; step < maxSteps; step++ {
		// Server ingests client's output (if any) and may emit output even
		// when returning an error (the ERROR command on auth failure).
		if sErr == nil && (len(toServer) > 0 || !serverDone) {
			out, done, err := server.NextHandshake(toServer)
			serverDone = done
			toClient = out
			toServer = nil
			if err != nil {
				sErr = err
			}
		}

		// Client ingests server's output.
		if cErr == nil && (len(toClient) > 0 || !clientDone) {
			out, done, err := client.NextHandshake(toClient)
			clientDone = done
			toServer = out
			toClient = nil
			if err != nil {
				cErr = err
			}
		}

		if (clientDone || cErr != nil) && (serverDone || sErr != nil) {
			return cErr, sErr
		}
	}
	t.Fatalf("pumpPlain: exceeded %d steps (client done=%v err=%v, server done=%v err=%v)", maxSteps, clientDone, cErr, serverDone, sErr)
	return cErr, sErr
}

func TestPlainHandshakeSuccess(t *testing.T) {
	clientMeta := map[string]string{
		"Socket-Type": "DEALER",
		"Identity":    "client-1",
	}
	serverMeta := map[string]string{
		"Socket-Type": "ROUTER",
		"Identity":    "server-1",
		"Resource":    "/api",
	}

	authenticate := func(user, pass string) bool {
		return user == "alice" && pass == "hunter2"
	}

	client := NewPlainClient("alice", "hunter2", clientMeta)
	server := NewPlainServer(authenticate, serverMeta)

	if client.AsServer() {
		t.Fatal("client.AsServer = true, want false")
	}
	if !server.AsServer() {
		t.Fatal("server.AsServer = false, want true")
	}
	if client.Name() != "PLAIN" || server.Name() != "PLAIN" {
		t.Fatalf("Name: client=%q server=%q", client.Name(), server.Name())
	}

	cerr, serr := pumpPlain(t, client, server)
	if cerr != nil || serr != nil {
		t.Fatalf("handshake failed: client=%v server=%v", cerr, serr)
	}

	clientImpl := client.(*plainMech)
	serverImpl := server.(*plainMech)

	if !mapsEqual(clientImpl.PeerMetadata(), serverMeta) {
		t.Fatalf("client peer metadata: got %v, want %v", clientImpl.PeerMetadata(), serverMeta)
	}
	if !mapsEqual(serverImpl.PeerMetadata(), clientMeta) {
		t.Fatalf("server peer metadata: got %v, want %v", serverImpl.PeerMetadata(), clientMeta)
	}

	// Both sides must have sent and received READY.
	if !clientImpl.readySent || !clientImpl.readyRecv {
		t.Fatalf("client ready flags: sent=%v recv=%v", clientImpl.readySent, clientImpl.readyRecv)
	}
	if !serverImpl.readySent || !serverImpl.readyRecv {
		t.Fatalf("server ready flags: sent=%v recv=%v", serverImpl.readySent, serverImpl.readyRecv)
	}
}

func TestPlainHandshakeWrongPassword(t *testing.T) {
	authenticate := func(user, pass string) bool {
		return user == "alice" && pass == "hunter2"
	}
	client := NewPlainClient("alice", "WRONG", nil)
	server := NewPlainServer(authenticate, nil)

	cerr, serr := pumpPlain(t, client, server)

	// Both sides eventually surface ErrAuthFailed: the server rejects HELLO
	// and emits ERROR; the client, upon receiving ERROR, returns
	// ErrAuthFailed from its own NextHandshake call.
	if serr == nil {
		t.Fatal("server: expected error, got nil")
	}
	if !errors.Is(serr, zmqerr.ErrAuthFailed) {
		t.Fatalf("server error: want ErrAuthFailed, got %v", serr)
	}
	if cerr == nil {
		t.Fatal("client: expected error, got nil")
	}
	if !errors.Is(cerr, zmqerr.ErrAuthFailed) {
		t.Fatalf("client error: want ErrAuthFailed, got %v", cerr)
	}
}

func TestPlainHandshakeNilAuthCallback(t *testing.T) {
	// A nil authenticate callback rejects everything.
	client := NewPlainClient("alice", "hunter2", nil)
	server := NewPlainServer(nil, nil)

	cerr, serr := pumpPlain(t, client, server)
	if !errors.Is(serr, zmqerr.ErrAuthFailed) {
		t.Fatalf("server error: want ErrAuthFailed, got %v", serr)
	}
	if !errors.Is(cerr, zmqerr.ErrAuthFailed) {
		t.Fatalf("client error: want ErrAuthFailed, got %v", cerr)
	}
}

func TestPlainMalformedHello(t *testing.T) {
	// Feed a malformed HELLO directly to a server mechanism and check the
	// error surfaces as ErrMalformedFrame.
	cases := []struct {
		name string
		body []byte
	}{
		{
			name: "empty-body",
			body: []byte{},
		},
		{
			name: "name-truncated",
			// Declares a 5-byte name but supplies none.
			body: []byte{0x05},
		},
		{
			name: "wrong-name",
			// Declares a well-formed "GOODBYE" command instead of HELLO.
			body: []byte{0x07, 'G', 'O', 'O', 'D', 'B', 'Y', 'E'},
		},
		{
			name: "missing-username-length",
			// HELLO with zero-byte payload.
			body: []byte{0x05, 'H', 'E', 'L', 'L', 'O'},
		},
		{
			name: "username-truncated",
			// Declares 4-byte username but supplies 2.
			body: []byte{0x05, 'H', 'E', 'L', 'L', 'O', 0x04, 'a', 'b'},
		},
		{
			name: "missing-password-length",
			// Declares a 0-byte username then nothing for password length.
			body: []byte{0x05, 'H', 'E', 'L', 'L', 'O', 0x00},
		},
		{
			name: "password-truncated",
			// Declares a 4-byte password but supplies 1.
			body: []byte{0x05, 'H', 'E', 'L', 'L', 'O', 0x00, 0x04, 'x'},
		},
		{
			name: "trailing-garbage",
			// Valid user+pass with trailing junk.
			body: []byte{0x05, 'H', 'E', 'L', 'L', 'O', 0x01, 'a', 0x01, 'b', 0xff},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := NewPlainServer(func(string, string) bool { return true }, nil)
			// Advance server past the passive seed state so the next call
			// exercises the HELLO parse path.
			if _, _, err := server.NextHandshake(nil); err != nil {
				t.Fatalf("seed: %v", err)
			}
			_, _, err := server.NextHandshake(tc.body)
			if err == nil {
				t.Fatalf("NextHandshake(%q): want error, got nil", tc.body)
			}
			if !errors.Is(err, zmqerr.ErrMalformedFrame) {
				t.Fatalf("NextHandshake(%q): want ErrMalformedFrame, got %v", tc.body, err)
			}
		})
	}
}

func TestPlainClientRejectsMalformedPeer(t *testing.T) {
	// The client starts by emitting HELLO; the server reply must be WELCOME
	// or ERROR — anything else is ErrMalformedFrame.
	client := NewPlainClient("alice", "hunter2", nil)

	// Seed.
	_, _, err := client.NextHandshake(nil)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Feed a bogus reply.
	bogus := []byte{0x06, 'N', 'O', 'P', 'E', 'R', 'S'}
	_, _, err = client.NextHandshake(bogus)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !errors.Is(err, zmqerr.ErrMalformedFrame) {
		t.Fatalf("want ErrMalformedFrame, got %v", err)
	}
}

func TestPlainClientRejectsWelcomeWithPayload(t *testing.T) {
	client := NewPlainClient("alice", "hunter2", nil)

	if _, _, err := client.NextHandshake(nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// WELCOME with an extra byte should fail.
	bad := []byte{0x07, 'W', 'E', 'L', 'C', 'O', 'M', 'E', 0xff}
	_, _, err := client.NextHandshake(bad)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !errors.Is(err, zmqerr.ErrMalformedFrame) {
		t.Fatalf("want ErrMalformedFrame, got %v", err)
	}
}

func TestPlainEncodeDecodeFrameIdentity(t *testing.T) {
	m := NewPlainClient("u", "p", nil)
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
				t.Fatalf("EncodeFrame: %v", err)
			}
			if wireFlags != tc.flags {
				t.Fatalf("EncodeFrame flags: got %#x, want %#x", wireFlags, tc.flags)
			}
			if len(enc) != len(input) {
				t.Fatalf("EncodeFrame length changed: %d -> %d", len(input), len(enc))
			}
			plainFlags, dec, err := m.DecodeFrame(wireFlags, enc)
			if err != nil {
				t.Fatalf("DecodeFrame: %v", err)
			}
			if plainFlags != tc.flags {
				t.Fatalf("DecodeFrame flags: got %#x, want %#x", plainFlags, tc.flags)
			}
			if len(dec) != len(input) {
				t.Fatalf("DecodeFrame length changed: %d -> %d", len(input), len(dec))
			}
		}
	}
}

func TestPlainMetadataIsolated(t *testing.T) {
	local := map[string]string{"Socket-Type": "REQ"}
	m := NewPlainClient("u", "p", local)

	local["Socket-Type"] = "REP" // must not leak into mechanism
	got := m.Metadata()
	if got["Socket-Type"] != "REQ" {
		t.Fatalf("Metadata not isolated: got %q, want %q", got["Socket-Type"], "REQ")
	}

	got["Socket-Type"] = "PUB" // mutating returned map must not affect next call
	if again := m.Metadata(); again["Socket-Type"] != "REQ" {
		t.Fatalf("Metadata not isolated on return: got %q, want %q", again["Socket-Type"], "REQ")
	}
}

func TestPlainSetPeerMetadataStores(t *testing.T) {
	m := NewPlainServer(func(string, string) bool { return true }, nil).(*plainMech)
	props := map[string]string{"Socket-Type": "PAIR"}
	if err := m.SetPeerMetadata(props); err != nil {
		t.Fatalf("SetPeerMetadata: %v", err)
	}
	if !mapsEqual(m.PeerMetadata(), props) {
		t.Fatalf("PeerMetadata: got %v, want %v", m.PeerMetadata(), props)
	}

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

func TestPlainHelloEncodesLengthPrefixedCredentials(t *testing.T) {
	// Drive only the client seed and inspect the wire bytes: they must
	// match RFC 24 §4 exactly.
	client := NewPlainClient("alice", "hunter2", nil)
	out, done, err := client.NextHandshake(nil)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if done {
		t.Fatal("seed: done=true")
	}

	// [5]"HELLO" [5]"alice" [7]"hunter2"
	want := []byte{
		0x05, 'H', 'E', 'L', 'L', 'O',
		0x05, 'a', 'l', 'i', 'c', 'e',
		0x07, 'h', 'u', 'n', 't', 'e', 'r', '2',
	}
	if len(out) != len(want) {
		t.Fatalf("HELLO body length: got %d, want %d (%x vs %x)", len(out), len(want), out, want)
	}
	for i := range want {
		if out[i] != want[i] {
			t.Fatalf("HELLO body byte %d: got %#x, want %#x (full: %x)", i, out[i], want[i], out)
		}
	}
}

func TestPlainServerInitialSeedIsPassive(t *testing.T) {
	// Server's first NextHandshake(nil) is the session driver's seed call.
	// Per the asymmetric client/server flow (client sends first), the
	// server must return (nil, false, nil) so the session loop reads HELLO
	// before the next NextHandshake call.
	server := NewPlainServer(func(string, string) bool { return true }, nil)
	out, done, err := server.NextHandshake(nil)
	if err != nil {
		t.Fatalf("seed: unexpected err %v", err)
	}
	if done {
		t.Fatal("seed should not report done")
	}
	if out != nil {
		t.Fatalf("seed should return nil out, got %x", out)
	}
	// A second nil seed is an error — the seed state is one-shot.
	_, _, err = server.NextHandshake(nil)
	if err == nil {
		t.Fatal("second NextHandshake with nil input should error")
	}
}
