package security

import (
	"testing"

	"github.com/stretchr/testify/require"

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
	require.Failf(t, "pumpPlain", "exceeded %d steps (client done=%v err=%v, server done=%v err=%v)", maxSteps, clientDone, cErr, serverDone, sErr)
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

	require.False(t, client.AsServer(), "client.AsServer should be false")
	require.True(t, server.AsServer(), "server.AsServer should be true")
	require.Equal(t, "PLAIN", client.Name())
	require.Equal(t, "PLAIN", server.Name())

	cerr, serr := pumpPlain(t, client, server)
	require.NoError(t, cerr, "client handshake")
	require.NoError(t, serr, "server handshake")

	clientImpl := client.(*plainMech)
	serverImpl := server.(*plainMech)

	require.Equal(t, serverMeta, clientImpl.PeerMetadata(), "client peer metadata")
	require.Equal(t, clientMeta, serverImpl.PeerMetadata(), "server peer metadata")

	// The client sends metadata in INITIATE and receives it in READY; the
	// server receives metadata in INITIATE and sends it in READY.
	require.True(t, clientImpl.metaSent, "client metadata sent flag")
	require.True(t, clientImpl.metaRecv, "client metadata recv flag")
	require.True(t, serverImpl.metaSent, "server metadata sent flag")
	require.True(t, serverImpl.metaRecv, "server metadata recv flag")
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
	require.ErrorIs(t, serr, zmqerr.ErrAuthFailed, "server error")
	require.ErrorIs(t, cerr, zmqerr.ErrAuthFailed, "client error")
}

func TestPlainHandshakeNilAuthCallback(t *testing.T) {
	// A nil authenticate callback rejects everything.
	client := NewPlainClient("alice", "hunter2", nil)
	server := NewPlainServer(nil, nil)

	cerr, serr := pumpPlain(t, client, server)
	require.ErrorIs(t, serr, zmqerr.ErrAuthFailed, "server error")
	require.ErrorIs(t, cerr, zmqerr.ErrAuthFailed, "client error")
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
			_, _, err := server.NextHandshake(nil)
			require.NoError(t, err, "seed")
			_, _, err = server.NextHandshake(tc.body)
			require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
		})
	}
}

func TestPlainClientRejectsMalformedPeer(t *testing.T) {
	// The client starts by emitting HELLO; the server reply must be WELCOME
	// or ERROR — anything else is ErrMalformedFrame.
	client := NewPlainClient("alice", "hunter2", nil)

	// Seed.
	_, _, err := client.NextHandshake(nil)
	require.NoError(t, err, "seed")

	// Feed a bogus reply.
	bogus := []byte{0x06, 'N', 'O', 'P', 'E', 'R', 'S'}
	_, _, err = client.NextHandshake(bogus)
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
}

func TestPlainClientRejectsWelcomeWithPayload(t *testing.T) {
	client := NewPlainClient("alice", "hunter2", nil)

	_, _, err := client.NextHandshake(nil)
	require.NoError(t, err, "seed")

	// WELCOME with an extra byte should fail.
	bad := []byte{0x07, 'W', 'E', 'L', 'C', 'O', 'M', 'E', 0xff}
	_, _, err = client.NextHandshake(bad)
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
}

func TestPlainClientAcceptsErrorAfterInitiate(t *testing.T) {
	client := NewPlainClient("alice", "hunter2", map[string]string{"Socket-Type": "PAIR"})

	out, done, err := client.NextHandshake(nil)
	require.NoError(t, err, "seed")
	require.False(t, done, "seed: done should be false")
	require.NotEmpty(t, out, "seed: out should not be empty")

	out, done, err = client.NextHandshake([]byte{0x07, 'W', 'E', 'L', 'C', 'O', 'M', 'E'})
	require.NoError(t, err, "WELCOME")
	require.False(t, done, "INITIATE: done should be false")
	require.NotEmpty(t, out, "INITIATE: out should not be empty")

	_, _, err = client.NextHandshake([]byte{0x05, 'E', 'R', 'R', 'O', 'R', 0x03, '4', '0', '0'})
	require.ErrorIs(t, err, zmqerr.ErrAuthFailed, "ERROR after INITIATE")
}

func TestPlainClientAcceptsCompatErrorAfterInitiate(t *testing.T) {
	client := NewPlainClient("alice", "hunter2", nil)

	_, _, err := client.NextHandshake(nil)
	require.NoError(t, err, "seed")
	out, done, err := client.NextHandshake([]byte{0x07, 'W', 'E', 'L', 'C', 'O', 'M', 'E'})
	require.NoError(t, err, "WELCOME")
	require.False(t, done, "INITIATE: done should be false")
	require.NotEmpty(t, out, "INITIATE: out should not be empty")

	_, _, err = client.NextHandshake([]byte{0x5e, 'R', 'R', 'O', 'R', 0x03, '4', '0', '0'})
	require.ErrorIs(t, err, zmqerr.ErrAuthFailed, "compat ERROR after INITIATE")
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
			require.NoError(t, err, "EncodeFrame")
			require.Equal(t, tc.flags, wireFlags, "EncodeFrame flags")
			require.Equal(t, len(input), len(enc), "EncodeFrame length")
			plainFlags, dec, err := m.DecodeFrame(wireFlags, enc)
			require.NoError(t, err, "DecodeFrame")
			require.Equal(t, tc.flags, plainFlags, "DecodeFrame flags")
			require.Equal(t, input, dec, "DecodeFrame output")
		}
	}
}

func TestPlainMetadataIsolated(t *testing.T) {
	local := map[string]string{"Socket-Type": "REQ"}
	m := NewPlainClient("u", "p", local)

	local["Socket-Type"] = "REP" // must not leak into mechanism
	got := m.Metadata()
	require.Equal(t, "REQ", got["Socket-Type"], "Metadata not isolated from caller mutation")

	got["Socket-Type"] = "PUB" // mutating returned map must not affect next call
	again := m.Metadata()
	require.Equal(t, "REQ", again["Socket-Type"], "Metadata not isolated on return")
}

func TestPlainSetPeerMetadataStores(t *testing.T) {
	m := NewPlainServer(func(string, string) bool { return true }, nil).(*plainMech)
	props := map[string]string{"Socket-Type": "PAIR"}
	require.NoError(t, m.SetPeerMetadata(props))
	require.Equal(t, props, m.PeerMetadata())

	props["Socket-Type"] = "PUB"
	require.Equal(t, "PAIR", m.PeerMetadata()["Socket-Type"], "PeerMetadata not isolated from caller mutations")

	require.NoError(t, m.SetPeerMetadata(nil))
	require.Nil(t, m.PeerMetadata(), "PeerMetadata after nil-set should be nil")
}

func TestPlainHelloEncodesLengthPrefixedCredentials(t *testing.T) {
	// Drive only the client seed and inspect the wire bytes: they must
	// match RFC 24 §4 exactly.
	client := NewPlainClient("alice", "hunter2", nil)
	out, done, err := client.NextHandshake(nil)
	require.NoError(t, err, "seed")
	require.False(t, done, "seed: done should be false")

	// [5]"HELLO" [5]"alice" [7]"hunter2"
	want := []byte{
		0x05, 'H', 'E', 'L', 'L', 'O',
		0x05, 'a', 'l', 'i', 'c', 'e',
		0x07, 'h', 'u', 'n', 't', 'e', 'r', '2',
	}
	require.Equal(t, want, out, "HELLO body")
}

func TestPlainServerInitialSeedIsPassive(t *testing.T) {
	// Server's first NextHandshake(nil) is the session driver's seed call.
	// Per the asymmetric client/server flow (client sends first), the
	// server must return (nil, false, nil) so the session loop reads HELLO
	// before the next NextHandshake call.
	server := NewPlainServer(func(string, string) bool { return true }, nil)
	out, done, err := server.NextHandshake(nil)
	require.NoError(t, err, "seed")
	require.False(t, done, "seed should not report done")
	require.Nil(t, out, "seed should return nil out")
	// A second nil seed is an error — the seed state is one-shot.
	_, _, err = server.NextHandshake(nil)
	require.Error(t, err, "second NextHandshake with nil input should error")
}
