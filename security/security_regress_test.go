package security

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
)

// rejectAll is a ZAP authorizer that refuses every peer.
func rejectAll(string, string, string, [][]byte) (string, map[string]string, error) {
	return "", nil, zmqerr.ErrAuthFailed
}

// libzmqBrokenError is the ERROR body libzmq 4.3.4 puts on the wire (see
// libzmqBrokenErrorPrefix), with reason "400".
var libzmqBrokenError = []byte{0x5E, 'R', 'R', 'O', 'R', 0x03, '4', '0', '0'}

// TestNullClientReportsServerError: a NULL server that rejects the client
// via ZAP sends ERROR. The client must report an auth failure carrying the
// reason, not a malformed frame.
func TestNullClientReportsServerError(t *testing.T) {
	srv := NewNull(RoleServer, nil)
	srv.(ZapConfigurable).ConfigureZAP(rejectAll, "global", "addr")
	cli := NewNull(RoleClient, nil)

	ready, _, err := cli.NextHandshake(nil)
	require.NoError(t, err)
	_, _, err = srv.NextHandshake(nil)
	require.NoError(t, err)
	errCmd, _, serr := srv.NextHandshake(ready)
	require.ErrorIs(t, serr, zmqerr.ErrAuthFailed)
	require.NotEmpty(t, errCmd)

	_, done, cerr := cli.NextHandshake(errCmd)
	require.False(t, done)
	require.ErrorIs(t, cerr, zmqerr.ErrAuthFailed)
	require.NotErrorIs(t, cerr, zmqerr.ErrMalformedFrame)
	require.Contains(t, cerr.Error(), "Authentication failure")
}

// TestNullClientAcceptsLibzmqBrokenError: the same tolerance PLAIN has for
// libzmq's mis-encoded ERROR name applies to NULL.
func TestNullClientAcceptsLibzmqBrokenError(t *testing.T) {
	cli := NewNull(RoleClient, nil)
	_, _, err := cli.NextHandshake(nil)
	require.NoError(t, err)
	_, _, err = cli.NextHandshake(libzmqBrokenError)
	require.ErrorIs(t, err, zmqerr.ErrAuthFailed)
	require.Contains(t, err.Error(), "400")
}

// TestNullClientMalformedErrorReason: an ERROR whose reason is truncated is
// still a malformed frame.
func TestNullClientMalformedErrorReason(t *testing.T) {
	cli := NewNull(RoleClient, nil)
	_, _, err := cli.NextHandshake(nil)
	require.NoError(t, err)
	_, _, err = cli.NextHandshake([]byte{5, 'E', 'R', 'R', 'O', 'R', 9, 'x'})
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
}

// TestDecodeError covers the shared ERROR recogniser directly.
func TestDecodeError(t *testing.T) {
	for _, tc := range []struct {
		name     string
		body     []byte
		isError  bool
		reason   string
		malforms bool
	}{
		{"well-formed", append([]byte{5, 'E', 'R', 'R', 'O', 'R', 3}, "bad"...), true, "bad", false},
		{"libzmq broken", libzmqBrokenError, true, "400", false},
		{"ready", EncodeReady(nil), false, "", false},
		{"empty", nil, false, "", false},
		{"truncated name", []byte{7, 'W', 'E'}, false, "", false},
		{"truncated reason", []byte{5, 'E', 'R', 'R', 'O', 'R', 9, 'x'}, true, "", true},
		{"missing reason", []byte{5, 'E', 'R', 'R', 'O', 'R'}, true, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reason, isError, err := DecodeError(tc.body)
			require.Equal(t, tc.isError, isError)
			if tc.malforms {
				require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.reason, reason)
		})
	}
}

// TestNullZAP_SkippedWithoutDomain: like libzmq, NULL consults ZAP only when
// a ZAP domain is configured, so installing a handler for PLAIN/CURVE
// sockets does not start vetting every NULL socket in the Context.
func TestNullZAP_SkippedWithoutDomain(t *testing.T) {
	client := NewNull(RoleClient, nil)
	server := NewNull(RoleServer, nil)
	called := false
	server.(*nullMech).ConfigureZAP(func(string, string, string, [][]byte) (string, map[string]string, error) {
		called = true
		return "", nil, zmqerr.ErrAuthFailed
	}, "", "addr")
	cerr, serr := pumpNull(t, client, server)
	require.NoError(t, cerr)
	require.NoError(t, serr)
	require.False(t, called, "NULL consulted ZAP without a domain")
}

// TestZapResponseLongMetadataName: a policy returning a metadata name that
// cannot be encoded must produce a 500 response, not panic inside the
// handshake.
func TestZapResponseLongMetadataName(t *testing.T) {
	long := strings.Repeat("n", 256)
	var frames [][]byte
	require.NotPanics(t, func() {
		frames = EncodeZapResponse(&ZapResponse{
			RequestID:  []byte("id"),
			StatusCode: ZapStatusOK,
			StatusText: "OK",
			UserID:     "alice",
			Metadata:   map[string]string{long: "x"},
		})
	})
	resp, err := DecodeZapResponse(frames)
	require.NoError(t, err)
	require.Equal(t, ZapStatusServerError, resp.StatusCode)
	require.Equal(t, "id", string(resp.RequestID))
	require.Empty(t, resp.UserID, "a failed response must not carry a user-id")
	require.Empty(t, resp.Metadata)

	// Through the handler: the policy's success is overridden.
	h := NewZapHandler(zapPolicyFunc(func(string, string, string, [][]byte) (string, map[string]string, error) {
		return "alice", map[string]string{long: "x"}, nil
	}))
	var out [][]byte
	require.NotPanics(t, func() {
		out = h.ServeRequest(EncodeZapRequest(&ZapRequest{RequestID: []byte("r"), Mechanism: "NULL"}))
	})
	resp, err = DecodeZapResponse(out)
	require.NoError(t, err)
	require.Equal(t, ZapStatusServerError, resp.StatusCode)
}

type zapPolicyFunc func(domain, address, mech string, creds [][]byte) (string, map[string]string, error)

func (f zapPolicyFunc) Authenticate(d, a, m string, c [][]byte) (string, map[string]string, error) {
	return f(d, a, m, c)
}

// TestIdentityFramingMarker: NULL and PLAIN (both roles) leave frames
// unmodified on the wire, which the session layer detects through this
// optional interface rather than by mechanism name.
func TestIdentityFramingMarker(t *testing.T) {
	type identityFramer interface{ IdentityFraming() bool }
	for name, m := range map[string]Mech{
		"null client":  NewNull(RoleClient, nil),
		"null server":  NewNull(RoleServer, nil),
		"plain client": NewPlainClient("u", "p", nil),
		"plain server": NewPlainServer(nil, nil),
	} {
		f, ok := m.(identityFramer)
		require.Truef(t, ok, "%s does not implement IdentityFraming", name)
		require.Truef(t, f.IdentityFraming(), "%s", name)
	}
}
