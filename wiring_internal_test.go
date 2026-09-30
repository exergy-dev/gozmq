package gozmq

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/security"
)

// TestNewMechanism_PlainRoleFromOption: the PLAIN handshake role comes from
// the option, whichever side of the connection the socket is on.
func TestNewMechanism_PlainRoleFromOption(t *testing.T) {
	zctx := NewContext()
	defer func() { _ = zctx.Close() }()
	srv, err := zctx.NewSocket(Pair, WithPlainAuth(func(string, string) bool { return true }))
	require.NoError(t, err)
	defer func() { _ = srv.Close() }()
	cli, err := zctx.NewSocket(Pair, WithPlainClient("u", "p"))
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	for _, role := range []security.Role{security.RoleClient, security.RoleServer} {
		m, err := srv.newMechanism(role)
		require.NoError(t, err)
		require.Equal(t, "PLAIN", m.Name())
		require.True(t, m.AsServer(), "WithPlainAuth socket must be the PLAIN server (transport role %d)", role)

		m, err = cli.newMechanism(role)
		require.NoError(t, err)
		require.Equal(t, "PLAIN", m.Name())
		require.False(t, m.AsServer(), "WithPlainClient socket must be the PLAIN client (transport role %d)", role)
	}
}

// TestNewSocket_RejectsConflictingSecurityOptions: a socket cannot be both
// the PLAIN server and the PLAIN client, nor PLAIN and CURVE at once. NewSocket
// rejects these, rather than failing every session later (PLAIN server plus
// client) or silently picking CURVE.
func TestNewSocket_RejectsConflictingSecurityOptions(t *testing.T) {
	zctx := NewContext()
	defer func() { _ = zctx.Close() }()
	key := make([]byte, 32)
	for name, opts := range map[string][]Option{
		"plain server+client": {WithPlainAuth(func(string, string) bool { return true }), WithPlainClient("u", "p")},
		"plain server+curve":  {WithPlainAuth(func(string, string) bool { return true }), WithCurveServer(key)},
		"plain client+curve":  {WithPlainClient("u", "p"), WithCurveClient(key, key, key)},
	} {
		_, err := zctx.NewSocket(Pair, opts...)
		require.ErrorContains(t, err, "mutually exclusive", name)
	}
}
