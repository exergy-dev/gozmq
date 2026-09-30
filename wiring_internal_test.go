package gozmq

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/iface"
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

	for _, role := range []iface.Role{iface.RoleClient, iface.RoleServer} {
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

// TestNewMechanism_PlainServerAndClientConflict: a socket cannot be both the
// PLAIN server and the PLAIN client; now that the role no longer comes from
// bind/connect, that combination is an error instead of a silent pick.
func TestNewMechanism_PlainServerAndClientConflict(t *testing.T) {
	zctx := NewContext()
	defer func() { _ = zctx.Close() }()
	s, err := zctx.NewSocket(Pair,
		WithPlainAuth(func(string, string) bool { return true }),
		WithPlainClient("u", "p"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	for _, role := range []iface.Role{iface.RoleClient, iface.RoleServer} {
		_, err := s.newMechanism(role)
		require.ErrorContains(t, err, "mutually exclusive")
	}
}
