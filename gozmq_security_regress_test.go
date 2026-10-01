package gozmq_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/security"
)

// Socket-level security regressions: each test pins a case where a
// configured mechanism was silently skipped or an unauthenticated peer could
// make the server do unbounded work.

// requireNoDelivery sends from src and asserts dst receives nothing within
// the window: the peers must never have completed a handshake.
func requireNoDelivery(t *testing.T, src, dst *gozmq.Socket, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	_ = src.SendBytes(ctx, []byte("unauthenticated"))
	got, err := dst.RecvBytes(ctx)
	require.Errorf(t, err, "%s: delivered %q without authentication", what, got)
}

// rejectMech is a custom mechanism whose handshake always fails, so any
// message that reaches its socket must have bypassed the handshake.
type rejectMech struct{ security.MechBase }

func (*rejectMech) Name() string { return "REJECT" }
func (*rejectMech) NextHandshake([]byte) ([]byte, bool, error) {
	return nil, false, errors.New("rejectMech: always reject")
}
func (*rejectMech) EncodeFrame(f uint8, b []byte) (uint8, []byte, error) { return f, b, nil }
func (*rejectMech) DecodeFrame(f uint8, b []byte) (uint8, []byte, error) { return f, b, nil }

// TestSecurity_CustomMechNotBypassed: a WithSecurityMech socket must not
// exchange traffic with a peer that skipped its handshake. (Over inproc,
// which runs no handshake, such a socket cannot Bind or Connect at all.)
func TestSecurity_CustomMechNotBypassed(t *testing.T) {
	t.Run("xor binder, NULL dialer", func(t *testing.T) {
		var frames atomic.Int64
		_, srv, cli := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0",
			[]gozmq.Option{gozmq.WithSecurityMech(newXORMech(&frames))}, nil)
		requireNoDelivery(t, cli, srv, "custom-mech binder")
	})
	t.Run("rejecting binder, NULL dialer", func(t *testing.T) {
		opt := gozmq.WithSecurityMech(func(r security.Role, m map[string]string) (security.Mech, error) {
			return &rejectMech{security.NewMechBase(r, m)}, nil
		})
		_, srv, cli := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair, "tcp://127.0.0.1:0",
			[]gozmq.Option{opt}, nil)
		requireNoDelivery(t, cli, srv, "rejecting binder")
	})
	t.Run("NULL binder, xor dialer", func(t *testing.T) {
		var frames atomic.Int64
		_, srv, cli := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0",
			nil, []gozmq.Option{gozmq.WithSecurityMech(newXORMech(&frames))})
		requireNoDelivery(t, cli, srv, "custom-mech dialer")
	})
	t.Run("xor both sides runs the mechanism", func(t *testing.T) {
		var frames atomic.Int64
		opt := gozmq.WithSecurityMech(newXORMech(&frames))
		_, srv, cli := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0",
			[]gozmq.Option{opt}, []gozmq.Option{opt})
		ctx := testCtx(t)
		require.NoError(t, cli.SendBytes(ctx, []byte("hello")))
		got, err := srv.RecvBytes(ctx)
		require.NoError(t, err)
		require.Equal(t, "hello", string(got))
		require.Positive(t, frames.Load(), "custom mechanism never saw a frame")
	})
}

// TestSecurity_PlainRoleFollowsOption: PLAIN server/client is decided by
// the configured option, not by whether the socket binds or connects.
// Before the fix a WithPlainAuth socket that connected (or a WithPlainClient
// socket that bound) fell back to NULL and authenticated nobody.
func TestSecurity_PlainRoleFollowsOption(t *testing.T) {
	const ep = "tcp://127.0.0.1:0"
	t.Run("auth socket connects to NULL peer", func(t *testing.T) {
		var called atomic.Bool
		_, nullSrv, authed := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, ep, nil,
			[]gozmq.Option{gozmq.WithPlainAuth(func(string, string) bool { called.Store(true); return false })})
		requireNoDelivery(t, nullSrv, authed, "WithPlainAuth connecting to NULL")
		require.False(t, called.Load(), "NULL peer never sends HELLO")
	})
	t.Run("client socket binds, NULL peer connects", func(t *testing.T) {
		_, plainCli, nullPeer := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, ep,
			[]gozmq.Option{gozmq.WithPlainClient("alice", "pw")}, nil)
		requireNoDelivery(t, nullPeer, plainCli, "WithPlainClient binding, NULL peer")
	})
	t.Run("auth socket connects to client socket", func(t *testing.T) {
		var gotUser atomic.Value
		_, cliSide, authSide := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, ep,
			[]gozmq.Option{gozmq.WithPlainClient("alice", "pw")},
			[]gozmq.Option{gozmq.WithPlainAuth(func(u, p string) bool {
				gotUser.Store(u)
				return u == "alice" && p == "pw"
			})})
		ctx := testCtx(t)
		require.NoError(t, cliSide.SendBytes(ctx, []byte("hi")))
		got, err := authSide.RecvBytes(ctx)
		require.NoError(t, err)
		require.Equal(t, "hi", string(got))
		require.Equal(t, "alice", gotUser.Load())
	})
	t.Run("auth socket connects and rejects", func(t *testing.T) {
		_, cliSide, authSide := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, ep,
			[]gozmq.Option{gozmq.WithPlainClient("alice", "wrong")},
			[]gozmq.Option{gozmq.WithPlainAuth(func(u, p string) bool { return false })})
		requireNoDelivery(t, cliSide, authSide, "rejected PLAIN client")
	})
}

// TestSecurity_PreAuthAllocationBounded: an unauthenticated peer must not be
// able to make the server reserve MaxMessageSize bytes by announcing a huge
// handshake frame. Before the fix each such connection pinned a 60 MiB body
// buffer (480 MiB for 8 connections) and was held open waiting for bytes
// that never came.
func TestSecurity_PreAuthAllocationBounded(t *testing.T) {
	_, srv := newTestSocket(t, gozmq.Pull, gozmq.WithPlainAuth(func(string, string) bool { return false }))
	require.NoError(t, srv.Bind("tcp://127.0.0.1:0"))
	addr := strings.TrimPrefix(srv.LastEndpoint(), "tcp://")

	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	const conns = 8
	cs := make([]net.Conn, 0, conns)
	t.Cleanup(func() {
		for _, c := range cs {
			_ = c.Close()
		}
	})
	for range conns {
		c, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		cs = append(cs, c)
		greet := make([]byte, 64)
		greet[0], greet[9], greet[10], greet[11] = 0xFF, 0x7F, 3, 1
		copy(greet[12:], "PLAIN")
		hdr := make([]byte, 9)
		hdr[0] = 0x04 | 0x02 // COMMAND|LONG
		binary.BigEndian.PutUint64(hdr[1:], 60<<20)
		msg := append(append(greet, hdr...), 5, 'H') // trickle two body bytes
		_, err = c.Write(msg)
		require.NoError(t, err)
	}

	// The server must drop every connection: after its 64-byte greeting the
	// next read sees EOF/reset, not a deadline.
	for i, c := range cs {
		require.NoError(t, c.SetReadDeadline(time.Now().Add(5*time.Second)))
		var greet [64]byte
		_, err := io.ReadFull(c, greet[:])
		require.NoErrorf(t, err, "conn %d: server greeting", i)
	}
	deadline := time.Now().Add(3 * time.Second)
	for i, c := range cs {
		require.NoError(t, c.SetReadDeadline(deadline))
		_, err := c.Read(make([]byte, 1))
		var ne net.Error
		assert.Falsef(t, errors.As(err, &ne) && ne.Timeout(),
			"conn %d: server kept an oversized pre-auth frame open", i)
		assert.Errorf(t, err, "conn %d: server sent data instead of closing", i)
	}

	var after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&after)
	growth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("HeapAlloc growth for %d oversized pre-auth frames: %d KiB", conns, growth>>10)
	require.Less(t, growth, int64(8<<20), "pre-auth frames allocated attacker-announced sizes")
}

// TestSecurity_ZapConsultedForConnectingServer: ZAP follows the mechanism's
// server role, so a PLAIN server that Connects is still vetted by the
// Context's ZAP handler.
func TestSecurity_ZapConsultedForConnectingServer(t *testing.T) {
	var zapCalls atomic.Int64
	handler := security.ZAPHandler(func(string, string, string, [][]byte) (string, map[string]string, error) {
		zapCalls.Add(1)
		return "", nil, gozmq.ErrAuthFailed
	})
	zctx := gozmq.NewContext(gozmq.WithZAPHandler(handler))
	t.Cleanup(func() { _ = zctx.Close() })

	cliSide, err := zctx.NewSocket(gozmq.Push, gozmq.WithPlainClient("alice", "pw"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cliSide.Close() })
	require.NoError(t, cliSide.Bind("tcp://127.0.0.1:0"))

	authSide, err := zctx.NewSocket(gozmq.Pull,
		gozmq.WithPlainAuth(func(string, string) bool { return true }),
		gozmq.WithZAPDomain("global"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = authSide.Close() })
	require.NoError(t, authSide.Connect(cliSide.LastEndpoint()))

	requireNoDelivery(t, cliSide, authSide, "ZAP-rejected PLAIN client")
	require.Positive(t, zapCalls.Load(), "ZAP handler never consulted")
}

// TestSecurity_ZAPPlainErrorReportsAuthFailure: a ZAP handler that rejects
// with a bare error (not wrapping ErrAuthFailed) is still reported as an
// authorization failure on the server's monitor.
func TestSecurity_ZAPPlainErrorReportsAuthFailure(t *testing.T) {
	handler := security.ZAPHandler(func(string, string, string, [][]byte) (string, map[string]string, error) {
		return "", nil, errors.New("not on the allowlist")
	})
	zctx := gozmq.NewContext(gozmq.WithZAPHandler(handler))
	t.Cleanup(func() { _ = zctx.Close() })

	srvEvents := make(chan gozmq.Event, 64)
	srv, err := zctx.NewSocket(gozmq.Pull,
		gozmq.WithPlainAuth(func(string, string) bool { return true }),
		gozmq.WithZAPDomain("global"),
		gozmq.WithMonitor(srvEvents, gozmq.EventAll))
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })
	require.NoError(t, srv.Bind("tcp://127.0.0.1:0"))

	cli, err := zctx.NewSocket(gozmq.Push, gozmq.WithPlainClient("alice", "pw"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	require.NoError(t, cli.Connect(srv.LastEndpoint()))

	collected := waitForEvents(t, srvEvents, 3*time.Second, func(evts []gozmq.Event) bool {
		return hasType(evts, gozmq.EventHandshakeFailedAuth)
	})
	require.True(t, hasType(collected, gozmq.EventHandshakeFailedAuth),
		"ZAP rejection not reported as an auth failure; events=%v", eventTypes(collected))
}
