package gozmq_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/security"
)

// Socket-level security regressions: each test pins a case where a
// configured mechanism was silently skipped or an unauthenticated peer could
// make the server do unbounded work.

// handshakeFailed is the monitor mask the no-delivery tests watch.
const handshakeFailed = gozmq.EventHandshakeFailedProtocol | gozmq.EventHandshakeFailedAuth

// rejectedPair is newSocketPairTypes with both sockets reporting handshake
// failures on the returned channel.
func rejectedPair(t *testing.T, srvType, cliType gozmq.SocketType, srvOpts, cliOpts []gozmq.Option) (<-chan gozmq.Event, *gozmq.Socket, *gozmq.Socket) {
	t.Helper()
	events := make(chan gozmq.Event, 16)
	mon := gozmq.WithMonitor(events, handshakeFailed)
	_, srv, cli := newSocketPairTypes(t, srvType, cliType, "tcp://127.0.0.1:0",
		append(srvOpts, mon), append(cliOpts, mon))
	return events, srv, cli
}

// requireNoDelivery waits for a handshake failure on events, then sends from
// src and asserts dst receives nothing: the peers never completed a handshake.
func requireNoDelivery(t *testing.T, events <-chan gozmq.Event, src, dst *gozmq.Socket, what string) {
	t.Helper()
	testutil.WaitEvent(t, events, 5*time.Second, func(e gozmq.Event) bool { return e.Type&handshakeFailed != 0 })
	ctx := testutil.Ctx(t, 100*time.Millisecond)
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
	t.Parallel()
	t.Run("xor binder, NULL dialer", func(t *testing.T) {
		var frames atomic.Int64
		events, srv, cli := rejectedPair(t, gozmq.Pull, gozmq.Push, []gozmq.Option{gozmq.WithSecurityMech(newXORMech(&frames))}, nil)
		requireNoDelivery(t, events, cli, srv, "custom-mech binder")
	})
	t.Run("rejecting binder, NULL dialer", func(t *testing.T) {
		opt := gozmq.WithSecurityMech(func(r security.Role, m map[string]string) (security.Mech, error) {
			return &rejectMech{security.NewMechBase(r, m)}, nil
		})
		events, srv, cli := rejectedPair(t, gozmq.Pair, gozmq.Pair, []gozmq.Option{opt}, nil)
		requireNoDelivery(t, events, cli, srv, "rejecting binder")
	})
	t.Run("NULL binder, xor dialer", func(t *testing.T) {
		var frames atomic.Int64
		events, srv, cli := rejectedPair(t, gozmq.Pull, gozmq.Push, nil, []gozmq.Option{gozmq.WithSecurityMech(newXORMech(&frames))})
		requireNoDelivery(t, events, cli, srv, "custom-mech dialer")
	})
	t.Run("xor both sides runs the mechanism", func(t *testing.T) {
		var frames atomic.Int64
		opt := gozmq.WithSecurityMech(newXORMech(&frames))
		_, srv, cli := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0",
			[]gozmq.Option{opt}, []gozmq.Option{opt})
		ctx := testutil.Ctx(t)
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
	t.Parallel()
	const ep = "tcp://127.0.0.1:0"
	t.Run("auth socket connects to NULL peer", func(t *testing.T) {
		var called atomic.Bool
		events, nullSrv, authed := rejectedPair(t, gozmq.Push, gozmq.Pull, nil, []gozmq.Option{gozmq.WithPlainAuth(func(string, string) bool { called.Store(true); return false })})
		requireNoDelivery(t, events, nullSrv, authed, "WithPlainAuth connecting to NULL")
		require.False(t, called.Load(), "NULL peer never sends HELLO")
	})
	t.Run("client socket binds, NULL peer connects", func(t *testing.T) {
		events, plainCli, nullPeer := rejectedPair(t, gozmq.Pull, gozmq.Push, []gozmq.Option{gozmq.WithPlainClient("alice", "pw")}, nil)
		requireNoDelivery(t, events, nullPeer, plainCli, "WithPlainClient binding, NULL peer")
	})
	t.Run("auth socket connects to client socket", func(t *testing.T) {
		var gotUser atomic.Value
		_, cliSide, authSide := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, ep,
			[]gozmq.Option{gozmq.WithPlainClient("alice", "pw")},
			[]gozmq.Option{gozmq.WithPlainAuth(func(u, p string) bool {
				gotUser.Store(u)
				return u == "alice" && p == "pw"
			})})
		ctx := testutil.Ctx(t)
		require.NoError(t, cliSide.SendBytes(ctx, []byte("hi")))
		got, err := authSide.RecvBytes(ctx)
		require.NoError(t, err)
		require.Equal(t, "hi", string(got))
		require.Equal(t, "alice", gotUser.Load())
	})
	t.Run("auth socket connects and rejects", func(t *testing.T) {
		events, cliSide, authSide := rejectedPair(t, gozmq.Push, gozmq.Pull, []gozmq.Option{gozmq.WithPlainClient("alice", "wrong")}, []gozmq.Option{gozmq.WithPlainAuth(func(u, p string) bool { return false })})
		requireNoDelivery(t, events, cliSide, authSide, "rejected PLAIN client")
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
	t.Parallel()
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

	events := make(chan gozmq.Event, 16)
	authSide, err := zctx.NewSocket(gozmq.Pull,
		gozmq.WithPlainAuth(func(string, string) bool { return true }),
		gozmq.WithZAPDomain("global"), gozmq.WithMonitor(events, handshakeFailed))
	require.NoError(t, err)
	t.Cleanup(func() { _ = authSide.Close() })
	require.NoError(t, authSide.Connect(cliSide.LastEndpoint()))

	requireNoDelivery(t, events, cliSide, authSide, "ZAP-rejected PLAIN client")
	require.Positive(t, zapCalls.Load(), "ZAP handler never consulted")
}

// TestSecurity_ZAPPlainErrorReportsAuthFailure: a ZAP handler that rejects
// with a bare error (not wrapping ErrAuthFailed) is still reported as an
// authorization failure on the server's monitor.
func TestSecurity_ZAPPlainErrorReportsAuthFailure(t *testing.T) {
	t.Parallel()
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

// requireConnOpen asserts a short read on c times out (the session is up)
// rather than returning EOF.
func requireConnOpen(t *testing.T, c net.Conn, what string) {
	t.Helper()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(300*time.Millisecond)))
	_, err := c.Read(make([]byte, 1))
	var ne net.Error
	require.Truef(t, errors.As(err, &ne) && ne.Timeout(), "%s: expected the session to stay open, got %v", what, err)
}

// requireConnClosed asserts the server closes c: draining it reaches EOF (or
// a reset) rather than a read timeout. Anything the server flushed before
// closing, such as a READY queued before the handshake failed, is discarded.
func requireConnClosed(t *testing.T, c net.Conn, what string) {
	t.Helper()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, err := io.Copy(io.Discard, c)
	var ne net.Error
	require.Falsef(t, errors.As(err, &ne) && ne.Timeout(), "%s: server kept the session open", what)
}

// TestSecurity_PostAuthStalledFrameAllocationBounded: after the handshake a
// peer that greets as ZMTP 3.0 gets no heartbeat, so a frame header that
// announces 64 MiB followed by silence used to pin 64 MiB per connection for
// as long as the connection lived. Allocation must track the bytes actually
// received. The connections stay open: this bounds memory without inventing
// an idle disconnect for legitimately quiet 3.0 peers.
func TestSecurity_PostAuthStalledFrameAllocationBounded(t *testing.T) {
	_, srv := newTestSocket(t, gozmq.Pull)
	require.NoError(t, srv.Bind("tcp://127.0.0.1:0"))
	addr := strings.TrimPrefix(srv.LastEndpoint(), "tcp://")

	const conns = 8
	cs := make([]net.Conn, 0, conns)
	for range conns {
		cs = append(cs, testutil.RawNullHandshake(t, addr, 0, "PUSH", nil))
	}

	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	for _, c := range cs {
		hdr := make([]byte, 9)
		hdr[0] = 0x02 // LONG
		binary.BigEndian.PutUint64(hdr[1:], 64<<20-1)
		_, err := c.Write(append(hdr, 'x', 'y')) // two body bytes, then silence
		require.NoError(t, err)
	}
	// Let every reader consume its header and park in the body read.
	time.Sleep(300 * time.Millisecond)

	var after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&after)
	growth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("HeapAlloc growth for %d stalled 64 MiB frames: %d KiB", conns, growth>>10)
	require.Less(t, growth, int64(8<<20), "stalled frames allocated attacker-announced sizes")

	var wg sync.WaitGroup
	for i, c := range cs {
		wg.Go(func() { requireConnOpen(t, c, fmt.Sprintf("conn %d", i)) })
	}
	wg.Wait()
}

// pingFrame builds a ZMTP PING command frame with the given context.
func pingFrame(context []byte) []byte {
	body := append([]byte{4, 'P', 'I', 'N', 'G', 0, 10}, context...)
	return append([]byte{0x04, byte(len(body))}, body...)
}

// TestSecurity_PingContextCapped: ZMTP 3.1 limits the PING context to 16
// bytes. A longer one used to be echoed back verbatim, letting an
// authenticated peer queue PONGs up to MaxMessageSize each; now it is a
// protocol error that ends the session.
func TestSecurity_PingContextCapped(t *testing.T) {
	t.Parallel()
	_, srv := newTestSocket(t, gozmq.Pull)
	require.NoError(t, srv.Bind("tcp://127.0.0.1:0"))
	addr := strings.TrimPrefix(srv.LastEndpoint(), "tcp://")

	ok := testutil.RawNullHandshake(t, addr, 1, "PUSH", nil)
	_, err := ok.Write(pingFrame(make([]byte, 16)))
	require.NoError(t, err)
	// The PONG comes back: COMMAND frame, "\x04PONG" + 16 context bytes.
	require.NoError(t, ok.SetReadDeadline(time.Now().Add(3*time.Second)))
	pong := make([]byte, 2+5+16)
	_, err = io.ReadFull(ok, pong)
	require.NoError(t, err, "PONG for a 16-byte context")
	require.Equal(t, []byte{0x04, 21, 4, 'P', 'O', 'N', 'G'}, pong[:7])

	bad := testutil.RawNullHandshake(t, addr, 1, "PUSH", nil)
	_, err = bad.Write(pingFrame(make([]byte, 17)))
	require.NoError(t, err)
	requireConnClosed(t, bad, "17-byte PING context")
}

// TestSecurity_PeerIdentityValidated: a peer-advertised Identity must obey
// ZMTP 3.1 (at most 255 bytes, no leading zero byte). A leading zero would
// let a peer collide with the identities a ROUTER synthesizes and take over
// another peer's routing slot.
func TestSecurity_PeerIdentityValidated(t *testing.T) {
	t.Parallel()
	_, srv := newTestSocket(t, gozmq.Router)
	require.NoError(t, srv.Bind("tcp://127.0.0.1:0"))
	addr := strings.TrimPrefix(srv.LastEndpoint(), "tcp://")

	good := testutil.RawNullHandshake(t, addr, 1, "DEALER", map[string]string{"Identity": "peer-a"})
	requireConnOpen(t, good, "valid identity")

	for name, id := range map[string]string{
		"leading-zero": "\x00peer",
		"too-long":     strings.Repeat("x", 256),
	} {
		c, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close() })
		require.NoError(t, c.SetDeadline(time.Now().Add(10*time.Second)))
		greet := testutil.Greeting("NULL", 1)
		_, err = c.Write(greet)
		require.NoError(t, err)
		_, err = io.ReadFull(c, make([]byte, 64))
		require.NoError(t, err, "%s: server greeting", name)
		ready := security.EncodeReady(map[string]string{"Socket-Type": "DEALER", "Identity": id})
		hdr := []byte{0x04 | 0x02, 0, 0, 0, 0, 0, 0, 0, 0}
		binary.BigEndian.PutUint64(hdr[1:], uint64(len(ready)))
		_, err = c.Write(append(hdr, ready...))
		require.NoError(t, err)
		requireConnClosed(t, c, name)
	}
}
