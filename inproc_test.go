package gozmq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/security"
	"github.com/exergy-dev/gozmq/transport"
)

// inprocSocket creates a socket on zctx, closed at test end.
func inprocSocket(t *testing.T, zctx *Context, typ SocketType, opts ...Option) *Socket {
	t.Helper()
	s, err := zctx.NewSocket(typ, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func newInprocContext(t *testing.T) *Context {
	t.Helper()
	zctx := NewContext()
	t.Cleanup(func() { _ = zctx.Close() })
	return zctx
}

func linkCount(s *Socket) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.links)
}

// requireRoundTrip sends body from src and requires dst to receive it.
func requireRoundTrip(t *testing.T, src, dst *Socket, body string) {
	t.Helper()
	ctx := testutil.Ctx(t)
	require.NoError(t, src.SendBytes(ctx, []byte(body)))
	got, err := dst.RecvBytes(ctx)
	require.NoError(t, err)
	require.Equal(t, body, string(got))
}

// requireEvent requires an event of type typ to be queued on ch already:
// inproc emits synchronously, so nothing is waited for.
func requireEvent(t *testing.T, ch <-chan Event, typ EventType) {
	t.Helper()
	testutil.WaitEvent(t, ch, time.Second, func(e Event) bool { return e.Type == typ })
}

// TestInproc_ConnectBeforeBindWaits: a Connect may precede its Bind.
func TestInproc_ConnectBeforeBindWaits(t *testing.T) {
	zctx := newInprocContext(t)
	const ep = "inproc://connect-before-bind"
	push := inprocSocket(t, zctx, Push)
	require.NoError(t, push.Connect(ep))
	pull := inprocSocket(t, zctx, Pull)
	require.NoError(t, pull.Bind(ep))
	requireRoundTrip(t, push, pull, "late bind")
}

// TestInproc_DisconnectPending: Disconnect cancels a Connect still waiting
// for its Bind, so the later Bind wires nothing.
func TestInproc_DisconnectPending(t *testing.T) {
	zctx := newInprocContext(t)
	const ep = "inproc://disconnect-pending"
	events := make(chan Event, 16)
	push := inprocSocket(t, zctx, Push, WithMonitor(events, EventAll))
	require.NoError(t, push.Connect(ep))
	require.Equal(t, 1, linkCount(push))

	require.NoError(t, push.Disconnect(ep))
	require.ErrorIs(t, push.Disconnect(ep), ErrEndpointUnknown)
	require.Zero(t, linkCount(push))
	require.Empty(t, zctx.inproc.pending)

	pull := inprocSocket(t, zctx, Pull)
	require.NoError(t, pull.Bind(ep))
	require.Zero(t, linkCount(pull))
	select {
	case ev := <-events:
		t.Fatalf("unexpected event %v after Disconnect", ev.Type)
	default:
	}
}

// TestInproc_DisconnectLive: Disconnect of an established link detaches
// both sides before it returns.
func TestInproc_DisconnectLive(t *testing.T) {
	zctx := newInprocContext(t)
	const ep = "inproc://disconnect-live"
	srvEvents, cliEvents := make(chan Event, 16), make(chan Event, 16)
	srv := inprocSocket(t, zctx, Pair, WithMonitor(srvEvents, EventAll))
	require.NoError(t, srv.Bind(ep))
	cli := inprocSocket(t, zctx, Pair, WithMonitor(cliEvents, EventAll))
	require.NoError(t, cli.Connect(ep))
	requireEvent(t, cliEvents, EventConnected)
	requireEvent(t, srvEvents, EventAccepted)
	requireRoundTrip(t, cli, srv, "before")

	require.NoError(t, cli.Disconnect(ep))
	requireEvent(t, cliEvents, EventDisconnected)
	requireEvent(t, srvEvents, EventDisconnected)
	require.Zero(t, linkCount(cli))
	require.Zero(t, linkCount(srv))

	// The server's PAIR engine has let go of the old peer, so a new one
	// is accepted.
	other := inprocSocket(t, zctx, Pair)
	require.NoError(t, other.Connect(ep))
	requireRoundTrip(t, other, srv, "after")
}

// TestInproc_SecurityRejected: inproc runs no handshake, so a socket with a
// security mechanism can neither Bind nor Connect there.
func TestInproc_SecurityRejected(t *testing.T) {
	key := make([]byte, 32)
	mech := func(r security.Role, m map[string]string) (security.Mech, error) {
		return security.NewNull(r, m), nil
	}
	zctx := newInprocContext(t)
	for name, opt := range map[string]Option{
		"plain-server": WithPlainAuth(func(string, string) bool { return true }),
		"plain-client": WithPlainClient("u", "p"),
		"curve-server": WithCurveServer(key),
		"curve-client": WithCurveClient(key, key, key),
		"custom":       WithSecurityMech(mech),
	} {
		t.Run(name, func(t *testing.T) {
			s := inprocSocket(t, zctx, Pair, opt)
			ep := "inproc://sec-" + name
			require.ErrorIs(t, s.Bind(ep), ErrEndpointInvalid)
			require.ErrorIs(t, s.Connect(ep), ErrEndpointInvalid)
		})
	}
}

// TestInproc_IdentityBothWays: each side of a ROUTER-ROUTER link sees the
// other's WithIdentity, so either can address the other first.
func TestInproc_IdentityBothWays(t *testing.T) {
	zctx := newInprocContext(t)
	const ep = "inproc://identity-both-ways"
	a := inprocSocket(t, zctx, Router, WithIdentity([]byte("A")), WithRouterMandatory(true))
	require.NoError(t, a.Bind(ep))
	b := inprocSocket(t, zctx, Router, WithIdentity([]byte("B")), WithRouterMandatory(true))
	require.NoError(t, b.Connect(ep))

	ctx := testutil.Ctx(t)
	for _, tc := range []struct {
		src, dst *Socket
		to, from string
	}{{b, a, "A", "B"}, {a, b, "B", "A"}} {
		m := NewMsg()
		m.AppendFrame([]byte(tc.to))
		m.AppendFrame([]byte("hi"))
		require.NoError(t, tc.src.SendMsg(ctx, m))
		got, err := tc.dst.RecvMsg(ctx)
		require.NoError(t, err)
		require.Equal(t, [][]byte{[]byte(tc.from), []byte("hi")}, got.Frames())
	}
}

// TestInproc_HWMIsSum: as in libzmq, a direction queues the sender's send
// HWM plus the receiver's recv HWM.
func TestInproc_HWMIsSum(t *testing.T) {
	zctx := newInprocContext(t)
	const ep = "inproc://hwm-sum"
	pull := inprocSocket(t, zctx, Pull, WithRecvHWM(3))
	require.NoError(t, pull.Bind(ep))
	push := inprocSocket(t, zctx, Push, WithSendHWM(2), WithLinger(0))
	require.NoError(t, push.Connect(ep))

	for i := range 5 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := push.SendBytes(ctx, []byte{byte(i)})
		cancel()
		require.NoError(t, err, "send %d", i)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, push.SendBytes(ctx, []byte("full")), context.DeadlineExceeded)
}

// TestInproc_IncompatibleTypes: a Connect to a bound socket of the wrong
// type fails synchronously; one that waited for the Bind is reported as
// EventConnectFailed and dropped.
func TestInproc_IncompatibleTypes(t *testing.T) {
	zctx := newInprocContext(t)
	push := inprocSocket(t, zctx, Push)
	require.NoError(t, push.Bind("inproc://incompat-bound"))
	pub := inprocSocket(t, zctx, Pub)
	require.ErrorIs(t, pub.Connect("inproc://incompat-bound"), ErrFSMViolation)
	require.Zero(t, linkCount(pub))
	require.Zero(t, linkCount(push))

	events := make(chan Event, 16)
	sub := inprocSocket(t, zctx, Sub, WithMonitor(events, EventAll))
	require.NoError(t, sub.Connect("inproc://incompat-pending"))
	pull := inprocSocket(t, zctx, Pull)
	require.NoError(t, pull.Bind("inproc://incompat-pending"))
	requireEvent(t, events, EventConnectFailed)
	require.Zero(t, linkCount(sub))
	require.Zero(t, linkCount(pull))
}

// TestInproc_UnbindKeepsLinks: Unbind releases the name but keeps the links
// already made; a later Connect waits for the next Bind.
func TestInproc_UnbindKeepsLinks(t *testing.T) {
	zctx := newInprocContext(t)
	const ep = "inproc://unbind-pends"
	pull := inprocSocket(t, zctx, Pull)
	require.NoError(t, pull.Bind(ep))
	early := inprocSocket(t, zctx, Push)
	require.NoError(t, early.Connect(ep))

	require.NoError(t, pull.Unbind(ep))
	require.ErrorIs(t, pull.Unbind(ep), ErrEndpointUnknown)
	requireRoundTrip(t, early, pull, "still linked")

	late := inprocSocket(t, zctx, Push)
	require.NoError(t, late.Connect(ep))
	require.Len(t, zctx.inproc.pending[ep], 1)

	pull2 := inprocSocket(t, zctx, Pull)
	require.NoError(t, pull2.Bind(ep))
	require.Empty(t, zctx.inproc.pending)
	requireRoundTrip(t, late, pull2, "rebound")
}

// TestInproc_LinksDoNotAccumulate: links remove themselves, whichever side
// ends them, so churn leaves the bound socket where it started.
func TestInproc_LinksDoNotAccumulate(t *testing.T) {
	zctx := newInprocContext(t)
	const ep = "inproc://link-churn"
	pull := inprocSocket(t, zctx, Pull)
	require.NoError(t, pull.Bind(ep))
	push := inprocSocket(t, zctx, Push)
	for i := range 1000 {
		if i%2 == 0 {
			s, err := zctx.NewSocket(Push)
			require.NoError(t, err)
			require.NoError(t, s.Connect(ep))
			require.NoError(t, s.Close())
			continue
		}
		require.NoError(t, push.Connect(ep))
		require.NoError(t, push.Disconnect(ep))
	}
	require.Zero(t, linkCount(pull))
	require.Zero(t, linkCount(push))
	zctx.mu.Lock()
	require.Len(t, zctx.sockets, 2)
	zctx.mu.Unlock()
}

// TestInproc_CloseReleasesNamesAndWaiters: closing a socket frees its bound
// names and drops its waiting Connects from the registry.
func TestInproc_CloseReleasesNamesAndWaiters(t *testing.T) {
	zctx := newInprocContext(t)
	s := inprocSocket(t, zctx, Pull)
	for i := range 3 {
		require.NoError(t, s.Bind(fmt.Sprintf("inproc://close-bound-%d", i)))
	}
	w := inprocSocket(t, zctx, Push)
	require.NoError(t, w.Connect("inproc://close-waiting"))
	require.NoError(t, s.Close())
	require.NoError(t, w.Close())

	r := &zctx.inproc
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Empty(t, r.bound)
	require.Empty(t, r.names)
	require.Empty(t, r.pending)
}

// TestInproc_DuplicateBind: a name is bound by one socket at a time.
func TestInproc_DuplicateBind(t *testing.T) {
	zctx := newInprocContext(t)
	a := inprocSocket(t, zctx, Pull)
	b := inprocSocket(t, zctx, Pull)
	require.NoError(t, a.Bind("inproc://dup"))
	require.ErrorIs(t, b.Bind("inproc://dup"), ErrEndpointInvalid)
	require.ErrorIs(t, b.Bind("inproc://"), ErrEndpointInvalid)
}

// TestRegisterTransport_InprocPanics: inproc is not a transport.
func TestRegisterTransport_InprocPanics(t *testing.T) {
	zctx := newInprocContext(t)
	require.PanicsWithValue(t,
		"gozmq: inproc is built into the Context and cannot be registered as a transport",
		func() { zctx.RegisterTransport(fakeScheme("inproc")) })
}

// fakeScheme is a Transport stub that only reports its scheme.
type fakeScheme string

func (f fakeScheme) Scheme() string { return string(f) }
func (fakeScheme) Dial(context.Context, string) (transport.Conn, error) {
	return nil, errors.New("unused")
}
func (fakeScheme) Listen(context.Context, string) (transport.Listener, error) {
	return nil, errors.New("unused")
}
