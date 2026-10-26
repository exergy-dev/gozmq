//go:build interop

package interop

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/security"
)

// The tests in this package drive one gozmq socket against one libzmq socket.
// Every pattern is exercised with each implementation on the bind side, so a
// test is written once against two interchangeable ends: a link joins a
// gozmq end and a libzmq end, and a direction says which of them binds.

// direction says which implementation binds (plays the server) on a link.
type direction bool

const (
	gozmqBinds  direction = true
	libzmqBinds direction = false
)

var bothDirections = []direction{gozmqBinds, libzmqBinds}

func (d direction) String() string {
	if d == gozmqBinds {
		return "gozmq-binds"
	}
	return "libzmq-binds"
}

// peerEvent is the connection-event vocabulary both monitors can express.
type peerEvent int

const (
	evConnected peerEvent = iota // client: TCP connection established
	evAccepted                   // server: connection accepted
	evDisconnected
)

const (
	waitTimeout = 5 * time.Second
	// eventBuffer is the monitor channel depth. gozmq drops events when the
	// channel is full, and a reconnecting client emits one per retry.
	eventBuffer = 256
)

// end is one end of a link, whichever implementation it is. send and recv
// fail the test on error (recv after waitTimeout); the *Within variants
// return the error so callers can assert on timeouts.
type end interface {
	send(frames ...[]byte)
	recv() [][]byte
	sendWithin(d time.Duration, frames ...[]byte) error
	recvWithin(d time.Duration) ([][]byte, error)
	subscribe(prefix []byte)
	disconnect(endpoint string)
	// waitFor fails the test unless ev is observed within waitTimeout.
	waitFor(ev peerEvent)
	// noEvent fails the test if ev is observed within d.
	noEvent(ev peerEvent, d time.Duration)
	close() error
}

// isTimeout reports whether err is a receive/send timeout from either
// implementation.
func isTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || isLibzmqTimeout(err)
}

func isLibzmqTimeout(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "Resource temporarily unavailable") || strings.Contains(err.Error(), "errno 11"))
}

// ---- gozmq end -----------------------------------------------------------

type gozmqEnd struct {
	t      *testing.T
	s      *gozmq.Socket
	events chan gozmq.Event
}

// newGozmqEnd creates a monitored gozmq socket. Linger is zero unless opts
// say otherwise so teardown never waits on unread messages.
func newGozmqEnd(t *testing.T, ctx *gozmq.Context, typ gozmq.SocketType, opts ...gozmq.Option) *gozmqEnd {
	t.Helper()
	events := make(chan gozmq.Event, eventBuffer)
	all := append([]gozmq.Option{gozmq.WithLinger(0), gozmq.WithMonitor(events, gozmq.EventAll)}, opts...)
	s, err := ctx.NewSocket(typ, all...)
	require.NoError(t, err, "gozmq new %v", typ)
	t.Cleanup(func() { _ = s.Close() })
	return &gozmqEnd{t: t, s: s, events: events}
}

func (e *gozmqEnd) send(frames ...[]byte) {
	e.t.Helper()
	require.NoError(e.t, e.sendWithin(waitTimeout, frames...), "gozmq send")
}

func (e *gozmqEnd) recv() [][]byte {
	e.t.Helper()
	got, err := e.recvWithin(waitTimeout)
	require.NoError(e.t, err, "gozmq recv")
	return got
}

func (e *gozmqEnd) sendWithin(d time.Duration, frames ...[]byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return e.s.SendMsg(ctx, gozmq.NewMsgFromFrames(frames...))
}

func (e *gozmqEnd) recvWithin(d time.Duration) ([][]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	m, err := e.s.RecvMsg(ctx)
	if err != nil {
		return nil, err
	}
	return m.Frames(), nil
}

func (e *gozmqEnd) subscribe(prefix []byte) {
	e.t.Helper()
	require.NoError(e.t, e.s.Subscribe(prefix))
}

func (e *gozmqEnd) disconnect(endpoint string) {
	e.t.Helper()
	require.NoError(e.t, e.s.Disconnect(endpoint))
}

func (e *gozmqEnd) close() error { return e.s.Close() }

var gozmqEvents = map[peerEvent]gozmq.EventType{
	evConnected:    gozmq.EventConnected,
	evAccepted:     gozmq.EventAccepted,
	evDisconnected: gozmq.EventDisconnected,
}

func (e *gozmqEnd) waitFor(ev peerEvent) {
	e.t.Helper()
	want := gozmqEvents[ev]
	testutil.WaitEvent(e.t, e.events, waitTimeout, func(got gozmq.Event) bool { return got.Type == want })
}

func (e *gozmqEnd) noEvent(ev peerEvent, d time.Duration) {
	e.t.Helper()
	want := gozmqEvents[ev]
	seen := testutil.CollectUntil(e.events, d, func(evs []gozmq.Event) bool {
		return len(evs) > 0 && evs[len(evs)-1].Type == want
	})
	for _, got := range seen {
		require.NotEqual(e.t, want, got.Type, "unexpected %v within %v", want, d)
	}
}

// handshake waits for the first handshake-outcome event and returns its type.
func (e *gozmqEnd) handshake() gozmq.EventType {
	e.t.Helper()
	ev := testutil.WaitEvent(e.t, e.events, waitTimeout, func(ev gozmq.Event) bool {
		switch ev.Type {
		case gozmq.EventHandshakeSucceeded, gozmq.EventHandshakeFailedAuth, gozmq.EventHandshakeFailedProtocol:
			return true
		}
		return false
	})
	return ev.Type
}

// requireSessionIntact drains buffered events and fails on any sign that the
// session was replaced: a disconnect, or a further handshake after a
// reconnect.
func (e *gozmqEnd) requireSessionIntact() {
	e.t.Helper()
	for {
		select {
		case ev := <-e.events:
			switch ev.Type {
			case gozmq.EventDisconnected, gozmq.EventClosed, gozmq.EventHandshakeSucceeded,
				gozmq.EventHandshakeFailedProtocol, gozmq.EventHandshakeFailedAuth:
				require.Failf(e.t, "session did not survive", "event %v (reason %q)", ev.Type, ev.Reason)
			}
		default:
			return
		}
	}
}

// ---- libzmq end ----------------------------------------------------------

type libEnd struct {
	t   *testing.T
	s   *Sock
	mon *Sock // inproc PAIR fed by zmq_socket_monitor, wired before bind/connect
}

// newLibEnd creates a libzmq socket with a monitor attached.
func newLibEnd(t *testing.T, ctx *Ctx, typ int) *libEnd {
	t.Helper()
	s := ctx.NewSocket(typ)
	t.Cleanup(func() { _ = s.Close() })
	monEP := fmt.Sprintf("inproc://monitor-%p", s)
	require.NoError(t, s.Monitor(monEP, 0))
	mon := ctx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = mon.Close() })
	require.NoError(t, mon.Connect(monEP))
	return &libEnd{t: t, s: s, mon: mon}
}

func (e *libEnd) set(opt int, val any) {
	e.t.Helper()
	require.NoError(e.t, e.s.SetOption(opt, val), "libzmq setsockopt %d", opt)
}

func (e *libEnd) send(frames ...[]byte) {
	e.t.Helper()
	require.NoError(e.t, e.sendWithin(waitTimeout, frames...), "libzmq send")
}

func (e *libEnd) recv() [][]byte {
	e.t.Helper()
	got, err := e.recvWithin(waitTimeout)
	require.NoError(e.t, err, "libzmq recv")
	return got
}

func (e *libEnd) sendWithin(d time.Duration, frames ...[]byte) error {
	e.set(ZMQ_SNDTIMEO, int(d/time.Millisecond))
	return e.s.Send(frames)
}

func (e *libEnd) recvWithin(d time.Duration) ([][]byte, error) {
	e.set(ZMQ_RCVTIMEO, int(d/time.Millisecond))
	return e.s.Recv()
}

func (e *libEnd) subscribe(prefix []byte) {
	e.t.Helper()
	require.NoError(e.t, e.s.Subscribe(prefix))
}

func (e *libEnd) disconnect(endpoint string) {
	e.t.Helper()
	require.NoError(e.t, e.s.Disconnect(libEndpoint(endpoint)))
}

func (e *libEnd) close() error { return e.s.Close() }

var libEvents = map[peerEvent]int{
	evConnected:    ZMQ_EVENT_CONNECTED,
	evAccepted:     ZMQ_EVENT_ACCEPTED,
	evDisconnected: ZMQ_EVENT_DISCONNECTED,
}

// nextEvent returns the next monitor event id, or false if none arrives
// within d.
func (e *libEnd) nextEvent(d time.Duration) (int, bool) {
	e.t.Helper()
	require.NoError(e.t, e.mon.SetOption(ZMQ_RCVTIMEO, int(d/time.Millisecond)))
	frames, err := e.mon.Recv()
	if err != nil {
		require.True(e.t, isLibzmqTimeout(err), "monitor recv: %v", err)
		return 0, false
	}
	// Event frame: uint16 event id, uint32 value (little-endian); then the
	// endpoint frame.
	require.Len(e.t, frames, 2, "monitor frames")
	require.GreaterOrEqual(e.t, len(frames[0]), 6, "monitor header")
	return int(binary.LittleEndian.Uint16(frames[0])), true
}

func (e *libEnd) waitFor(ev peerEvent) {
	e.t.Helper()
	want := libEvents[ev]
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if got, ok := e.nextEvent(100 * time.Millisecond); ok && got == want {
			return
		}
	}
	require.Failf(e.t, "timed out", "no libzmq monitor event %d within %v", want, waitTimeout)
}

func (e *libEnd) noEvent(ev peerEvent, d time.Duration) {
	e.t.Helper()
	want := libEvents[ev]
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if got, ok := e.nextEvent(time.Until(deadline)); ok {
			require.NotEqual(e.t, want, got, "unexpected libzmq monitor event within %v", d)
		}
	}
}

// libEndpoint maps a gozmq endpoint onto libzmq's spelling: libzmq has no
// tcp6:// scheme and uses tcp:// plus ZMQ_IPV6 instead.
func libEndpoint(ep string) string { return strings.Replace(ep, "tcp6://", "tcp://", 1) }

var libTypes = map[gozmq.SocketType]int{
	gozmq.Pair: ZMQ_PAIR, gozmq.Pub: ZMQ_PUB, gozmq.Sub: ZMQ_SUB, gozmq.Req: ZMQ_REQ,
	gozmq.Rep: ZMQ_REP, gozmq.Dealer: ZMQ_DEALER, gozmq.Router: ZMQ_ROUTER, gozmq.Pull: ZMQ_PULL,
	gozmq.Push: ZMQ_PUSH, gozmq.XPub: ZMQ_XPUB, gozmq.XSub: ZMQ_XSUB, gozmq.Stream: ZMQ_STREAM,
}

// ---- link ----------------------------------------------------------------

// spec describes a link independently of which implementation binds. The
// server* fields apply to whichever side binds, the client* fields to
// whichever side connects; the *Opts go to that side when it is gozmq, the
// *Lib hooks run on it when it is libzmq, before Bind/Connect.
type spec struct {
	server, client         gozmq.SocketType
	serverOpts, clientOpts []gozmq.Option
	serverLib, clientLib   func(e *libEnd)
	ctxOpts                []gozmq.ContextOption
	// mech is the security mechanism; nil means NULL.
	mech *mech
	// libZAP is the ZAP handler a libzmq server consults; nil takes the
	// mechanism's own (PLAIN needs one, the others do not).
	libZAP security.ZAPHandler
	// endpoint defaults to tcp://127.0.0.1:0.
	endpoint string
	// skipIfBindFails skips the test when the server cannot bind (tcp6 on a
	// host without IPv6 loopback).
	skipIfBindFails bool
	// retryBind retries the bind for a second: a fixed port just released
	// by another socket may not be reusable immediately.
	retryBind bool
}

// pairSpec is a NULL PAIR link over loopback TCP.
func pairSpec() spec { return spec{server: gozmq.Pair, client: gozmq.Pair} }

type link struct {
	t      *testing.T
	dir    direction
	sp     spec
	gCtx   *gozmq.Context
	lCtx   *Ctx
	g      *gozmqEnd
	l      *libEnd
	server end
	client end
	// endpoint is the resolved server endpoint in gozmq spelling.
	endpoint string
	// zapDone reports the libzmq ZAP handler's completion when one runs;
	// zapSock is its REP socket.
	zapDone <-chan error
	zapSock *Sock
}

// newLink builds, binds and connects a link; it does not wait for the
// handshake.
func newLink(t *testing.T, dir direction, sp spec) *link {
	t.Helper()
	if sp.endpoint == "" {
		sp.endpoint = "tcp://127.0.0.1:0"
	}
	m := sp.mech
	if m == nil {
		m = nullMech()
	}
	l := &link{t: t, dir: dir, sp: sp, gCtx: gozmq.NewContext(sp.ctxOpts...), lCtx: NewCtx()}
	t.Cleanup(func() { _ = l.gCtx.Close() })
	t.Cleanup(func() { _ = l.lCtx.Close() })

	if dir == gozmqBinds {
		l.g = l.newGozmq(sp.server, m.gozmqServer, sp.serverOpts)
		l.l = l.newLib(sp.client, m.libClient, sp.clientLib)
		l.server, l.client = l.g, l.l
	} else {
		l.l = l.newLib(sp.server, m.libServer, sp.serverLib)
		l.g = l.newGozmq(sp.client, m.gozmqClient, sp.clientOpts)
		l.server, l.client = l.l, l.g
		zap := sp.libZAP
		if zap == nil {
			zap = m.libZAP
		}
		if zap != nil {
			l.zapDone, l.zapSock = startLibzmqZAPHandler(t, l.lCtx, zap)
		}
	}
	l.endpoint = l.bind(l.server, sp.endpoint)
	l.connect(l.client, l.endpoint)
	return l
}

func (l *link) newGozmq(typ gozmq.SocketType, mechOpts, extra []gozmq.Option) *gozmqEnd {
	l.t.Helper()
	return newGozmqEnd(l.t, l.gCtx, typ, append(append([]gozmq.Option{}, mechOpts...), extra...)...)
}

func (l *link) newLib(typ gozmq.SocketType, mechSetup, extra func(e *libEnd)) *libEnd {
	l.t.Helper()
	e := newLibEnd(l.t, l.lCtx, libTypes[typ])
	if strings.Contains(l.sp.endpoint, "[::") {
		e.set(ZMQ_IPV6, 1)
	}
	// Security options must precede Bind/Connect on libzmq.
	if mechSetup != nil {
		mechSetup(e)
	}
	if extra != nil {
		extra(e)
	}
	return e
}

// bind binds e to endpoint and returns the resolved endpoint.
func (l *link) bind(e end, endpoint string) string {
	l.t.Helper()
	var bindOnce func() (string, error)
	switch s := e.(type) {
	case *gozmqEnd:
		bindOnce = func() (string, error) {
			if err := s.s.Bind(endpoint); err != nil {
				return "", err
			}
			return s.s.LastEndpoint(), nil
		}
	case *libEnd:
		bindOnce = func() (string, error) {
			if err := retryEINTR(func() error { return s.s.Bind(libEndpoint(endpoint)) }); err != nil {
				return "", err
			}
			return s.s.LastEndpoint()
		}
	}
	deadline := time.Now().Add(time.Second)
	for {
		resolved, err := bindOnce()
		if err == nil {
			require.NotEmpty(l.t, resolved, "LastEndpoint after bind")
			return resolved
		}
		if l.sp.skipIfBindFails {
			l.t.Skipf("cannot bind %s: %v", endpoint, err)
		}
		if !l.sp.retryBind || time.Now().After(deadline) {
			require.NoError(l.t, err, "bind %s", endpoint)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func (l *link) connect(e end, endpoint string) {
	l.t.Helper()
	switch s := e.(type) {
	case *gozmqEnd:
		require.NoError(l.t, s.s.Connect(endpoint), "gozmq connect")
	case *libEnd:
		require.NoError(l.t, retryEINTR(func() error { return s.s.Connect(libEndpoint(endpoint)) }), "libzmq connect")
	}
}

// retryEINTR retries a libzmq call that returned EINTR. Go's async
// preemption raises signals that interrupt libzmq's syscalls, which the
// wrapper surfaces verbatim; bind and connect are safe to repeat.
func retryEINTR(fn func() error) error {
	var err error
	for range 10 {
		if err = fn(); err == nil || !isEINTR(err) {
			return err
		}
	}
	return err
}

func isEINTR(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "Interrupted system call") || strings.Contains(err.Error(), "errno 4)"))
}

// handshake returns the gozmq side's handshake outcome.
func (l *link) handshake() gozmq.EventType {
	l.t.Helper()
	return l.g.handshake()
}

// requireHandshake waits for the gozmq side to report a successful
// handshake and, when a libzmq ZAP handler is involved, for it to have
// answered.
func (l *link) requireHandshake() {
	l.t.Helper()
	require.Equal(l.t, gozmq.EventHandshakeSucceeded, l.handshake(), "handshake")
	l.requireZAPDone()
}

// requireZAPDone asserts the libzmq ZAP handler, if any, served its request.
func (l *link) requireZAPDone() {
	l.t.Helper()
	if l.zapDone == nil {
		return
	}
	require.NoError(l.t, testutil.RequireRecv(l.t, l.zapDone, waitTimeout, "libzmq ZAP handler did not complete"),
		"libzmq ZAP handler")
}

// closeLib closes the libzmq socket so its peer sees the connection drop.
// The ZAP socket goes first: a libzmq session that consulted ZAP holds a pipe
// to that REP socket, and tearing the pipe down needs the REP socket's owner
// to process commands, which an idle socket never does, so the session (and
// its TCP connection) would otherwise outlive zmq_close.
func (l *link) closeLib() {
	l.t.Helper()
	if l.zapSock != nil {
		require.NoError(l.t, l.zapSock.Close())
	}
	require.NoError(l.t, l.l.close())
}

// restartServer closes the server and binds a fresh socket of the same
// implementation and options to the same endpoint, retrying while the port
// is released.
func (l *link) restartServer() end {
	l.t.Helper()
	require.NoError(l.t, l.server.close(), "close server")
	m := l.sp.mech
	if m == nil {
		m = nullMech()
	}
	l.sp.retryBind = true
	if l.dir == gozmqBinds {
		l.server = l.newGozmq(l.sp.server, m.gozmqServer, l.sp.serverOpts)
	} else {
		l.server = l.newLib(l.sp.server, m.libServer, l.sp.serverLib)
	}
	l.bind(l.server, l.endpoint)
	return l.server
}

// roundTrip sends a multipart message with an empty middle frame from a to b
// and a single frame back.
func roundTrip(t *testing.T, a, b end) {
	t.Helper()
	fwd := [][]byte{[]byte("a-to-b"), {}, []byte("multipart")}
	a.send(fwd...)
	requireFrames(t, fwd, b.recv())
	back := [][]byte{[]byte("b-to-a")}
	b.send(back...)
	requireFrames(t, back, a.recv())
}

// requireFrames compares two multipart messages frame by frame, treating a
// nil frame and an empty one as equal.
func requireFrames(t *testing.T, want, got [][]byte) {
	t.Helper()
	require.Len(t, got, len(want), "frame count: got %q, want %q", got, want)
	for i := range want {
		require.True(t, bytes.Equal(want[i], got[i]), "frame %d: got %q, want %q", i, got[i], want[i])
	}
}
