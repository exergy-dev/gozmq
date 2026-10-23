package gozmq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
)

// waitEvent blocks until an event of type typ arrives on events.
func waitEvent(t *testing.T, events <-chan gozmq.Event, typ gozmq.EventType) {
	t.Helper()
	testutil.WaitEvent(t, events, 5*time.Second, func(e gozmq.Event) bool { return e.Type == typ })
}

// TestPeerDetach_QueuedMessagesSurvive: messages a sender already delivered
// into the receiver's pipe stay receivable after the sender disconnects,
// whether the departing pipe was the only peer (the FairQueue's single-pipe
// path) or one of several. A ROUTER receiver still stamps them with the
// departed peer's identity.
func TestPeerDetach_QueuedMessagesSurvive(t *testing.T) {
	t.Parallel()
	for _, transport := range []string{"tcp", "inproc"} {
		for _, extraPeer := range []bool{false, true} {
			for _, router := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/extraPeer=%v/router=%v", transport, extraPeer, router), func(t *testing.T) {
					testPeerDetachQueued(t, transport, extraPeer, router)
				})
			}
		}
	}
}

func testPeerDetachQueued(t *testing.T, transport string, extraPeer, router bool) {
	recvType, sendType := gozmq.Pull, gozmq.Push
	if router {
		recvType, sendType = gozmq.Router, gozmq.Dealer
	}
	events := make(chan gozmq.Event, 64)
	zctx, pull := newTestSocket(t, recvType, gozmq.WithMonitor(events, gozmq.EventDisconnected))
	ep := "tcp://127.0.0.1:0"
	if transport == "inproc" {
		ep = "inproc://" + t.Name()
	}
	require.NoError(t, pull.Bind(ep))
	ep = pull.LastEndpoint()

	if extraPeer {
		idle, err := zctx.NewSocket(sendType)
		require.NoError(t, err)
		t.Cleanup(func() { _ = idle.Close() })
		require.NoError(t, idle.Connect(ep))
	}
	// TCP linger returns as soon as the writer has flushed. Inproc linger
	// waits for the pipe to close, but the messages are already in the
	// shared pipe buffer, so a short linger loses nothing.
	linger := 5 * time.Second
	if transport == "inproc" {
		linger = 50 * time.Millisecond
	}
	push, err := zctx.NewSocket(sendType, gozmq.WithLinger(linger))
	require.NoError(t, err)
	require.NoError(t, push.Connect(ep))

	// recv returns the body and, for ROUTER, the stamped identity.
	recv := func(ctx context.Context) (id []byte, body string, err error) {
		m, err := pull.RecvMsg(ctx)
		if err != nil {
			return nil, "", err
		}
		f := m.Frames()
		if router {
			return f[0], string(f[1]), nil
		}
		return nil, string(f[0]), nil
	}

	ctx := testutil.Ctx(t)
	require.NoError(t, push.SendBytes(ctx, []byte("warmup")))
	peerID, _, err := recv(ctx)
	require.NoError(t, err)

	const n = 500
	for i := range n {
		require.NoError(t, push.SendBytes(ctx, fmt.Appendf(nil, "%d", i)))
	}
	require.NoError(t, push.Close())
	// The receiver has detached the sender's pipe before we read anything.
	waitEvent(t, events, gozmq.EventDisconnected)

	ctx = testutil.Ctx(t)
	for i := range n {
		id, body, err := recv(ctx)
		require.NoError(t, err, "recv %d of %d after sender detached", i, n)
		require.Equal(t, fmt.Sprint(i), body)
		require.Equal(t, peerID, id, "identity of message %d", i)
	}
}

// requireSubEvent receives the next XPUB subscription event and checks it.
func requireSubEvent(t *testing.T, xpub *gozmq.Socket, subscribe bool, prefix string) {
	t.Helper()
	m, err := xpub.RecvMsg(testutil.Ctx(t))
	require.NoError(t, err)
	want := string(append([]byte{0}, prefix...))
	if subscribe {
		want = string(append([]byte{1}, prefix...))
	}
	require.Equal(t, want, string(m.Frames()[0]))
}

// TestXPub_EarlyDetachKeepsRefcounts: a peer that subscribes and leaves
// before the application reads the SUBSCRIBE must not leave a stale
// refcount behind, which would suppress a later subscriber's SUBSCRIBE.
func TestXPub_EarlyDetachKeepsRefcounts(t *testing.T) {
	t.Parallel()
	events := make(chan gozmq.Event, 64)
	zctx, xpub := newTestSocket(t, gozmq.XPub, gozmq.WithMonitor(events, gozmq.EventDisconnected))
	require.NoError(t, xpub.Bind("tcp://127.0.0.1:0"))
	ep := xpub.LastEndpoint()

	sub1, err := zctx.NewSocket(gozmq.Sub, gozmq.WithSubscribe([]byte("A")))
	require.NoError(t, err)
	require.NoError(t, sub1.Connect(ep))
	syncSub(t, xpub, sub1, []byte("A")) // XPUB has applied the SUBSCRIBE
	require.NoError(t, sub1.Close())
	waitEvent(t, events, gozmq.EventDisconnected)

	requireSubEvent(t, xpub, true, "A")
	requireSubEvent(t, xpub, false, "A")

	sub2, err := zctx.NewSocket(gozmq.Sub, gozmq.WithSubscribe([]byte("A")))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub2.Close() })
	require.NoError(t, sub2.Connect(ep))
	requireSubEvent(t, xpub, true, "A")
}

// TestXPub_ForeignCancelIgnored: a CANCEL for a prefix the sending peer never
// subscribed to must not disturb another peer's subscription or surface.
func TestXPub_ForeignCancelIgnored(t *testing.T) {
	t.Parallel()
	zctx, xpub := newTestSocket(t, gozmq.XPub)
	require.NoError(t, xpub.Bind("tcp://127.0.0.1:0"))
	ep := xpub.LastEndpoint()
	ctx := testutil.Ctx(t)

	good, err := zctx.NewSocket(gozmq.Sub, gozmq.WithSubscribe([]byte("A")))
	require.NoError(t, err)
	t.Cleanup(func() { _ = good.Close() })
	require.NoError(t, good.Connect(ep))
	requireSubEvent(t, xpub, true, "A")

	evil, err := zctx.NewSocket(gozmq.XSub, gozmq.WithSubscribe([]byte("B")))
	require.NoError(t, err)
	t.Cleanup(func() { _ = evil.Close() })
	require.NoError(t, evil.Connect(ep))
	requireSubEvent(t, xpub, true, "B") // evil is attached

	require.NoError(t, evil.SendBytes(ctx, []byte("\x00A"))) // never subscribed to A
	require.NoError(t, evil.SendBytes(ctx, []byte("\x01C")))
	// Per-peer FIFO: the CANCEL was processed before SUBSCRIBE C.
	requireSubEvent(t, xpub, true, "C")

	syncSub(t, xpub, good, []byte("A")) // good still receives A
}
