package gozmq_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
)

// waitPoll issues a single blocking Poll bounded by budget and returns
// the ready events. A blocking Poll must wake on its own once readiness
// arrives — including on single-peer sockets (regression coverage for
// the fixed wakeup bug that previously required spinning short Polls).
func waitPoll(t *testing.T, p *gozmq.Poller, budget time.Duration) []gozmq.PollEvent {
	t.Helper()
	ev, err := p.Poll(testutil.Ctx(t, budget))
	require.NoError(t, err, "poll")
	require.NotEmpty(t, ev, "poll returned no events and no error")
	return ev
}

// requireOnlyReady asserts events names exactly one socket, s, with the
// ready bit set.
func requireOnlyReady(t *testing.T, events []gozmq.PollEvent, s *gozmq.Socket, ready gozmq.PollEvents) {
	t.Helper()
	require.Len(t, events, 1, "unexpected poll events: %+v", events)
	require.Same(t, s, events[0].Socket, "event socket mismatch")
	require.NotZero(t, events[0].Events&ready, "expected %#x, got %#x", ready, events[0].Events)
}

// TestPoller_PullReadsAfterPush registers a PULL socket, pushes a
// message from a PUSH peer, and asserts Poll reports PollIn for the
// PULL socket. After the user RecvMsgs the message, a subsequent
// Poll with a short context should not return PollIn (no new data).
func TestPoller_PullReadsAfterPush(t *testing.T) {
	t.Parallel()
	_, pull, push := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", nil, nil)

	ctx := testutil.Ctx(t)
	require.NoError(t, push.SendMsg(ctx, gozmq.NewMsgFromBytes([]byte("hi"))), "push send")

	p := gozmq.NewPoller()
	p.Add(pull, gozmq.PollIn)

	requireOnlyReady(t, waitPoll(t, p, 2*time.Second), pull, gozmq.PollIn)

	// Drain the message.
	m, err := pull.RecvMsg(ctx)
	require.NoError(t, err, "recv")
	require.Equal(t, "hi", string(m.Bytes()))

	// No more data → Poll with a short ctx should time out.
	noCtx, noCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer noCancel()
	ev, err := p.Poll(noCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "expected timeout, got events %+v", ev)
}

func TestPoller_ZeroValue(t *testing.T) {
	t.Parallel()
	_, server, client := newSocketPair(t, gozmq.Pair, "inproc://poller-zero-value")

	require.NoError(t, client.SendMsg(testutil.Ctx(t), gozmq.NewMsgFromBytes([]byte("hi"))), "client send")

	var p gozmq.Poller
	p.Add(server, gozmq.PollIn)

	requireOnlyReady(t, waitPoll(t, &p, 2*time.Second), server, gozmq.PollIn)
}

func TestPoller_PairPollOut(t *testing.T) {
	t.Parallel()
	_, _, client := newSocketPair(t, gozmq.Pair, "inproc://poller-pair-out")

	p := gozmq.NewPoller()
	p.Add(client, gozmq.PollOut)

	requireOnlyReady(t, waitPoll(t, p, 2*time.Second), client, gozmq.PollOut)
}

func TestPoller_ReqPollInOut(t *testing.T) {
	t.Parallel()
	_, rep, req := newSocketPairTypes(t, gozmq.Rep, gozmq.Req, "inproc://poller-req", nil, nil)

	pOut := gozmq.NewPoller()
	pOut.Add(req, gozmq.PollOut)
	requireOnlyReady(t, waitPoll(t, pOut, 2*time.Second), req, gozmq.PollOut)

	callCtx := testutil.Ctx(t)
	require.NoError(t, req.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("request"))), "req send")
	request, err := rep.RecvMsg(callCtx)
	require.NoError(t, err, "rep recv")
	request.Release()
	require.NoError(t, rep.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("reply"))), "rep send")

	pIn := gozmq.NewPoller()
	pIn.Add(req, gozmq.PollIn)
	requireOnlyReady(t, waitPoll(t, pIn, 2*time.Second), req, gozmq.PollIn)
}

func TestPoller_PubPollOut(t *testing.T) {
	t.Parallel()
	_, _, pub := newSocketPairTypes(t, gozmq.Sub, gozmq.Pub, "inproc://poller-pub",
		[]gozmq.Option{gozmq.WithSubscribe(nil)}, nil)

	p := gozmq.NewPoller()
	p.Add(pub, gozmq.PollOut)
	requireOnlyReady(t, waitPoll(t, p, 2*time.Second), pub, gozmq.PollOut)
}

func TestPoller_XPubPollIn(t *testing.T) {
	t.Parallel()
	_, xpub, xsub := newSocketPairTypes(t, gozmq.XPub, gozmq.XSub, "inproc://poller-xpub", nil, nil)

	require.NoError(t, xsub.SendMsg(testutil.Ctx(t), gozmq.NewMsgFromBytes([]byte{1, 't'})), "xsub subscribe")

	p := gozmq.NewPoller()
	p.Add(xpub, gozmq.PollIn)
	requireOnlyReady(t, waitPoll(t, p, 2*time.Second), xpub, gozmq.PollIn)
}

func TestPoller_StreamPollInOut(t *testing.T) {
	t.Parallel()
	ctx := gozmq.NewContext()
	defer ctx.Close()

	stream, err := ctx.NewSocket(gozmq.Stream)
	require.NoError(t, err, "new stream")
	defer stream.Close()
	require.NoError(t, stream.Bind("tcp://127.0.0.1:0"), "bind stream")

	conn, err := net.DialTimeout("tcp", stream.LastEndpoint()[len("tcp://"):], 2*time.Second)
	require.NoError(t, err, "dial stream")
	defer conn.Close()
	_, err = conn.Write([]byte("hello"))
	require.NoError(t, err, "write stream peer")

	// Block on PollIn alone first: a PollIn|PollOut registration would
	// return immediately once the peer attaches (PollOut ready) and turn
	// the wait into a spin loop.
	pIn := gozmq.NewPoller()
	pIn.Add(stream, gozmq.PollIn)
	requireOnlyReady(t, waitPoll(t, pIn, 2*time.Second), stream, gozmq.PollIn)

	// With data still queued and the peer attached, a combined
	// registration must report both directions ready.
	pBoth := gozmq.NewPoller()
	pBoth.Add(stream, gozmq.PollIn|gozmq.PollOut)
	events := waitPoll(t, pBoth, 2*time.Second)
	require.Len(t, events, 1, "combined poll events = %+v, want PollIn|PollOut", events)
	require.Equal(t, gozmq.PollIn|gozmq.PollOut, events[0].Events, "combined poll events")
}

// TestPoller_RemoveStopsReporting registers a PULL, then removes it,
// then sends a message via PUSH. Poll must not report the removed
// socket.
func TestPoller_RemoveStopsReporting(t *testing.T) {
	t.Parallel()
	_, pull, push := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", nil, nil)

	p := gozmq.NewPoller()
	p.Add(pull, gozmq.PollIn)
	p.Remove(pull)

	ctx := testutil.Ctx(t)
	require.NoError(t, push.SendMsg(ctx, gozmq.NewMsgFromBytes([]byte("ignored"))), "send")

	pollCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	events, err := p.Poll(pollCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "removed socket still reports events: %+v", events)

	// Drain the message so test teardown is clean.
	_, err = pull.RecvMsg(ctx)
	require.NoError(t, err, "recv")
}

// TestPoller_OnlySecondPullReady registers two PULL sockets and
// pushes to only the second; Poll must report exactly the second.
func TestPoller_OnlySecondPullReady(t *testing.T) {
	t.Parallel()
	zctx := gozmq.NewContext()
	defer zctx.Close()

	pullA, err := zctx.NewSocket(gozmq.Pull)
	require.NoError(t, err, "new pullA")
	defer pullA.Close()
	require.NoError(t, pullA.Bind("tcp://127.0.0.1:0"), "bindA")

	pullB, err := zctx.NewSocket(gozmq.Pull)
	require.NoError(t, err, "new pullB")
	defer pullB.Close()
	require.NoError(t, pullB.Bind("tcp://127.0.0.1:0"), "bindB")

	push, err := zctx.NewSocket(gozmq.Push)
	require.NoError(t, err, "new push")
	defer push.Close()
	require.NoError(t, push.Connect(pullB.LastEndpoint()), "connect")

	ctx := testutil.Ctx(t)
	require.NoError(t, push.SendMsg(ctx, gozmq.NewMsgFromBytes([]byte("bee"))), "push send")

	p := gozmq.NewPoller()
	p.Add(pullA, gozmq.PollIn)
	p.Add(pullB, gozmq.PollIn)

	requireOnlyReady(t, waitPoll(t, p, 2*time.Second), pullB, gozmq.PollIn)

	// Drain so teardown is clean.
	_, err = pullB.RecvMsg(ctx)
	require.NoError(t, err, "recv")
}

// TestPoller_TwoWaitersSameSocket parks two Polls on one readable socket —
// through two Pollers and through concurrent Poll calls on one Poller (which
// the Poller documents as safe). Engines expose a single-slot readiness
// gate, so the first waiter to wake consumes the token; the message stays
// queued, and the second waiter must still wake rather than sleep until its
// ctx expires.
func TestPoller_TwoWaitersSameSocket(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		pollers func() (*gozmq.Poller, *gozmq.Poller)
	}{
		{"two pollers", func() (*gozmq.Poller, *gozmq.Poller) { return gozmq.NewPoller(), gozmq.NewPoller() }},
		{"one poller", func() (*gozmq.Poller, *gozmq.Poller) { p := gozmq.NewPoller(); return p, p }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			zctx := gozmq.NewContext()
			defer zctx.Close()
			pull, err := zctx.NewSocket(gozmq.Pull, gozmq.WithLinger(0))
			require.NoError(t, err, "new pull")
			defer pull.Close()
			require.NoError(t, pull.Bind("inproc://poller-two-waiters"), "bind")
			push, err := zctx.NewSocket(gozmq.Push, gozmq.WithLinger(0))
			require.NoError(t, err, "new push")
			defer push.Close()
			require.NoError(t, push.Connect("inproc://poller-two-waiters"), "connect")

			p1, p2 := tc.pollers()
			p1.Add(pull, gozmq.PollIn)
			p2.Add(pull, gozmq.PollIn)
			res := make(chan error, 2)
			for _, p := range []*gozmq.Poller{p1, p2} {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()
					_, err := p.Poll(ctx)
					res <- err
				}()
			}
			// Let both waiters park. If one has not yet, it finds the
			// message on its immediate scan and the test passes
			// trivially rather than flaking.
			time.Sleep(100 * time.Millisecond)

			require.NoError(t, push.SendMsg(testutil.Ctx(t, time.Second), gozmq.NewMsgFromBytes([]byte("x"))), "send")
			for range 2 {
				require.NoError(t, <-res, "poll on a readable socket")
			}
		})
	}
}

// TestPoller_ContextCancel asserts Poll returns ctx.Err() when ctx
// cancels before any socket becomes ready.
func TestPoller_ContextCancel(t *testing.T) {
	t.Parallel()
	zctx := gozmq.NewContext()
	defer zctx.Close()

	pull, err := zctx.NewSocket(gozmq.Pull)
	require.NoError(t, err, "new pull")
	defer pull.Close()
	require.NoError(t, pull.Bind("tcp://127.0.0.1:0"), "bind")

	p := gozmq.NewPoller()
	p.Add(pull, gozmq.PollIn)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	events, err := p.Poll(ctx)
	require.ErrorIs(t, err, context.Canceled, "expected Canceled, got events %+v", events)
}

// TestPoller_DealerRouterPollOut verifies PollOut on a DEALER once
// it has connected to a ROUTER peer.
func TestPoller_DealerRouterPollOut(t *testing.T) {
	t.Parallel()
	_, _, dealer := newSocketPairTypes(t, gozmq.Router, gozmq.Dealer, "tcp://127.0.0.1:0", nil, nil)

	p := gozmq.NewPoller()
	p.Add(dealer, gozmq.PollOut)

	// DEALER does not become PollOut-ready until its session pipe is
	// attached post-handshake; the blocking Poll parks until the
	// attach arms the engine's out-gate.
	requireOnlyReady(t, waitPoll(t, p, 2*time.Second), dealer, gozmq.PollOut)
}

// BenchmarkPoller_PollReady measures steady-state Poll over a populated
// registration set with one always-ready socket: the cached-snapshot
// fast path (no reflect.Select rebuild per call).
func BenchmarkPoller_PollReady(b *testing.B) {
	zctx := gozmq.NewContext()
	defer zctx.Close()

	p := gozmq.NewPoller()
	// Seven idle PULL sockets plus one PAIR pair with a buffered message.
	for i := 0; i < 7; i++ {
		s, err := zctx.NewSocket(gozmq.Pull)
		require.NoError(b, err)
		defer s.Close()
		p.Add(s, gozmq.PollIn)
	}
	a, err := zctx.NewSocket(gozmq.Pair)
	require.NoError(b, err)
	defer a.Close()
	require.NoError(b, a.Bind("inproc://bench-poller"))
	z, err := zctx.NewSocket(gozmq.Pair)
	require.NoError(b, err)
	defer z.Close()
	require.NoError(b, z.Connect("inproc://bench-poller"))
	require.NoError(b, z.SendBytes(context.Background(), []byte("x")))
	p.Add(a, gozmq.PollIn)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Poll(ctx); err != nil {
			b.Fatal(err)
		}
	}
	// Exclude the deferred socket/context teardown (linger drain) from
	// the per-op accounting.
	b.StopTimer()
}
