package gozmq_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/rkm/gozmq"
)

// waitPoll spins Poll with a short timeout until at least one event
// arrives or the test budget expires. This buys us deterministic
// behaviour against the brief race between SendMsg landing on the
// peer's session pipe and the FairQueue forwarder enqueuing it into
// inbound — both are async, and Poll's level signal can only fire
// once the forwarder side completes.
func waitPoll(t *testing.T, p *gozmq.Poller, budget time.Duration) []gozmq.PollEvent {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		ev, err := p.Poll(ctx)
		cancel()
		if err == nil && len(ev) > 0 {
			return ev
		}
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("poll: %v", err)
		}
	}
	t.Fatalf("poll budget exhausted without ready events")
	return nil
}

// TestPoller_PullReadsAfterPush registers a PULL socket, pushes a
// message from a PUSH peer, and asserts Poll reports PollIn for the
// PULL socket. After the user RecvMsgs the message, a subsequent
// Poll with a short context should not return PollIn (no new data).
func TestPoller_PullReadsAfterPush(t *testing.T) {
	_, pull, push := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", nil, nil)

	sendCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := push.SendMsg(sendCtx, gozmq.NewMsgFromBytes([]byte("hi"))); err != nil {
		t.Fatalf("push send: %v", err)
	}

	p := gozmq.NewPoller()
	p.Add(pull, gozmq.PollIn)

	events := waitPoll(t, p, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %+v", len(events), events)
	}
	if events[0].Socket != pull {
		t.Fatalf("event socket mismatch")
	}
	if events[0].Events&gozmq.PollIn == 0 {
		t.Fatalf("expected PollIn, got %#x", events[0].Events)
	}

	// Drain the message.
	recvCtx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	m, err := pull.RecvMsg(recvCtx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if string(m.Bytes()) != "hi" {
		t.Fatalf("want hi, got %q", m.Bytes())
	}

	// No more data → Poll with a short ctx should time out.
	noCtx, noCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer noCancel()
	ev, err := p.Poll(noCtx)
	if err == nil {
		t.Fatalf("expected timeout, got events %+v", ev)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestPoller_ZeroValue(t *testing.T) {
	_, server, client := newSocketPair(t, gozmq.Pair, "inproc://poller-zero-value")

	sendCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.SendMsg(sendCtx, gozmq.NewMsgFromBytes([]byte("hi"))); err != nil {
		t.Fatalf("client send: %v", err)
	}

	var p gozmq.Poller
	p.Add(server, gozmq.PollIn)

	events := waitPoll(t, &p, 2*time.Second)
	if len(events) != 1 || events[0].Socket != server || events[0].Events&gozmq.PollIn == 0 {
		t.Fatalf("unexpected poll events: %+v", events)
	}
}

func TestPoller_PairPollOut(t *testing.T) {
	_, _, client := newSocketPair(t, gozmq.Pair, "inproc://poller-pair-out")

	p := gozmq.NewPoller()
	p.Add(client, gozmq.PollOut)

	events := waitPoll(t, p, 2*time.Second)
	if len(events) != 1 || events[0].Socket != client || events[0].Events&gozmq.PollOut == 0 {
		t.Fatalf("unexpected poll events: %+v", events)
	}
}

func TestPoller_ReqPollInOut(t *testing.T) {
	_, rep, req := newSocketPairTypes(t, gozmq.Rep, gozmq.Req, "inproc://poller-req", nil, nil)

	pOut := gozmq.NewPoller()
	pOut.Add(req, gozmq.PollOut)
	events := waitPoll(t, pOut, 2*time.Second)
	if len(events) != 1 || events[0].Socket != req || events[0].Events&gozmq.PollOut == 0 {
		t.Fatalf("unexpected PollOut events: %+v", events)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := req.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("request"))); err != nil {
		t.Fatalf("req send: %v", err)
	}
	request, err := rep.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("rep recv: %v", err)
	}
	request.Release()
	if err := rep.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("reply"))); err != nil {
		t.Fatalf("rep send: %v", err)
	}

	pIn := gozmq.NewPoller()
	pIn.Add(req, gozmq.PollIn)
	events = waitPoll(t, pIn, 2*time.Second)
	if len(events) != 1 || events[0].Socket != req || events[0].Events&gozmq.PollIn == 0 {
		t.Fatalf("unexpected PollIn events: %+v", events)
	}
}

func TestPoller_PubPollOut(t *testing.T) {
	_, _, pub := newSocketPairTypes(t, gozmq.Sub, gozmq.Pub, "inproc://poller-pub",
		[]gozmq.Option{gozmq.WithSubscribe(nil)}, nil)

	p := gozmq.NewPoller()
	p.Add(pub, gozmq.PollOut)
	events := waitPoll(t, p, 2*time.Second)
	if len(events) != 1 || events[0].Socket != pub || events[0].Events&gozmq.PollOut == 0 {
		t.Fatalf("unexpected poll events: %+v", events)
	}
}

func TestPoller_XPubPollIn(t *testing.T) {
	_, xpub, xsub := newSocketPairTypes(t, gozmq.XPub, gozmq.XSub, "inproc://poller-xpub", nil, nil)

	callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := xsub.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{1, 't'})); err != nil {
		t.Fatalf("xsub subscribe: %v", err)
	}

	p := gozmq.NewPoller()
	p.Add(xpub, gozmq.PollIn)
	events := waitPoll(t, p, 2*time.Second)
	if len(events) != 1 || events[0].Socket != xpub || events[0].Events&gozmq.PollIn == 0 {
		t.Fatalf("unexpected poll events: %+v", events)
	}
}

func TestPoller_StreamPollInOut(t *testing.T) {
	ctx := gozmq.NewContext()
	defer ctx.Close()

	stream, err := ctx.NewSocket(gozmq.Stream)
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}
	defer stream.Close()
	if err := stream.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bind stream: %v", err)
	}

	conn, err := net.DialTimeout("tcp", stream.LastEndpoint()[len("tcp://"):], 2*time.Second)
	if err != nil {
		t.Fatalf("dial stream: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatalf("write stream peer: %v", err)
	}

	p := gozmq.NewPoller()
	p.Add(stream, gozmq.PollIn|gozmq.PollOut)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pollCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		events, err := p.Poll(pollCtx)
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("poll stream: %v", err)
		}
		for _, ev := range events {
			if ev.Socket == stream && ev.Events&gozmq.PollIn != 0 {
				return
			}
		}
	}
	t.Fatalf("stream PollIn did not become ready")
}

// TestPoller_RemoveStopsReporting registers a PULL, then removes it,
// then sends a message via PUSH. Poll must not report the removed
// socket.
func TestPoller_RemoveStopsReporting(t *testing.T) {
	_, pull, push := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", nil, nil)

	p := gozmq.NewPoller()
	p.Add(pull, gozmq.PollIn)
	p.Remove(pull)

	sendCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := push.SendMsg(sendCtx, gozmq.NewMsgFromBytes([]byte("ignored"))); err != nil {
		t.Fatalf("send: %v", err)
	}

	pollCtx, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	events, err := p.Poll(pollCtx)
	if err == nil {
		t.Fatalf("removed socket still reports events: %+v", events)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	// Drain the message so test teardown is clean.
	recvCtx, cancel3 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel3()
	if _, err := pull.RecvMsg(recvCtx); err != nil {
		t.Fatalf("recv: %v", err)
	}
}

// TestPoller_OnlySecondPullReady registers two PULL sockets and
// pushes to only the second; Poll must report exactly the second.
func TestPoller_OnlySecondPullReady(t *testing.T) {
	zctx := gozmq.NewContext()
	defer zctx.Close()

	pullA, err := zctx.NewSocket(gozmq.Pull)
	if err != nil {
		t.Fatalf("new pullA: %v", err)
	}
	defer pullA.Close()
	if err := pullA.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bindA: %v", err)
	}

	pullB, err := zctx.NewSocket(gozmq.Pull)
	if err != nil {
		t.Fatalf("new pullB: %v", err)
	}
	defer pullB.Close()
	if err := pullB.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bindB: %v", err)
	}

	push, err := zctx.NewSocket(gozmq.Push)
	if err != nil {
		t.Fatalf("new push: %v", err)
	}
	defer push.Close()
	if err := push.Connect(pullB.LastEndpoint()); err != nil {
		t.Fatalf("connect: %v", err)
	}

	sendCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := push.SendMsg(sendCtx, gozmq.NewMsgFromBytes([]byte("bee"))); err != nil {
		t.Fatalf("push send: %v", err)
	}

	p := gozmq.NewPoller()
	p.Add(pullA, gozmq.PollIn)
	p.Add(pullB, gozmq.PollIn)

	events := waitPoll(t, p, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("want exactly 1 event, got %d: %+v", len(events), events)
	}
	if events[0].Socket != pullB {
		t.Fatalf("expected pullB, got different socket")
	}
	if events[0].Events&gozmq.PollIn == 0 {
		t.Fatalf("expected PollIn, got %#x", events[0].Events)
	}

	// Drain so teardown is clean.
	recvCtx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if _, err := pullB.RecvMsg(recvCtx); err != nil {
		t.Fatalf("recv: %v", err)
	}
}

// TestPoller_ContextCancel asserts Poll returns ctx.Err() when ctx
// cancels before any socket becomes ready.
func TestPoller_ContextCancel(t *testing.T) {
	zctx := gozmq.NewContext()
	defer zctx.Close()

	pull, err := zctx.NewSocket(gozmq.Pull)
	if err != nil {
		t.Fatalf("new pull: %v", err)
	}
	defer pull.Close()
	if err := pull.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bind: %v", err)
	}

	p := gozmq.NewPoller()
	p.Add(pull, gozmq.PollIn)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	events, err := p.Poll(ctx)
	if err == nil {
		t.Fatalf("expected error, got events %+v", events)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Canceled, got %v", err)
	}
}

// TestPoller_DealerRouterPollOut verifies PollOut on a DEALER once
// it has connected to a ROUTER peer.
func TestPoller_DealerRouterPollOut(t *testing.T) {
	_, _, dealer := newSocketPairTypes(t, gozmq.Router, gozmq.Dealer, "tcp://127.0.0.1:0", nil, nil)

	p := gozmq.NewPoller()
	p.Add(dealer, gozmq.PollOut)

	// DEALER does not become PollOut-ready until its session pipe is
	// attached post-handshake. waitPoll polls repeatedly within the
	// budget; once the handshake completes the LoadBalancer signals
	// readyWrite.
	events := waitPoll(t, p, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %+v", len(events), events)
	}
	if events[0].Socket != dealer {
		t.Fatalf("expected dealer socket")
	}
	if events[0].Events&gozmq.PollOut == 0 {
		t.Fatalf("expected PollOut, got %#x", events[0].Events)
	}
}
