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

// TestPoller_WakesOnInprocDelivery drives inproc senders into HWM (both
// sides at 1) while the receiver alternates a blocking Poll with a RecvMsg.
// Every delivered message must wake the parked Poll, whether its send went
// through immediately or blocked first. PAIR used to send with a raw
// channel send that never armed the receiver's ready gate, so a Poll on the
// receiving PAIR hung.
func TestPoller_WakesOnInprocDelivery(t *testing.T) {
	t.Parallel()
	hwm := []gozmq.Option{gozmq.WithSendHWM(1), gozmq.WithRecvHWM(1)}
	for _, tc := range []struct {
		name         string
		recv, send   gozmq.SocketType
		recvIsServer bool
	}{
		{"pair", gozmq.Pair, gozmq.Pair, true},
		{"push-pull", gozmq.Pull, gozmq.Push, true},
		{"pull-push-bind", gozmq.Push, gozmq.Pull, false},
		{"dealer-router", gozmq.Router, gozmq.Dealer, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, server, client := newSocketPairTypes(t, tc.recv, tc.send,
				"inproc://poll-blocked-"+tc.name, hwm, hwm)
			recv, send := server, client
			if !tc.recvIsServer {
				recv, send = client, server
			}
			const n = 200
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			sent := make(chan error, 1)
			go func() {
				for i := 0; i < n; i++ {
					if err := send.SendMsg(ctx, gozmq.NewMsgFromBytes([]byte(fmt.Sprint(i)))); err != nil {
						sent <- err
						return
					}
				}
				sent <- nil
			}()
			p := gozmq.NewPoller()
			p.Add(recv, gozmq.PollIn)
			for i := 0; i < n; i++ {
				if i%8 == 0 {
					// Let the sender run into HWM and park.
					time.Sleep(time.Millisecond)
				}
				pctx, pcancel := context.WithTimeout(ctx, 2*time.Second)
				ev, err := p.Poll(pctx)
				pcancel()
				require.NoError(t, err, "Poll %d did not wake", i)
				require.Len(t, ev, 1)
				_, err = recv.RecvMsg(ctx)
				require.NoError(t, err, "RecvMsg %d", i)
			}
			require.NoError(t, <-sent)
		})
	}
}

// TestPoller_RepSubXSub covers the engines that used to lack readiness
// signals entirely (REP, SUB, XSUB), so a Poller never reported them.
func TestPoller_RepSubXSub(t *testing.T) {
	t.Parallel()
	t.Run("rep", func(t *testing.T) {
		_, rep, req := newSocketPairTypes(t, gozmq.Rep, gozmq.Req, "inproc://poller-rep", nil, nil)
		ctx := testutil.Ctx(t)
		pIn := gozmq.NewPoller()
		pIn.Add(rep, gozmq.PollIn|gozmq.PollOut)
		go func() { _ = req.SendMsg(ctx, gozmq.NewMsgFromBytes([]byte("q"))) }()
		ev := waitPoll(t, pIn, 2*time.Second)
		require.Equal(t, gozmq.PollIn, ev[0].Events, "request pending, no reply owed yet")
		_, err := rep.RecvMsg(ctx)
		require.NoError(t, err)
		ev = waitPoll(t, pIn, 2*time.Second)
		require.Equal(t, gozmq.PollOut, ev[0].Events, "reply owed")
	})
	t.Run("sub", func(t *testing.T) {
		_, pub, sub := newPubSubPair(t, "inproc://poller-sub", nil, nil, nil)
		p := gozmq.NewPoller()
		p.Add(sub, gozmq.PollIn)
		go func() {
			time.Sleep(20 * time.Millisecond) // let Poll park first
			_ = pub.SendMsg(context.Background(), gozmq.NewMsgFromBytes([]byte("x")))
		}()
		ev := waitPoll(t, p, 2*time.Second)
		require.Equal(t, gozmq.PollIn, ev[0].Events)
	})
	t.Run("xsub", func(t *testing.T) {
		_, xpub, xsub := newSocketPairTypes(t, gozmq.XPub, gozmq.XSub, "inproc://poller-xsub", nil, nil)
		ctx := testutil.Ctx(t)
		pOut := gozmq.NewPoller()
		pOut.Add(xsub, gozmq.PollOut)
		require.Equal(t, gozmq.PollOut, waitPoll(t, pOut, 2*time.Second)[0].Events)

		require.NoError(t, xsub.SendMsg(ctx, gozmq.NewMsgFromBytes([]byte{1})))
		_, err := xpub.RecvMsg(ctx) // subscription event
		require.NoError(t, err)
		pIn := gozmq.NewPoller()
		pIn.Add(xsub, gozmq.PollIn)
		go func() {
			time.Sleep(20 * time.Millisecond)
			_ = xpub.SendMsg(context.Background(), gozmq.NewMsgFromBytes([]byte("x")))
		}()
		require.Equal(t, gozmq.PollIn, waitPoll(t, pIn, 2*time.Second)[0].Events)
	})
}

// TestPoller_RepPollInAfterSend: a Poll parked on PollIn while REP owes a
// reply must wake when Send makes the request already queued receivable.
// Nothing arrives at that point, so only REP's own state change can wake it.
func TestPoller_RepPollInAfterSend(t *testing.T) {
	t.Parallel()
	_, rep, dealer := newSocketPairTypes(t, gozmq.Rep, gozmq.Dealer, "inproc://poller-rep-send", nil, nil)
	ctx := testutil.Ctx(t)
	for _, q := range []string{"q1", "q2"} {
		require.NoError(t, dealer.SendMsg(ctx, gozmq.NewMsgFromFrames([]byte{}, []byte(q))))
	}
	m, err := rep.RecvMsg(ctx)
	require.NoError(t, err)
	m.Release()

	p := gozmq.NewPoller()
	p.Add(rep, gozmq.PollIn)
	polled := make(chan []gozmq.PollEvent, 1)
	go func() {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		ev, _ := p.Poll(pctx)
		polled <- ev
	}()
	time.Sleep(20 * time.Millisecond) // let Poll park while a reply is owed
	require.NoError(t, rep.SendMsg(ctx, gozmq.NewMsgFromBytes([]byte("r1"))))
	ev := <-polled
	require.Len(t, ev, 1, "Poll did not wake when Send made q2 receivable")
	require.Equal(t, gozmq.PollIn, ev[0].Events)
	// Drain both directions so neither Close lingers.
	for _, s := range []*gozmq.Socket{rep, dealer} {
		m, err := s.RecvMsg(ctx)
		require.NoError(t, err)
		m.Release()
	}
}

// TestPoller_MixedSocketsWakeEach parks one Poller over three engine
// kinds — PULL (FairQueue in-gate), XPUB (event queue in-gate) and an
// unpeered PUSH (LoadBalancer out-gate) — and makes each ready in turn
// while Poll is parked. Every Poll must wake for exactly that socket.
func TestPoller_MixedSocketsWakeEach(t *testing.T) {
	t.Parallel()
	zctx := gozmq.NewContext()
	t.Cleanup(func() { _ = zctx.Close() })
	newSock := func(typ gozmq.SocketType) *gozmq.Socket {
		s, err := zctx.NewSocket(typ)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	pull, xpub, push := newSock(gozmq.Pull), newSock(gozmq.XPub), newSock(gozmq.Push)
	require.NoError(t, pull.Bind("inproc://poller-mixed-pull"))
	require.NoError(t, xpub.Bind("inproc://poller-mixed-xpub"))
	require.NoError(t, push.Bind("inproc://poller-mixed-push"))
	pusher, sub, puller := newSock(gozmq.Push), newSock(gozmq.Sub), newSock(gozmq.Pull)
	require.NoError(t, pusher.Connect("inproc://poller-mixed-pull"))
	require.NoError(t, sub.Connect("inproc://poller-mixed-xpub"))

	p := gozmq.NewPoller()
	p.Add(pull, gozmq.PollIn)
	p.Add(xpub, gozmq.PollIn)
	p.Add(push, gozmq.PollOut)
	ctx := testutil.Ctx(t)
	for _, step := range []struct {
		name    string
		trigger func()
		want    *gozmq.Socket
		events  gozmq.PollEvents
		consume func()
	}{
		{"pull", func() {
			require.NoError(t, pusher.SendMsg(ctx, gozmq.NewMsgFromBytes([]byte("x"))))
		}, pull, gozmq.PollIn, func() {
			m, err := pull.RecvMsg(ctx)
			require.NoError(t, err)
			m.Release()
		}},
		{"xpub", func() {
			require.NoError(t, sub.Subscribe([]byte("t")))
		}, xpub, gozmq.PollIn, func() {
			m, err := xpub.RecvMsg(ctx)
			require.NoError(t, err)
			m.Release()
		}},
		{"push", func() {
			require.NoError(t, puller.Connect("inproc://poller-mixed-push"))
		}, push, gozmq.PollOut, func() {}},
	} {
		polled := make(chan []gozmq.PollEvent, 1)
		go func() {
			pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			ev, _ := p.Poll(pctx)
			polled <- ev
		}()
		time.Sleep(20 * time.Millisecond) // let Poll park
		step.trigger()
		ev := <-polled
		require.Len(t, ev, 1, step.name)
		require.Equal(t, step.want, ev[0].Socket, step.name)
		require.Equal(t, step.events, ev[0].Events, step.name)
		step.consume()
	}
}
