package gozmq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
)

// TestPoller_WakesOnInprocDelivery drives inproc senders into HWM (both
// sides at 1) while the receiver alternates a blocking Poll with a RecvMsg.
// Every delivered message must wake the parked Poll, whether its send went
// through immediately or blocked first. PAIR used to send with a raw
// channel send that never armed the receiver's ready gate, so a Poll on the
// receiving PAIR hung.
func TestPoller_WakesOnInprocDelivery(t *testing.T) {
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
	t.Run("rep", func(t *testing.T) {
		_, rep, req := newSocketPairTypes(t, gozmq.Rep, gozmq.Req, "inproc://poller-rep", nil, nil)
		ctx := testCtx(t)
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
		ctx := testCtx(t)
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
