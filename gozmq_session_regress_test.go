package gozmq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/exergy-dev/gozmq"
)

// TestLingerDrainsUnboundedQueue: Socket.Close with a positive linger must
// flush the whole outbound queue, including an HWM-0 (unbounded) one whose
// overflow spills past the pipe's channel. The receiving
// side runs with an idle second PUSH peer so its FairQueue is in
// multi-peer (forwarder) mode, which drains a detached pipe to EOF.
func TestLingerDrainsUnboundedQueue(t *testing.T) {
	for _, tc := range []struct{ sendHWM, recvHWM int }{
		{1000, 1000},
		{0, 1000},
		{1000, 0},
		{0, 0},
	} {
		t.Run(fmt.Sprintf("send%d_recv%d", tc.sendHWM, tc.recvHWM), func(t *testing.T) {
			zctx := gozmq.NewContext()
			defer zctx.Close()
			pull, err := zctx.NewSocket(gozmq.Pull, gozmq.WithRecvHWM(tc.recvHWM))
			require.NoError(t, err)
			defer pull.Close()
			require.NoError(t, pull.Bind("tcp://127.0.0.1:0"))
			ep := pull.LastEndpoint()
			idle, err := zctx.NewSocket(gozmq.Push) // forces FairQueue multi-peer mode
			require.NoError(t, err)
			defer idle.Close()
			require.NoError(t, idle.Connect(ep))
			push, err := zctx.NewSocket(gozmq.Push,
				gozmq.WithSendHWM(tc.sendHWM), gozmq.WithLinger(5*time.Second))
			require.NoError(t, err)
			require.NoError(t, push.Connect(ep))

			c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			require.NoError(t, push.SendBytes(c, []byte("warmup")))
			_, err = pull.RecvBytes(c)
			require.NoError(t, err)

			got := make(chan int, 1)
			go func() {
				k := 0
				for {
					rc, rcancel := context.WithTimeout(context.Background(), time.Second)
					_, err := pull.RecvBytes(rc)
					rcancel()
					if err != nil {
						got <- k
						return
					}
					k++
				}
			}()
			const n = 20000
			for i := 0; i < n; i++ {
				require.NoError(t, push.SendBytes(c, []byte("x")))
			}
			require.NoError(t, push.Close())
			require.Equal(t, n, <-got, "messages delivered after linger close")
		})
	}
}

// TestRecvHWM0NoLeak: an HWM-0 inbound pipe whose session has ended with
// messages still queued must not keep any goroutine alive past
// Socket.Close, on either the bind (accepted session) or the connect
// (reconnector) side.
func TestRecvHWM0NoLeak(t *testing.T) {
	for _, pullBinds := range []bool{true, false} {
		t.Run(fmt.Sprintf("pullBinds=%v", pullBinds), func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			zctx := gozmq.NewContext()
			pull, err := zctx.NewSocket(gozmq.Pull, gozmq.WithRecvHWM(0))
			require.NoError(t, err)
			push, err := zctx.NewSocket(gozmq.Push)
			require.NoError(t, err)
			if pullBinds {
				require.NoError(t, pull.Bind("tcp://127.0.0.1:0"))
				require.NoError(t, push.Connect(pull.LastEndpoint()))
			} else {
				require.NoError(t, push.Bind("tcp://127.0.0.1:0"))
				require.NoError(t, pull.Connect(push.LastEndpoint()))
			}
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for i := 0; i < 100; i++ {
				require.NoError(t, push.SendBytes(c, []byte("x")))
			}
			// Let the messages reach pull's pipe, then end the session
			// from the push side while they are still unread.
			time.Sleep(100 * time.Millisecond)
			require.NoError(t, push.Close())
			time.Sleep(100 * time.Millisecond)
			require.NoError(t, pull.Close())
			require.NoError(t, zctx.Close())
		})
	}
}
