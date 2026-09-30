package gozmq_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
)

// TestInprocCloseLingerEndsWhenConsumed: on the inproc direct-pipe path,
// Close lingers only while something we sent is still unconsumed. With the
// peer alive, a fully consumed outbound queue must not hold Close for the
// whole linger, and a queued one must release Close as soon as the peer
// reads it — not when linger expires, and not only when the peer closes.
func TestInprocCloseLingerEndsWhenConsumed(t *testing.T) {
	const (
		linger = 3 * time.Second
		n      = 50
	)
	for _, hwm := range []int{1000, 0} {
		for _, pushBinds := range []bool{false, true} {
			name := fmt.Sprintf("sendHWM%d_pushBinds=%v", hwm, pushBinds)
			setup := func(t *testing.T) (push, pull *gozmq.Socket) {
				zctx := gozmq.NewContext()
				t.Cleanup(func() { _ = zctx.Close() })
				var err error
				push, err = zctx.NewSocket(gozmq.Push, gozmq.WithSendHWM(hwm), gozmq.WithLinger(linger))
				require.NoError(t, err)
				pull, err = zctx.NewSocket(gozmq.Pull)
				require.NoError(t, err)
				t.Cleanup(func() { _ = pull.Close() })
				ep := "inproc://linger-" + t.Name()
				if pushBinds {
					require.NoError(t, push.Bind(ep))
					require.NoError(t, pull.Connect(ep))
				} else {
					require.NoError(t, pull.Bind(ep))
					require.NoError(t, push.Connect(ep))
				}
				return push, pull
			}

			t.Run(name+"/consumed", func(t *testing.T) {
				push, pull := setup(t)
				ctx := testCtx(t)
				for i := 0; i < n; i++ {
					require.NoError(t, push.SendBytes(ctx, []byte("x")))
				}
				for i := 0; i < n; i++ {
					_, err := pull.RecvBytes(ctx)
					require.NoError(t, err)
				}
				start := time.Now()
				require.NoError(t, push.Close())
				elapsed := time.Since(start)
				t.Logf("Close took %v (linger %v)", elapsed, linger)
				require.Less(t, elapsed, linger/4, "Close waited for linger with nothing queued")

				// The receiving side has nothing outbound either.
				start = time.Now()
				require.NoError(t, pull.Close())
				require.Less(t, time.Since(start), linger/4, "PULL Close waited for linger")
			})

			t.Run(name+"/queued", func(t *testing.T) {
				push, pull := setup(t)
				ctx := testCtx(t)
				for i := 0; i < n; i++ {
					require.NoError(t, push.SendBytes(ctx, []byte("x")))
				}
				const readDelay = 200 * time.Millisecond
				got := make(chan int, 1)
				go func() {
					time.Sleep(readDelay)
					k := 0
					for ; k < n; k++ {
						if _, err := pull.RecvBytes(ctx); err != nil {
							break
						}
					}
					got <- k
				}()
				start := time.Now()
				require.NoError(t, push.Close())
				elapsed := time.Since(start)
				t.Logf("Close took %v (linger %v, reader starts at %v)", elapsed, linger, readDelay)
				require.Equal(t, n, <-got, "queued messages lost")
				require.GreaterOrEqual(t, elapsed, readDelay/2, "Close did not wait for queued messages")
				require.Less(t, elapsed, linger/2, "Close waited for linger after the peer consumed everything")
			})
		}
	}
}

// TestConnectAttachFailureTearsDownSession: a Connect whose handshake
// succeeds but whose pipe the engine refuses (a PAIR that already has a
// peer) must not report EventConnected or leave the session running
// unattached; it backs off and retries like any other failed attempt.
func TestConnectAttachFailureTearsDownSession(t *testing.T) {
	zctx := gozmq.NewContext()
	defer zctx.Close()
	var eps []string
	for range 2 {
		b, err := zctx.NewSocket(gozmq.Pair)
		require.NoError(t, err)
		defer b.Close()
		require.NoError(t, b.Bind("tcp://127.0.0.1:0"))
		eps = append(eps, b.LastEndpoint())
	}
	events := make(chan gozmq.Event, 64)
	a, err := zctx.NewSocket(gozmq.Pair,
		gozmq.WithReconnectInterval(20*time.Millisecond),
		gozmq.WithMonitor(events, gozmq.EventConnected|gozmq.EventConnectDelayed|gozmq.EventConnectRetried))
	require.NoError(t, err)
	defer a.Close()

	waitEvent := func(want gozmq.EventType, ep string) {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case ev := <-events:
				if ev.Type == gozmq.EventConnected && ev.Endpoint == eps[1] {
					t.Fatalf("EventConnected for %s although the engine refused the pipe", ev.Endpoint)
				}
				if ev.Type == want && ev.Endpoint == ep {
					return
				}
			case <-deadline:
				t.Fatalf("no event %v for %s", want, ep)
			}
		}
	}
	require.NoError(t, a.Connect(eps[0]))
	waitEvent(gozmq.EventConnected, eps[0])

	// The second attempt handshakes, is refused, and retries.
	require.NoError(t, a.Connect(eps[1]))
	waitEvent(gozmq.EventConnectDelayed, eps[1])
	waitEvent(gozmq.EventConnectRetried, eps[1])
}

// TestRecvBytesMultipartWrapsSentinel: every returned error wraps a
// sentinel (errors.go), including RecvBytes' multipart refusal.
func TestRecvBytesMultipartWrapsSentinel(t *testing.T) {
	_, a, b := newSocketPair(t, gozmq.Pair, "inproc://recvbytes-multipart")
	ctx := testCtx(t)
	require.NoError(t, a.SendMsg(ctx, gozmq.NewMsgFromFrames([]byte("a"), []byte("b"))))
	_, err := b.RecvBytes(ctx)
	require.ErrorIs(t, err, gozmq.ErrFSMViolation)
}
