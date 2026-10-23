package perf_test

// Scalability benchmarks: many-peer fan-in, receive cost with idle peers,
// wide PUB fanout, XPUB subscription churn and per-connection setup cost.
// They are the regression bar for removing per-pipe goroutines, so each
// one keeps its setup (connects, handshakes) outside the timed region.

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
)

const scaleMsgSize = 64

func BenchmarkPushPullFanIn_Inproc(b *testing.B) {
	for _, n := range []int{2, 16, 256} {
		b.Run(fmt.Sprintf("pushers=%d", n), func(b *testing.B) {
			benchPushPullFanIn(b, n, inprocEndpoint())
		})
	}
}

func BenchmarkPushPullFanIn_TCP(b *testing.B) {
	for _, n := range []int{2, 16} {
		b.Run(fmt.Sprintf("pushers=%d", n), func(b *testing.B) {
			benchPushPullFanIn(b, n, tcpEndpoint())
		})
	}
}

// benchPushPullFanIn splits b.N messages across nPush PUSH sockets that
// send concurrently, and times their receipt at the single PULL in the
// benchmark goroutine. A warm-up message from every pusher proves each
// link is attached before the timer starts.
func benchPushPullFanIn(b *testing.B, nPush int, endpoint string) {
	b.Helper()
	b.StopTimer()

	pull := bind(b, gozmq.Pull, endpoint)
	pushers := make([]*gozmq.Socket, nPush)
	for i := range pushers {
		pushers[i] = pull.connect(gozmq.Push)
	}

	payload := newPayload(scaleMsgSize)

	warmCtx := testutil.Ctx(b, setupTimeout)
	for i, p := range pushers {
		if err := p.SendMsg(warmCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
			b.Fatalf("warmup send[%d]: %v", i, err)
		}
	}
	for i := range pushers {
		m, err := pull.sock.RecvMsg(warmCtx)
		if err != nil {
			b.Fatalf("warmup recv[%d]: %v", i, err)
		}
		m.Release()
	}

	runCtx, runCancel := context.WithTimeout(context.Background(), benchTimeout)
	defer runCancel()

	start := make(chan struct{})
	errCh := make(chan error, nPush)
	var sendWG sync.WaitGroup
	for i, p := range pushers {
		count := b.N / nPush
		if i < b.N%nPush {
			count++
		}
		sendWG.Add(1)
		go func() {
			defer sendWG.Done()
			<-start
			for j := 0; j < count; j++ {
				if err := p.SendMsg(runCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
					errCh <- fmt.Errorf("pusher %d send[%d]: %w", i, j, err)
					runCancel() // unblock the receiver
					return
				}
			}
		}()
	}

	b.SetBytes(scaleMsgSize)
	b.ResetTimer()
	b.StartTimer()
	close(start)

	for i := 0; i < b.N; i++ {
		m, err := pull.sock.RecvMsg(runCtx)
		if err != nil {
			b.StopTimer()
			runCancel()
			sendWG.Wait()
			select {
			case sendErr := <-errCh:
				b.Fatalf("recv[%d]: %v (%v)", i, err, sendErr)
			default:
				b.Fatalf("recv[%d]: %v", i, err)
			}
		}
		m.Release()
	}

	b.StopTimer()
	sendWG.Wait()
}

// BenchmarkRouterIdlePeers_Inproc round-trips 64B between one DEALER and a
// ROUTER echo while idle DEALERs are also attached to the ROUTER. The
// per-round-trip cost should not grow with the number of idle peers.
func BenchmarkRouterIdlePeers_Inproc(b *testing.B) {
	for _, n := range []int{0, 1000} {
		b.Run(fmt.Sprintf("idle=%d", n), func(b *testing.B) {
			benchRouterIdlePeers(b, n, inprocEndpoint())
		})
	}
}

// benchRouterIdlePeers attaches nIdle DEALERs to the ROUTER before the
// active one connects. Each idle DEALER sends one hello that the ROUTER
// consumes, proving the link is attached; afterwards the DEALER stays
// silent.
func benchRouterIdlePeers(b *testing.B, nIdle int, endpoint string) {
	b.Helper()
	b.StopTimer()

	router := bind(b, gozmq.Router, endpoint)

	setupCtx := testutil.Ctx(b, setupTimeout)
	hello := []byte("hello")
	for i := 0; i < nIdle; i++ {
		d := router.connect(gozmq.Dealer)
		if err := d.SendMsg(setupCtx, gozmq.NewMsgFromBytes(hello)); err != nil {
			b.Fatalf("idle hello[%d]: %v", i, err)
		}
	}
	for i := 0; i < nIdle; i++ {
		if _, err := router.sock.RecvMsg(setupCtx); err != nil {
			b.Fatalf("router recv hello[%d]: %v", i, err)
		}
	}

	dealer := router.connect(gozmq.Dealer, gozmq.WithIdentity([]byte("bench-dealer")))
	echoRoundTrips(b, router.sock, dealer, scaleMsgSize)
}

// BenchmarkPubSubFanoutWide_Inproc is BenchmarkPubSubFanout_Inproc at 64
// subscribers, where per-subscriber costs in the PUB dominate.
func BenchmarkPubSubFanoutWide_Inproc(b *testing.B) {
	const nSub = 64
	b.Run(fmt.Sprintf("subs=%d", nSub), func(b *testing.B) {
		benchPubSubFanout(b, inprocEndpoint(), scaleMsgSize, nSub)
	})
}

// BenchmarkXPubSubscribeChurn_Inproc times one subscription event per
// iteration: an XSUB alternately subscribes to and cancels a rotating set
// of prefixes, and a verbose XPUB receives each event in lockstep.
func BenchmarkXPubSubscribeChurn_Inproc(b *testing.B) {
	b.StopTimer()

	xpub, xsub := newPair(b, gozmq.XPub, gozmq.XSub,
		[]gozmq.Option{gozmq.WithXPubVerbose(true)}, nil, inprocEndpoint())

	const nPrefix = 64
	var frames [nPrefix][2][]byte // [prefix][0=cancel, 1=subscribe]
	for i := range frames {
		topic := fmt.Appendf(nil, "topic-%02d", i)
		frames[i][0] = testutil.SubscribeFrame(false, topic)
		frames[i][1] = testutil.SubscribeFrame(true, topic)
	}

	callCtx := testutil.Ctx(b, benchTimeout)

	// event sends the i-th churn event and receives it on the XPUB.
	event := func(i int) error {
		f := frames[(i/2)%nPrefix][1-i%2]
		if err := xsub.SendMsg(callCtx, gozmq.NewMsgFromBytes(f)); err != nil {
			return fmt.Errorf("xsub send: %w", err)
		}
		m, err := xpub.RecvMsg(callCtx)
		if err != nil {
			return fmt.Errorf("xpub recv: %w", err)
		}
		if got := m.Bytes(); len(got) != len(f) || got[0] != f[0] {
			return fmt.Errorf("xpub recv %q, want %q", got, f)
		}
		m.Release()
		return nil
	}

	// Warm-up: one subscribe/cancel pair proves the link is attached.
	for i := 0; i < 2; i++ {
		if err := event(i); err != nil {
			b.Fatalf("warmup: %v", err)
		}
	}

	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := event(i); err != nil {
			b.Fatalf("event[%d]: %v", i, err)
		}
	}
	b.StopTimer()
}

// BenchmarkConnectHandshake_TCP times one connection lifetime per
// iteration: create a PUSH, connect it to a bound PULL (dial plus NULL
// handshake), deliver one message and close the PUSH. The Context and the
// PULL persist across iterations.
func BenchmarkConnectHandshake_TCP(b *testing.B) {
	b.StopTimer()

	pull := bind(b, gozmq.Pull, tcpEndpoint())
	ep := pull.sock.LastEndpoint()

	payload := newPayload(scaleMsgSize)
	callCtx := testutil.Ctx(b, benchTimeout)

	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		push, err := pull.ctx.NewSocket(gozmq.Push)
		if err != nil {
			b.Fatalf("new push[%d]: %v", i, err)
		}
		if err := push.Connect(ep); err != nil {
			_ = push.Close()
			b.Fatalf("connect[%d]: %v", i, err)
		}
		if err := push.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
			_ = push.Close()
			b.Fatalf("send[%d]: %v", i, err)
		}
		m, err := pull.sock.RecvMsg(callCtx)
		if err != nil {
			_ = push.Close()
			b.Fatalf("recv[%d]: %v", i, err)
		}
		m.Release()
		if err := push.Close(); err != nil {
			b.Fatalf("close[%d]: %v", i, err)
		}
	}
	b.StopTimer()
}
