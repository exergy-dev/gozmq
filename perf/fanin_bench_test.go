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
	"time"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
)

const scaleMsgSize = 64

// -----------------------------------------------------------------------------
// PUSH fan-in: one PULL fed by many concurrent PUSH sockets.
// -----------------------------------------------------------------------------

func BenchmarkPushPullFanIn_Inproc(b *testing.B) {
	for _, n := range []int{2, 16, 256} {
		b.Run(fmt.Sprintf("pushers=%d", n), func(b *testing.B) {
			benchPushPullFanIn(b, n, inprocName(b, fmt.Sprintf("fanin-inproc-%d", n)))
		})
	}
}

func BenchmarkPushPullFanIn_TCP(b *testing.B) {
	for _, n := range []int{2, 16} {
		b.Run(fmt.Sprintf("pushers=%d", n), func(b *testing.B) {
			benchPushPullFanIn(b, n, freeTCPEndpoint)
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

	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	pull, err := ctxZmq.NewSocket(gozmq.Pull)
	if err != nil {
		b.Fatalf("new pull: %v", err)
	}
	defer pull.Close()
	if err := pull.Bind(endpoint); err != nil {
		b.Fatalf("bind: %v", err)
	}

	pushers := make([]*gozmq.Socket, nPush)
	defer func() {
		for _, p := range pushers {
			if p != nil {
				_ = p.Close()
			}
		}
	}()
	for i := range pushers {
		p, err := ctxZmq.NewSocket(gozmq.Push)
		if err != nil {
			b.Fatalf("new push[%d]: %v", i, err)
		}
		pushers[i] = p
		if err := p.Connect(pull.LastEndpoint()); err != nil {
			b.Fatalf("connect[%d]: %v", i, err)
		}
	}

	payload := newPayload(scaleMsgSize)

	warmCtx, warmCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer warmCancel()
	for i, p := range pushers {
		if err := p.SendMsg(warmCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
			b.Fatalf("warmup send[%d]: %v", i, err)
		}
	}
	for i := range pushers {
		m, err := pull.RecvMsg(warmCtx)
		if err != nil {
			b.Fatalf("warmup recv[%d]: %v", i, err)
		}
		m.Release()
	}

	runCtx, runCancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
		m, err := pull.RecvMsg(runCtx)
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

// -----------------------------------------------------------------------------
// ROUTER receive cost with many idle peers attached.
// -----------------------------------------------------------------------------

// BenchmarkRouterIdlePeers_Inproc round-trips 64B between one DEALER and a
// ROUTER echo while idle DEALERs are also attached to the ROUTER. The
// per-round-trip cost should not grow with the number of idle peers.
func BenchmarkRouterIdlePeers_Inproc(b *testing.B) {
	for _, n := range []int{0, 1000} {
		b.Run(fmt.Sprintf("idle=%d", n), func(b *testing.B) {
			benchRouterIdlePeers(b, n, inprocName(b, fmt.Sprintf("router-idle-%d", n)))
		})
	}
}

func benchRouterIdlePeers(b *testing.B, nIdle int, endpoint string) {
	b.Helper()
	b.StopTimer()

	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	router, err := ctxZmq.NewSocket(gozmq.Router)
	if err != nil {
		b.Fatalf("new router: %v", err)
	}
	defer router.Close()
	if err := router.Bind(endpoint); err != nil {
		b.Fatalf("bind: %v", err)
	}

	setupCtx, setupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer setupCancel()

	// Each idle DEALER sends one hello that the ROUTER consumes, proving
	// the link is attached; afterwards the DEALER stays silent.
	idle := make([]*gozmq.Socket, nIdle)
	defer func() {
		for _, d := range idle {
			if d != nil {
				_ = d.Close()
			}
		}
	}()
	hello := []byte("hello")
	for i := range idle {
		d, err := ctxZmq.NewSocket(gozmq.Dealer)
		if err != nil {
			b.Fatalf("new idle dealer[%d]: %v", i, err)
		}
		idle[i] = d
		if err := d.Connect(endpoint); err != nil {
			b.Fatalf("idle connect[%d]: %v", i, err)
		}
		if err := d.SendMsg(setupCtx, gozmq.NewMsgFromBytes(hello)); err != nil {
			b.Fatalf("idle hello[%d]: %v", i, err)
		}
	}
	for i := range idle {
		if _, err := router.RecvMsg(setupCtx); err != nil {
			b.Fatalf("router recv hello[%d]: %v", i, err)
		}
	}

	dealer, err := ctxZmq.NewSocket(gozmq.Dealer, gozmq.WithIdentity([]byte("bench-dealer")))
	if err != nil {
		b.Fatalf("new dealer: %v", err)
	}
	defer dealer.Close()
	if err := dealer.Connect(endpoint); err != nil {
		b.Fatalf("connect: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var echoWG sync.WaitGroup
	echoWG.Add(1)
	go func() {
		defer echoWG.Done()
		for {
			m, err := router.RecvMsg(runCtx)
			if err != nil {
				return
			}
			if err := router.SendMsg(runCtx, gozmq.NewMsgFromFrames(m.Frames()...)); err != nil {
				return
			}
		}
	}()

	payload := newPayload(scaleMsgSize)

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer callCancel()
	for i := 0; i < 2; i++ {
		if err := dealer.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
			b.Fatalf("warmup send: %v", err)
		}
		if _, err := dealer.RecvMsg(callCtx); err != nil {
			b.Fatalf("warmup recv: %v", err)
		}
	}

	b.SetBytes(scaleMsgSize)
	b.ResetTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if err := dealer.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
			b.Fatalf("send[%d]: %v", i, err)
		}
		if _, err := dealer.RecvMsg(callCtx); err != nil {
			b.Fatalf("recv[%d]: %v", i, err)
		}
	}

	b.StopTimer()
	cancel()
	_ = router.Close()
	echoWG.Wait()
}

// -----------------------------------------------------------------------------
// Wide PUB fanout.
// -----------------------------------------------------------------------------

// BenchmarkPubSubFanoutWide_Inproc is BenchmarkPubSubFanout_Inproc at 64
// subscribers, where per-subscriber costs in the PUB dominate.
func BenchmarkPubSubFanoutWide_Inproc(b *testing.B) {
	const nSub = 64
	b.Run(fmt.Sprintf("subs=%d", nSub), func(b *testing.B) {
		benchPubSubFanout(b, scaleMsgSize, inprocName(b, "pubsub-wide-inproc"), nSub)
	})
}

// -----------------------------------------------------------------------------
// XPUB subscription churn.
// -----------------------------------------------------------------------------

// BenchmarkXPubSubscribeChurn_Inproc times one subscription event per
// iteration: an XSUB alternately subscribes to and cancels a rotating set
// of prefixes, and a verbose XPUB receives each event in lockstep.
func BenchmarkXPubSubscribeChurn_Inproc(b *testing.B) {
	b.StopTimer()

	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	xpub, err := ctxZmq.NewSocket(gozmq.XPub, gozmq.WithXPubVerbose(true))
	if err != nil {
		b.Fatalf("new xpub: %v", err)
	}
	defer xpub.Close()
	endpoint := inprocName(b, "xpub-churn")
	if err := xpub.Bind(endpoint); err != nil {
		b.Fatalf("bind: %v", err)
	}

	xsub, err := ctxZmq.NewSocket(gozmq.XSub)
	if err != nil {
		b.Fatalf("new xsub: %v", err)
	}
	defer xsub.Close()
	if err := xsub.Connect(endpoint); err != nil {
		b.Fatalf("connect: %v", err)
	}

	const nPrefix = 64
	var frames [nPrefix][2][]byte // [prefix][0=cancel, 1=subscribe]
	for i := range frames {
		topic := fmt.Appendf(nil, "topic-%02d", i)
		frames[i][0] = append([]byte{0x00}, topic...)
		frames[i][1] = testutil.SubscribeFrame(true, topic)
	}

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer callCancel()

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

// -----------------------------------------------------------------------------
// TCP connection setup.
// -----------------------------------------------------------------------------

// BenchmarkConnectHandshake_TCP times one connection lifetime per
// iteration: create a PUSH, connect it to a bound PULL (dial plus NULL
// handshake), deliver one message and close the PUSH. The Context and the
// PULL persist across iterations.
func BenchmarkConnectHandshake_TCP(b *testing.B) {
	b.StopTimer()

	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	pull, err := ctxZmq.NewSocket(gozmq.Pull)
	if err != nil {
		b.Fatalf("new pull: %v", err)
	}
	defer pull.Close()
	if err := pull.Bind(freeTCPEndpoint); err != nil {
		b.Fatalf("bind: %v", err)
	}
	ep := pull.LastEndpoint()

	payload := newPayload(scaleMsgSize)

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer callCancel()

	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		push, err := ctxZmq.NewSocket(gozmq.Push)
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
		m, err := pull.RecvMsg(callCtx)
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
