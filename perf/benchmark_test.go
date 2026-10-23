// Package perf_test houses the gozmq benchmark suite. The standalone CLI
// tools that mirror libzmq's perf/ directory (local_lat, remote_lat,
// local_thr, remote_thr) live in sibling packages under perf/.
//
// The Go benchmarks here exercise the public gozmq API — they are the
// regression bar referenced in design §10.6 and the targets in §11
// (>1M msg/s inproc, >500k msg/s TCP loopback, <10µs median REQ/REP
// inproc). The benchmarks report what the implementation actually does
// today and provide the harness for tracking regressions per commit.
//
// Run all benchmarks:
//
//	go test -bench=. -benchmem -benchtime=3s ./perf/
//
// Every benchmark that moves a payload calls b.SetBytes(msgSize) so
// -benchmem reports MB/s; for the latency benchmarks ns/op is the relevant
// figure. perf/benchgate.sh compares runs by benchmark name, so names are
// stable across refactors.
//
// Shared shape: setup (Context, bind, connect, handshake proof) happens with
// the timer stopped, the timed loop runs in the benchmark goroutine, and
// any helper goroutine is joined before the function returns so goleak (via
// TestMain) stays quiet. Sockets and Contexts are closed through b.Cleanup,
// which the testing package runs after every timed invocation.
package perf_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
)

// sizeSpec is one rung of a message-size ladder: the sub-benchmark name and
// the payload length.
type sizeSpec struct {
	name string
	n    int
}

var (
	// sizes is the canonical message-size ladder. Matches libzmq's perf/
	// convention of small / medium / large.
	sizes = []sizeSpec{{"32B", 32}, {"64B", 64}, {"1KB", 1024}, {"64KB", 64 * 1024}}

	// latencySizes drops the 64B point: for a strictly alternating
	// round trip the per-iteration cost at 32B and 64B is dominated by the
	// same fixed framing overhead, so the 64B sample adds noise without
	// information.
	latencySizes = []sizeSpec{{"32B", 32}, {"1KB", 1024}, {"64KB", 64 * 1024}}

	// largeSizes is the ladder where per-frame allocation and GC cost
	// dominate TCP throughput.
	largeSizes = []sizeSpec{{"8KB", 8 << 10}, {"64KB", 64 << 10}, {"256KB", 256 << 10}, {"1MB", 1 << 20}}

	// curveSizes brackets the CURVE per-message cost: one small frame,
	// where the fixed nonce/MAC overhead dominates, and one large frame,
	// where the stream cipher does.
	curveSizes = []sizeSpec{{"64B", 64}, {"64KB", 64 << 10}}
)

const (
	// benchTimeout bounds every blocking call in a timed loop. It only
	// decides when a hung benchmark fails, never what it measures.
	benchTimeout = 5 * time.Minute
	// setupTimeout bounds handshakes, warm-ups and subscription sync.
	setupTimeout = 30 * time.Second
	// drainWait is how long a streaming benchmark waits for its receivers
	// to catch up with the sender once every message has been sent.
	drainWait = 30 * time.Second
	// benchHWM raises both high-water marks well above any reasonable b.N
	// so a PUB or XPUB does not drop during the timed phase. PUB fanout is
	// non-blocking by design (design §9); at the default HWM (1000) a
	// throughput benchmark with b.N in the hundreds of thousands would
	// start dropping long before the slowest SUB drained, skewing both
	// ns/op and MB/s.
	benchHWM = 1_000_000
)

// transport returns the endpoint a benchmark binds.
type transport func() string

// inprocEndpoint returns a unique inproc endpoint so concurrent b.Run
// invocations don't collide in the Context registry.
func inprocEndpoint() string {
	return "inproc://bench-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// tcpEndpoint returns the loopback wildcard endpoint; the bound socket's
// LastEndpoint resolves the port.
func tcpEndpoint() string { return "tcp://127.0.0.1:0" }

// forEachSize runs bench once per rung of ladder as a sub-benchmark named
// after the rung, on a fresh endpoint from tr.
func forEachSize(b *testing.B, ladder []sizeSpec, tr transport, bench func(b *testing.B, endpoint string, size int)) {
	for _, sz := range ladder {
		b.Run(sz.name, func(b *testing.B) { bench(b, tr(), sz.n) })
	}
}

// newPayload returns a size-n byte slice filled with a deterministic
// pattern so the decoder can't take a zero-copy shortcut that skews
// numbers vs. real traffic.
//
// The first byte is forced to 'A' (0x41) purely to keep payloads stable
// across historical runs. A leading 0x00/0x01 is delivered intact: the
// SUB receive path never sniffs the payload (see internal/socket/sub.go).
func newPayload(n int) []byte {
	b := make([]byte, n)
	if n > 0 {
		b[0] = 'A'
	}
	for i := 1; i < n; i++ {
		b[i] = byte(i & 0xff)
	}
	return b
}

// bound is the server side of a benchmark: a Context and one socket bound
// in it. Both are closed when the benchmark function returns, after any
// client created through connect.
type bound struct {
	b    *testing.B
	ctx  *gozmq.Context
	sock *gozmq.Socket
}

// bind creates a Context and binds a typ socket to endpoint in it.
func bind(b *testing.B, typ gozmq.SocketType, endpoint string, opts ...gozmq.Option) bound {
	b.Helper()
	ctx := gozmq.NewContext()
	b.Cleanup(func() { _ = ctx.Close() })
	s := newSocket(b, ctx, typ, opts)
	if err := s.Bind(endpoint); err != nil {
		b.Fatalf("bind %v to %s: %v", typ, endpoint, err)
	}
	return bound{b, ctx, s}
}

// connect creates a typ socket in the same Context, connected to the bound
// socket's resolved endpoint.
func (s bound) connect(typ gozmq.SocketType, opts ...gozmq.Option) *gozmq.Socket {
	s.b.Helper()
	c := newSocket(s.b, s.ctx, typ, opts)
	if err := c.Connect(s.sock.LastEndpoint()); err != nil {
		s.b.Fatalf("connect %v: %v", typ, err)
	}
	return c
}

// newSocket creates a typ socket in ctx, closed when the benchmark
// function returns.
func newSocket(b *testing.B, ctx *gozmq.Context, typ gozmq.SocketType, opts []gozmq.Option) *gozmq.Socket {
	b.Helper()
	s, err := ctx.NewSocket(typ, opts...)
	if err != nil {
		b.Fatalf("new %v: %v", typ, err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s
}

// newPair binds a serverType socket to endpoint and connects a clientType
// socket to it, both in one fresh Context.
func newPair(b *testing.B, serverType, clientType gozmq.SocketType, serverOpts, clientOpts []gozmq.Option, endpoint string) (server, client *gozmq.Socket) {
	b.Helper()
	srv := bind(b, serverType, endpoint, serverOpts...)
	return srv.sock, srv.connect(clientType, clientOpts...)
}

// benchEcho binds a serverType socket, connects a clientType socket and
// times b.N client→server→client round trips of a size-byte payload.
func benchEcho(b *testing.B, serverType, clientType gozmq.SocketType, serverOpts, clientOpts []gozmq.Option, endpoint string, size int) {
	b.Helper()
	b.StopTimer()
	server, client := newPair(b, serverType, clientType, serverOpts, clientOpts, endpoint)
	echoRoundTrips(b, server, client, size)
}

// echoRoundTrips drives b.N round trips between client and server with a
// fresh Msg per iteration. A goroutine echoes every message the server
// receives frame for frame, so a ROUTER's leading identity frame still
// routes the reply to the originating DEALER (which strips it again) and a
// REP sends the body straight back. Two untimed round trips first force
// the handshake to complete and amortize slow-joiner and TCP_NODELAY
// quirks. The timer is stopped before the echo goroutine is torn down, so
// neither setup nor teardown lands in ns/op.
func echoRoundTrips(b *testing.B, server, client *gozmq.Socket, size int) {
	b.Helper()
	b.StopTimer()

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var echoWG sync.WaitGroup
	echoWG.Add(1)
	go func() {
		defer echoWG.Done()
		for {
			m, err := server.RecvMsg(runCtx)
			if err != nil {
				return
			}
			if err := server.SendMsg(runCtx, gozmq.NewMsgFromFrames(m.Frames()...)); err != nil {
				return
			}
		}
	}()

	payload := newPayload(size)
	callCtx := testutil.Ctx(b, benchTimeout)
	for i := 0; i < 2; i++ {
		if err := client.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
			b.Fatalf("warmup send: %v", err)
		}
		if _, err := client.RecvMsg(callCtx); err != nil {
			b.Fatalf("warmup recv: %v", err)
		}
	}

	b.SetBytes(int64(size))
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := client.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
			b.Fatalf("send[%d]: %v", i, err)
		}
		if _, err := client.RecvMsg(callCtx); err != nil {
			b.Fatalf("recv[%d]: %v", i, err)
		}
	}
	b.StopTimer()

	// Cancel the echo goroutine and wait for it before the Context is torn
	// down — otherwise goleak (via gozmq.Context.Close) flags the echo.
	cancel()
	_ = server.Close()
	echoWG.Wait()
}

func benchReqRep(b *testing.B, endpoint string, size int) {
	benchEcho(b, gozmq.Rep, gozmq.Req, nil, nil, endpoint, size)
}

// benchRouterDealer gives the DEALER a known identity; the ROUTER sees
// [identity, body] and the echo returns both frames.
func benchRouterDealer(b *testing.B, endpoint string, size int) {
	benchEcho(b, gozmq.Router, gozmq.Dealer, nil,
		[]gozmq.Option{gozmq.WithIdentity([]byte("bench-dealer"))}, endpoint, size)
}

func BenchmarkReqRepRoundTrip_Inproc(b *testing.B) {
	forEachSize(b, sizes, inprocEndpoint, benchReqRep)
}
func BenchmarkReqRepRoundTrip_TCP(b *testing.B) { forEachSize(b, sizes, tcpEndpoint, benchReqRep) }

// BenchmarkReqRepThroughput_* drive the same REQ↔REP echo loop as
// BenchmarkReqRepRoundTrip_* on the latency ladder, so peak request
// throughput can be read as MB/s without inverting the latency figure.
func BenchmarkReqRepThroughput_Inproc(b *testing.B) {
	forEachSize(b, latencySizes, inprocEndpoint, benchReqRep)
}

func BenchmarkReqRepThroughput_TCP(b *testing.B) {
	forEachSize(b, latencySizes, tcpEndpoint, benchReqRep)
}

func BenchmarkRouterDealerLatency_Inproc(b *testing.B) {
	forEachSize(b, latencySizes, inprocEndpoint, benchRouterDealer)
}

func BenchmarkRouterDealerLatency_TCP(b *testing.B) {
	forEachSize(b, latencySizes, tcpEndpoint, benchRouterDealer)
}

// warmStream sends one size-byte message from sender and receives it on
// every receiver, proving the handshake and the first flight.
func warmStream(b *testing.B, sender *gozmq.Socket, size int, receivers ...*gozmq.Socket) {
	b.Helper()
	ctx := testutil.Ctx(b, setupTimeout)
	if err := sender.SendMsg(ctx, gozmq.NewMsgFromBytes(newPayload(size))); err != nil {
		b.Fatalf("warmup send: %v", err)
	}
	for i, r := range receivers {
		m, err := r.RecvMsg(ctx)
		if err != nil {
			b.Fatalf("warmup recv[%d]: %v", i, err)
		}
		m.Release()
	}
}

// benchStream times b.N sends of a size-byte payload from sender, with one
// goroutine per receiver counting (and releasing) what arrives, then waits
// up to wait for every receiver to reach b.N so the bytes reported cover
// what was actually delivered. The timer covers the sends plus the slowest
// receiver's drain. It returns the smallest per-receiver count, capped at
// b.N, so a lossy path is visible to the caller. Every link must already
// be proven attached (see warmStream and syncSubs).
func benchStream(b *testing.B, sender *gozmq.Socket, receivers []*gozmq.Socket, size int, wait time.Duration) int64 {
	b.Helper()
	b.StopTimer()

	recvCtx, recvCancel := context.WithCancel(context.Background())
	defer recvCancel()
	counts := make([]atomic.Int64, len(receivers))
	var recvWG sync.WaitGroup
	for i, r := range receivers {
		recvWG.Add(1)
		go func() {
			defer recvWG.Done()
			for {
				m, err := r.RecvMsg(recvCtx)
				if err != nil {
					return
				}
				m.Release() // a no-op unless the receive buffer pool is on
				counts[i].Add(1)
			}
		}()
	}
	delivered := func() int64 {
		n := int64(b.N)
		for i := range counts {
			n = min(n, counts[i].Load())
		}
		return n
	}

	payload := newPayload(size)
	sendCtx := testutil.Ctx(b, benchTimeout)

	b.SetBytes(int64(size))
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := sender.SendMsg(sendCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
			b.Fatalf("send[%d]: %v", i, err)
		}
	}
	deadline := time.Now().Add(wait)
	for delivered() < int64(b.N) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	b.StopTimer()

	recvCancel()
	for _, r := range receivers {
		_ = r.Close()
	}
	recvWG.Wait()
	return delivered()
}

// syncSubs proves that pub delivers to every sub before the timed phase,
// without a fixed slow-joiner sleep: it publishes sequence-numbered probes
// until each sub has received one, then drains each sub up to the final
// probe. PUB-side filtering drops the probes published before a
// subscription propagated; the pipe is FIFO, so once a sub has one probe it
// also has every later one, and draining to the last sequence number
// leaves its queue empty for the benchmark payloads.
func syncSubs(b *testing.B, pub *gozmq.Socket, subs ...*gozmq.Socket) {
	b.Helper()
	ctx := testutil.Ctx(b, setupTimeout)
	probe := func(i int) []byte { return fmt.Appendf(nil, "gozmq-bench-sync-probe\x00%d", i) }

	got := make([][]byte, len(subs))  // the latest probe each sub received
	pending := make([]int, len(subs)) // subs that have not received one yet
	for i := range pending {
		pending[i] = i
	}
	last := -1
	for len(pending) > 0 {
		last++
		if err := pub.SendMsg(ctx, gozmq.NewMsgFromBytes(probe(last))); err != nil {
			b.Fatalf("sync probe send %d: %v", last, err)
		}
		pending = slices.DeleteFunc(pending, func(i int) bool {
			tryCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
			defer cancel()
			m, err := subs[i].RecvMsg(tryCtx)
			if err != nil {
				if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
					b.Fatalf("sync probe recv sub[%d]: %v", i, err)
				}
				return false
			}
			got[i] = bytes.Clone(m.Bytes())
			m.Release()
			return true
		})
	}

	want := probe(last)
	for i, s := range subs {
		for !bytes.Equal(got[i], want) {
			m, err := s.RecvMsg(ctx)
			if err != nil {
				b.Fatalf("sync drain sub[%d]: %v", i, err)
			}
			got[i] = bytes.Clone(m.Bytes())
			m.Release()
		}
	}
}

// benchPushPullThroughput streams b.N size-byte messages PUSH→PULL. The
// PULL is bound with pullOpts and the PUSH connected with pushOpts; one
// untimed message proves the handshake. Both HWMs default to 1000, so the
// producer may run that far ahead of the consumer: the timer covers the
// steady-state throughput, not the startup.
func benchPushPullThroughput(b *testing.B, endpoint string, size int, pullOpts, pushOpts []gozmq.Option) {
	b.Helper()
	b.StopTimer()
	pull, push := newPair(b, gozmq.Pull, gozmq.Push, pullOpts, pushOpts, endpoint)
	warmStream(b, push, size, pull)
	benchStream(b, push, []*gozmq.Socket{pull}, size, drainWait)
}

func benchPushPull(b *testing.B, endpoint string, size int) {
	benchPushPullThroughput(b, endpoint, size, nil, nil)
}

func BenchmarkPushPullThroughput_Inproc(b *testing.B) {
	forEachSize(b, sizes, inprocEndpoint, benchPushPull)
}

func BenchmarkPushPullThroughput_TCP(b *testing.B) { forEachSize(b, sizes, tcpEndpoint, benchPushPull) }

// BenchmarkPushPullLarge_TCP measures large-frame TCP throughput with the
// receive buffer pool off (plain memory, the default) and on, where the
// per-frame allocation and GC cost of plain memory dominates. The PULL
// side releases each message, which returns its buffer to the pool.
func BenchmarkPushPullLarge_TCP(b *testing.B) {
	for _, pooled := range []bool{false, true} {
		b.Run(fmt.Sprintf("pool=%v", pooled), func(b *testing.B) {
			forEachSize(b, largeSizes, tcpEndpoint, func(b *testing.B, endpoint string, size int) {
				benchPushPullThroughput(b, endpoint, size, []gozmq.Option{gozmq.WithRecvBufferPool(pooled)}, nil)
			})
		})
	}
}

// BenchmarkPushPullCurve_TCP is BenchmarkPushPullThroughput_TCP with CURVE
// on both ends, so the per-message encrypt/decrypt path is measurable
// against its NULL sibling (same MB/s and allocs/op reporting).
func BenchmarkPushPullCurve_TCP(b *testing.B) {
	serverPub, serverSec := testutil.CurveKeys(b)
	clientPub, clientSec := testutil.CurveKeys(b)
	forEachSize(b, curveSizes, tcpEndpoint, func(b *testing.B, endpoint string, size int) {
		benchPushPullThroughput(b, endpoint, size,
			[]gozmq.Option{gozmq.WithCurveServer(serverSec[:])},
			[]gozmq.Option{gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:])})
	})
}

// BenchmarkPubSubFanout_Inproc measures throughput of a 1-PUB → 3-SUB fanout,
// all subscribed to the same topic. Each b.N iteration sends one message;
// the timer covers the PUB send plus the slowest SUB receive.
func BenchmarkPubSubFanout_Inproc(b *testing.B) {
	forEachSize(b, sizes, inprocEndpoint, func(b *testing.B, endpoint string, size int) {
		benchPubSubFanout(b, endpoint, size, 3)
	})
}

// benchPubSubFanout streams b.N size-byte messages from one PUB to nSub
// SUBs. HWM is raised on every socket (benchHWM); if the SUBs still lag by
// the drain deadline the timer stops anyway and the smallest per-SUB count
// is reported as msgs-delivered — better than a timeout-sized hang on the
// honest-but-lossy PUB path.
func benchPubSubFanout(b *testing.B, endpoint string, size, nSub int) {
	b.Helper()
	b.StopTimer()

	pub := bind(b, gozmq.Pub, endpoint, gozmq.WithSendHWM(benchHWM), gozmq.WithRecvHWM(benchHWM))
	subs := make([]*gozmq.Socket, nSub)
	for i := range subs {
		// Empty prefix = subscribe-to-all; matches the benchmark's
		// "everyone gets everything" fanout intent.
		subs[i] = pub.connect(gozmq.Sub,
			gozmq.WithSendHWM(benchHWM), gozmq.WithRecvHWM(benchHWM), gozmq.WithSubscribe([]byte{}))
	}
	syncSubs(b, pub.sock, subs...)

	delivered := benchStream(b, pub.sock, subs, size, 3*time.Second)
	b.ReportMetric(float64(delivered), "msgs-delivered")
}

// subscribeAll subscribes xsub to every topic via the synthetic 0x01 frame,
// consumes the subscription that the XPUB surfaces on its receive path so
// it does not sit in the XPUB queue during the timed phase, and proves the
// filter is live on the XPUB's pipe.
func subscribeAll(b *testing.B, xpub, xsub *gozmq.Socket) {
	b.Helper()
	ctx := testutil.Ctx(b, setupTimeout)
	if err := xsub.SendMsg(ctx, gozmq.NewMsgFromBytes(testutil.SubscribeFrame(true, nil))); err != nil {
		b.Fatalf("xsub subscribe: %v", err)
	}
	if _, err := xpub.RecvMsg(ctx); err != nil {
		b.Fatalf("xpub recv subscription: %v", err)
	}
	syncSubs(b, xpub, xsub)
}

// BenchmarkXPubXSubLatency_Inproc measures one-shot XPUB→XSUB delivery on
// an empty-topic subscription. Each iteration sends one message and
// receives one — the ns/op number is XPUB send + slow-joiner-free XSUB
// recv on inproc.
func BenchmarkXPubXSubLatency_Inproc(b *testing.B) {
	forEachSize(b, latencySizes, inprocEndpoint, benchXPubXSubLatency)
}

func benchXPubXSubLatency(b *testing.B, endpoint string, size int) {
	b.Helper()
	b.StopTimer()

	xpub, xsub := newPair(b, gozmq.XPub, gozmq.XSub, nil, nil, endpoint)
	subscribeAll(b, xpub, xsub)

	payload := newPayload(size)
	callCtx := testutil.Ctx(b, benchTimeout)

	b.SetBytes(int64(size))
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := xpub.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
			b.Fatalf("send[%d]: %v", i, err)
		}
		if _, err := xsub.RecvMsg(callCtx); err != nil {
			b.Fatalf("recv[%d]: %v", i, err)
		}
	}
	b.StopTimer()
}

// BenchmarkXPubXSubThroughput_Inproc streams b.N messages from XPUB to a
// single XSUB receiver and waits for the drain — measures msg/s + MB/s,
// not per-iteration latency.
func BenchmarkXPubXSubThroughput_Inproc(b *testing.B) {
	forEachSize(b, sizes, inprocEndpoint, benchXPubXSubThroughput)
}

func benchXPubXSubThroughput(b *testing.B, endpoint string, size int) {
	b.Helper()
	b.StopTimer()

	hwm := []gozmq.Option{gozmq.WithSendHWM(benchHWM), gozmq.WithRecvHWM(benchHWM)}
	xpub, xsub := newPair(b, gozmq.XPub, gozmq.XSub, hwm, hwm, endpoint)
	subscribeAll(b, xpub, xsub)
	warmStream(b, xpub, size, xsub)
	benchStream(b, xpub, []*gozmq.Socket{xsub}, size, drainWait)
}
