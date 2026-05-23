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
// Each throughput benchmark calls b.SetBytes(msgSize) so -benchmem reports
// MB/s. Latency benchmarks do not SetBytes — ns/op is the relevant figure.
package perf_test

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rkm/gozmq"
)

// sizes is the canonical message-size ladder for every benchmark. Matches
// libzmq's perf/ convention of small / medium / large.
var sizes = []struct {
	name string
	n    int
}{
	{"32B", 32},
	{"64B", 64},
	{"1KB", 1024},
	{"64KB", 64 * 1024},
}

// freeTCPEndpoint returns "tcp://127.0.0.1:0"; callers bind and read back
// LastEndpoint() to obtain the resolved port.
const freeTCPEndpoint = "tcp://127.0.0.1:0"

// Matches internal/session.largeFrameThreshold. Throughput receivers discard
// messages immediately, so large receives can synchronously return their
// pool-backed frame buffers via Msg.Reset without adding work to small-frame
// benchmarks.
const largeFrameResetThreshold = 4 * 1024

func shouldResetReceivedLargeFrames(endpoint string, msgSize int) bool {
	if msgSize <= largeFrameResetThreshold {
		return false
	}
	return !strings.HasPrefix(endpoint, "inproc://")
}

// inprocName returns a unique inproc endpoint per benchmark sub-test so
// concurrent b.Run invocations don't collide in the Context registry.
func inprocName(b *testing.B, tag string) string {
	b.Helper()
	return "inproc://bench-" + tag + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// newPayload returns a size-n byte slice filled with a deterministic
// pattern so the decoder can't take a zero-copy shortcut that skews
// numbers vs. real traffic.
//
// The first byte is forced to 'A' (0x41) because the SUB engine currently
// treats a leading 0x00 / 0x01 byte as a synthetic SUBSCRIBE/CANCEL
// command and drops the message before returning from Recv (see
// internal/socket/sub.go — isSubscriptionMsg). Until that
// discrimination to look at a COMMAND flag instead of data content, any
// pub/sub benchmark payload must avoid those two bytes in position 0.
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

// -----------------------------------------------------------------------------
// REQ / REP round-trip latency.
// -----------------------------------------------------------------------------

func BenchmarkReqRepRoundTrip_Inproc(b *testing.B) {
	for _, sz := range sizes {
		sz := sz
		b.Run(sz.name, func(b *testing.B) {
			benchReqRepRoundTrip(b, sz.n, inprocName(b, "reqrep-inproc-"+sz.name))
		})
	}
}

func BenchmarkReqRepRoundTrip_TCP(b *testing.B) {
	for _, sz := range sizes {
		sz := sz
		b.Run(sz.name, func(b *testing.B) {
			benchReqRepRoundTrip(b, sz.n, freeTCPEndpoint)
		})
	}
}

// benchReqRepRoundTrip binds a REP + connects a REQ on endpoint, warms the
// handshake, then drives b.N round trips with a fresh Msg per iteration.
// b.StopTimer/StartTimer is used so setup/teardown never land in ns/op.
func benchReqRepRoundTrip(b *testing.B, msgSize int, endpoint string) {
	b.Helper()
	b.StopTimer()

	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	rep, err := ctxZmq.NewSocket(gozmq.Rep)
	if err != nil {
		b.Fatalf("new rep: %v", err)
	}
	defer rep.Close()
	if err := rep.Bind(endpoint); err != nil {
		b.Fatalf("bind: %v", err)
	}
	ep := rep.LastEndpoint()

	req, err := ctxZmq.NewSocket(gozmq.Req)
	if err != nil {
		b.Fatalf("new req: %v", err)
	}
	defer req.Close()
	if err := req.Connect(ep); err != nil {
		b.Fatalf("connect: %v", err)
	}

	// Echo goroutine: the REP side blocks in Recv, sends the body back.
	// A single ctx cancels cleanly on benchmark exit.
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var echoWG sync.WaitGroup
	echoWG.Add(1)
	go func() {
		defer echoWG.Done()
		for {
			m, err := rep.RecvMsg(runCtx)
			if err != nil {
				return
			}
			if err := rep.SendMsg(runCtx, gozmq.NewMsgFromBytes(m.Bytes())); err != nil {
				return
			}
		}
	}()

	payload := newPayload(msgSize)

	// Warm up: force the handshake to complete and the first echo to
	// settle before we start timing. Two round trips is enough to amortize
	// slow-joiner and TCP_NODELAY quirks.
	callCtx, callCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer callCancel()
	for i := 0; i < 2; i++ {
		if err := req.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
			b.Fatalf("warmup send: %v", err)
		}
		if _, err := req.RecvMsg(callCtx); err != nil {
			b.Fatalf("warmup recv: %v", err)
		}
	}

	b.SetBytes(int64(msgSize))
	b.ResetTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if err := req.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
			b.Fatalf("send[%d]: %v", i, err)
		}
		if _, err := req.RecvMsg(callCtx); err != nil {
			b.Fatalf("recv[%d]: %v", i, err)
		}
	}

	b.StopTimer()
	// Cancel the echo goroutine and wait for exit before we tear down the
	// context — otherwise goleak (via gozmq.Context.Close) flags the echo.
	cancel()
	_ = rep.Close()
	echoWG.Wait()
}

// -----------------------------------------------------------------------------
// PUSH / PULL throughput.
// -----------------------------------------------------------------------------

func BenchmarkPushPullThroughput_Inproc(b *testing.B) {
	for _, sz := range sizes {
		sz := sz
		b.Run(sz.name, func(b *testing.B) {
			benchPushPullThroughput(b, sz.n, inprocName(b, "pushpull-inproc-"+sz.name))
		})
	}
}

func BenchmarkPushPullThroughput_TCP(b *testing.B) {
	for _, sz := range sizes {
		sz := sz
		b.Run(sz.name, func(b *testing.B) {
			benchPushPullThroughput(b, sz.n, freeTCPEndpoint)
		})
	}
}

// benchPushPullThroughput times b.N PUSH→PULL deliveries of a size-msgSize
// payload. The PUSH side runs in the benchmark goroutine; a helper runs
// the PULL side. Because HWM on both ends defaults to 1000, we let the
// producer race ahead by that much before the consumer starts receiving —
// the timer covers the steady-state throughput, not the startup.
func benchPushPullThroughput(b *testing.B, msgSize int, endpoint string) {
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

	push, err := ctxZmq.NewSocket(gozmq.Push)
	if err != nil {
		b.Fatalf("new push: %v", err)
	}
	defer push.Close()
	if err := push.Connect(pull.LastEndpoint()); err != nil {
		b.Fatalf("connect: %v", err)
	}

	payload := newPayload(msgSize)
	resetLarge := shouldResetReceivedLargeFrames(endpoint, msgSize)

	// Warm up the handshake + first flight.
	warmCtx, warmCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := push.SendMsg(warmCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
		warmCancel()
		b.Fatalf("warmup send: %v", err)
	}
	if m, err := pull.RecvMsg(warmCtx); err != nil {
		warmCancel()
		b.Fatalf("warmup recv: %v", err)
	} else if resetLarge {
		// The throughput receiver discards payloads after accounting for
		// delivery, so return large pool-backed buffers synchronously.
		m.Reset()
	}
	warmCancel()

	recvCtx, recvCancel := context.WithCancel(context.Background())
	defer recvCancel()

	var received atomic.Int64
	var recvWG sync.WaitGroup
	recvWG.Add(1)
	go func() {
		defer recvWG.Done()
		for {
			m, err := pull.RecvMsg(recvCtx)
			if err != nil {
				return
			}
			if resetLarge {
				m.Reset()
			}
			received.Add(1)
		}
	}()

	sendCtx, sendCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer sendCancel()

	b.SetBytes(int64(msgSize))
	b.ResetTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if err := push.SendMsg(sendCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
			b.Fatalf("send[%d]: %v", i, err)
		}
	}

	// Drain: wait until every sent message is observed by the PULL side
	// so b.N × msgSize accurately reflects the bytes measured.
	deadline := time.Now().Add(30 * time.Second)
	for received.Load() < int64(b.N) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	b.StopTimer()

	recvCancel()
	_ = pull.Close()
	recvWG.Wait()
}

// -----------------------------------------------------------------------------
// PUB / SUB fanout.
// -----------------------------------------------------------------------------

// BenchmarkPubSubFanout_Inproc measures throughput of a 1-PUB → 3-SUB fanout,
// all subscribed to the same topic. Each b.N iteration sends one message;
// the timer covers the PUB send plus the slowest SUB receive.
func BenchmarkPubSubFanout_Inproc(b *testing.B) {
	for _, sz := range sizes {
		sz := sz
		b.Run(sz.name, func(b *testing.B) {
			benchPubSubFanout(b, sz.n, inprocName(b, "pubsub-inproc-"+sz.name), 3)
		})
	}
}

func benchPubSubFanout(b *testing.B, msgSize int, endpoint string, nSub int) {
	b.Helper()
	b.StopTimer()

	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	// Raise HWM well above any reasonable b.N so PUB doesn't drop
	// messages during the timed phase. PUB fanout is non-blocking by
	// design (design §9); at default HWM (1000) a throughput benchmark
	// with b.N in the hundreds of thousands would start dropping long
	// before the slowest SUB drained, skewing both ns/op and MB/s.
	const benchHWM = 1_000_000

	pub, err := ctxZmq.NewSocket(gozmq.Pub,
		gozmq.WithSendHWM(benchHWM), gozmq.WithRecvHWM(benchHWM))
	if err != nil {
		b.Fatalf("new pub: %v", err)
	}
	defer pub.Close()
	if err := pub.Bind(endpoint); err != nil {
		b.Fatalf("bind: %v", err)
	}

	subs := make([]*gozmq.Socket, nSub)
	for i := range subs {
		s, err := ctxZmq.NewSocket(gozmq.Sub,
			gozmq.WithSendHWM(benchHWM), gozmq.WithRecvHWM(benchHWM))
		if err != nil {
			b.Fatalf("new sub[%d]: %v", i, err)
		}
		// Empty prefix = subscribe-to-all; matches the benchmark's
		// "everyone gets everything" fanout intent.
		if err := s.Subscribe([]byte("")); err != nil {
			b.Fatalf("sub subscribe[%d]: %v", i, err)
		}
		if err := s.Connect(pub.LastEndpoint()); err != nil {
			b.Fatalf("sub connect[%d]: %v", i, err)
		}
		subs[i] = s
	}
	defer func() {
		for _, s := range subs {
			_ = s.Close()
		}
	}()

	// Slow-joiner: give SUBSCRIBE commands a window to propagate back to
	// the PUB before we start publishing. design §10.6 / perf/README.md
	// call this out explicitly.
	time.Sleep(200 * time.Millisecond)

	payload := newPayload(msgSize)
	resetLarge := shouldResetReceivedLargeFrames(endpoint, msgSize)

	// Hard warm-up: alternate pub-sends and short sub-drains until every
	// sub has seen at least one message. The 200ms sleep above is a best-
	// effort heuristic; PUB drops messages for sub pipes that haven't
	// completed handshake + SUBSCRIBE forwarding yet, so a b.N=1 timed
	// loop would race the setup.
	warmCtx, warmCancel := context.WithTimeout(context.Background(), 15*time.Second)
	warmDeadline := time.Now().Add(10 * time.Second)
	drained := make([]bool, nSub)
	allDrained := false
	for !allDrained {
		// Publish a burst so whichever peers are now attached get at
		// least one message, and any still-joining peers catch up once
		// their pipe is attached.
		for j := 0; j < 16; j++ {
			if err := pub.SendMsg(warmCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
				warmCancel()
				b.Fatalf("warmup send: %v", err)
			}
		}
		allDrained = true
		for i, s := range subs {
			ctxShort, cancelShort := context.WithTimeout(warmCtx, 100*time.Millisecond)
			if m, err := s.RecvMsg(ctxShort); err == nil {
				if resetLarge {
					m.Reset()
				}
				drained[i] = true
			} else if !drained[i] {
				allDrained = false
			}
			cancelShort()
		}
		if time.Now().After(warmDeadline) {
			warmCancel()
			b.Fatalf("subscribers did not drain warmup in 10s (drained=%v)", drained)
		}
	}
	// Drain any stragglers on each sub so they don't leak into the timed
	// phase.
	for _, s := range subs {
		for {
			ctxShort, cancelShort := context.WithTimeout(warmCtx, 10*time.Millisecond)
			m, err := s.RecvMsg(ctxShort)
			cancelShort()
			if err != nil {
				break
			}
			if resetLarge {
				m.Reset()
			}
		}
	}
	warmCancel()

	recvCtx, recvCancel := context.WithCancel(context.Background())
	defer recvCancel()

	// Per-sub drain goroutine. We gate b.N iterations on all nSub having
	// received the next message, so the benchmark measures the slowest
	// SUB — the honest fanout number.
	var recvWG sync.WaitGroup
	subCounts := make([]atomic.Int64, nSub)
	for i, s := range subs {
		i, s := i, s
		recvWG.Add(1)
		go func() {
			defer recvWG.Done()
			for {
				m, err := s.RecvMsg(recvCtx)
				if err != nil {
					return
				}
				if resetLarge {
					m.Reset()
				}
				subCounts[i].Add(1)
			}
		}()
	}

	sendCtx, sendCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer sendCancel()

	b.SetBytes(int64(msgSize))
	b.ResetTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if err := pub.SendMsg(sendCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
			b.Fatalf("send[%d]: %v", i, err)
		}
	}

	// Wait for all subs to drain to b.N before stopping the timer. HWM is
	// raised above; if subs still lag by the 3-second deadline we stop
	// anyway and report the actual received count via ReportMetric —
	// better than a timeout-sized hang on the honest-but-lossy PUB path.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for i := 0; i < nSub; i++ {
			if subCounts[i].Load() < int64(b.N) {
				done = false
				break
			}
		}
		if done {
			break
		}
		time.Sleep(time.Millisecond)
	}
	b.StopTimer()

	// Report the minimum received count so slow-sub drops are visible.
	minReceived := int64(b.N)
	for i := 0; i < nSub; i++ {
		if c := subCounts[i].Load(); c < minReceived {
			minReceived = c
		}
	}
	b.ReportMetric(float64(minReceived), "msgs-delivered")

	recvCancel()
	for _, s := range subs {
		_ = s.Close()
	}
	recvWG.Wait()
}
