package perf_test

// The pattern-by-transport matrix: the rows benchmark_test.go and
// tls_bench_test.go do not already have, so that every socket pattern runs
// over every transport it supports (inproc, TCP, IPC, TLS, and CURVE over
// TCP for the request/reply and pub/sub shapes). Names follow the existing
// Benchmark<Pattern><What>_<Transport>/<size> convention, so benchgate and
// benchstat line them up across commits. The STREAM rows are TCP only: a
// STREAM socket carries raw TCP bytes to a non-ZMQ peer.

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
)

// pairMaker binds a serverType socket and connects a clientType socket to
// it, on some transport. onEndpoint adapts the endpoint-based transports;
// newTLSPair is the TLS one.
type pairMaker func(b *testing.B, serverType, clientType gozmq.SocketType, serverOpts, clientOpts []gozmq.Option) (server, client *gozmq.Socket)

func onEndpoint(tr transport) pairMaker {
	return func(b *testing.B, st, ct gozmq.SocketType, so, co []gozmq.Option) (*gozmq.Socket, *gozmq.Socket) {
		return newPair(b, st, ct, so, co, tr())
	}
}

// forEachSizeOn runs bench once per rung of ladder over mk.
func forEachSizeOn(b *testing.B, ladder []sizeSpec, mk pairMaker, bench func(b *testing.B, mk pairMaker, size int)) {
	for _, sz := range ladder {
		b.Run(sz.name, func(b *testing.B) { bench(b, mk, sz.n) })
	}
}

var streamHWM = []gozmq.Option{gozmq.WithSendHWM(benchHWM), gozmq.WithRecvHWM(benchHWM)}

// --- PAIR ---------------------------------------------------------------

// benchPairThroughput streams b.N messages from one PAIR to the other.
func benchPairThroughput(b *testing.B, mk pairMaker, size int) {
	b.StopTimer()
	recv, send := mk(b, gozmq.Pair, gozmq.Pair, streamHWM, streamHWM)
	warmStream(b, send, size, recv)
	benchStream(b, send, []*gozmq.Socket{recv}, size, drainWait)
}

// benchPairRoundTrip times PAIR to PAIR round trips (the server echoes).
func benchPairRoundTrip(b *testing.B, mk pairMaker, size int) {
	b.StopTimer()
	server, client := mk(b, gozmq.Pair, gozmq.Pair, nil, nil)
	echoRoundTrips(b, server, client, size)
}

func BenchmarkPairThroughput_Inproc(b *testing.B) {
	forEachSizeOn(b, sizes, onEndpoint(inprocEndpoint), benchPairThroughput)
}
func BenchmarkPairThroughput_TCP(b *testing.B) {
	forEachSizeOn(b, sizes, onEndpoint(tcpEndpoint), benchPairThroughput)
}
func BenchmarkPairThroughput_IPC(b *testing.B) {
	forEachSizeOn(b, ipcSizes, onEndpoint(ipcTransport(b)), benchPairThroughput)
}
func BenchmarkPairThroughput_TLS(b *testing.B) {
	forEachSizeOn(b, sizes, newTLSPair, benchPairThroughput)
}
func BenchmarkPairRoundTrip_Inproc(b *testing.B) {
	forEachSizeOn(b, latencySizes, onEndpoint(inprocEndpoint), benchPairRoundTrip)
}
func BenchmarkPairRoundTrip_TCP(b *testing.B) {
	forEachSizeOn(b, latencySizes, onEndpoint(tcpEndpoint), benchPairRoundTrip)
}
func BenchmarkPairRoundTrip_IPC(b *testing.B) {
	forEachSizeOn(b, latencySizes, onEndpoint(ipcTransport(b)), benchPairRoundTrip)
}
func BenchmarkPairRoundTrip_TLS(b *testing.B) {
	forEachSizeOn(b, latencySizes, newTLSPair, benchPairRoundTrip)
}

// --- REQ/REP and ROUTER/DEALER round trips on the remaining transports ----

func benchReqRepOn(b *testing.B, mk pairMaker, size int) {
	b.StopTimer()
	server, client := mk(b, gozmq.Rep, gozmq.Req, nil, nil)
	echoRoundTrips(b, server, client, size)
}

func benchRouterDealerOn(b *testing.B, mk pairMaker, size int) {
	b.StopTimer()
	server, client := mk(b, gozmq.Router, gozmq.Dealer, nil, []gozmq.Option{gozmq.WithIdentity([]byte("bench-dealer"))})
	echoRoundTrips(b, server, client, size)
}

func BenchmarkReqRepRoundTrip_TLS(b *testing.B) {
	forEachSizeOn(b, latencySizes, newTLSPair, benchReqRepOn)
}
func BenchmarkRouterDealerLatency_IPC(b *testing.B) {
	forEachSize(b, latencySizes, ipcTransport(b), benchRouterDealer)
}
func BenchmarkRouterDealerLatency_TLS(b *testing.B) {
	forEachSizeOn(b, latencySizes, newTLSPair, benchRouterDealerOn)
}

// BenchmarkReqRepCurve_TCP is BenchmarkReqRepRoundTrip_TCP with CURVE on
// both ends: the per-message seal and open on each leg of a round trip.
func BenchmarkReqRepCurve_TCP(b *testing.B) {
	serverPub, serverSec := testutil.CurveKeys(b)
	clientPub, clientSec := testutil.CurveKeys(b)
	forEachSize(b, curveSizes, tcpEndpoint, func(b *testing.B, endpoint string, size int) {
		benchEcho(b, gozmq.Rep, gozmq.Req,
			[]gozmq.Option{gozmq.WithCurveServer(serverSec[:])},
			[]gozmq.Option{gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:])}, endpoint, size)
	})
}

// --- DEALER -> ROUTER and ROUTER -> DEALER streaming --------------------

// benchDealerRouterThroughput streams b.N messages from a DEALER to the
// ROUTER it is connected to; the ROUTER receives [identity, body].
func benchDealerRouterThroughput(b *testing.B, mk pairMaker, size int) {
	b.StopTimer()
	router, dealer := mk(b, gozmq.Router, gozmq.Dealer, streamHWM, streamHWM)
	warmStream(b, dealer, size, router)
	benchStream(b, dealer, []*gozmq.Socket{router}, size, drainWait)
}

// benchRouterDealerThroughput streams b.N [identity, body] messages from a
// ROUTER to the DEALER it addresses. The DEALER binds with a known
// identity and says hello first, which proves the ROUTER has the route.
func benchRouterDealerThroughput(b *testing.B, mk pairMaker, size int) {
	b.StopTimer()
	id := []byte("bench-dealer")
	dealer, router := mk(b, gozmq.Dealer, gozmq.Router,
		append([]gozmq.Option{gozmq.WithIdentity(id)}, streamHWM...),
		append([]gozmq.Option{gozmq.WithRouterMandatory(true)}, streamHWM...))
	ctx := testutil.Ctx(b, setupTimeout)
	if err := dealer.SendMsg(ctx, gozmq.NewMsgFromBytes([]byte("hello"))); err != nil {
		b.Fatalf("dealer hello: %v", err)
	}
	m, err := router.RecvMsg(ctx)
	if err != nil {
		b.Fatalf("router recv hello: %v", err)
	}
	m.Release()
	payload := newPayload(size)
	benchStreamMsgs(b, router, []*gozmq.Socket{dealer}, size,
		func() *gozmq.Msg { return gozmq.NewMsgFromFrames(id, payload) }, drainWait)
}

func BenchmarkDealerRouterThroughput_Inproc(b *testing.B) {
	forEachSizeOn(b, sizes, onEndpoint(inprocEndpoint), benchDealerRouterThroughput)
}
func BenchmarkDealerRouterThroughput_TCP(b *testing.B) {
	forEachSizeOn(b, sizes, onEndpoint(tcpEndpoint), benchDealerRouterThroughput)
}
func BenchmarkDealerRouterThroughput_IPC(b *testing.B) {
	forEachSizeOn(b, ipcSizes, onEndpoint(ipcTransport(b)), benchDealerRouterThroughput)
}
func BenchmarkDealerRouterThroughput_TLS(b *testing.B) {
	forEachSizeOn(b, sizes, newTLSPair, benchDealerRouterThroughput)
}
func BenchmarkRouterDealerThroughput_Inproc(b *testing.B) {
	forEachSizeOn(b, sizes, onEndpoint(inprocEndpoint), benchRouterDealerThroughput)
}
func BenchmarkRouterDealerThroughput_TCP(b *testing.B) {
	forEachSizeOn(b, sizes, onEndpoint(tcpEndpoint), benchRouterDealerThroughput)
}
func BenchmarkRouterDealerThroughput_IPC(b *testing.B) {
	forEachSizeOn(b, ipcSizes, onEndpoint(ipcTransport(b)), benchRouterDealerThroughput)
}
func BenchmarkRouterDealerThroughput_TLS(b *testing.B) {
	forEachSizeOn(b, sizes, newTLSPair, benchRouterDealerThroughput)
}

// --- PUB/SUB and XPUB/XSUB on the remaining transports -------------------

// benchPubSubOn is benchPubSub over a pairMaker (one subscribe-to-all SUB).
func benchPubSubOn(b *testing.B, mk pairMaker, size int) {
	b.StopTimer()
	pub, sub := mk(b, gozmq.Pub, gozmq.Sub, streamHWM, append([]gozmq.Option{gozmq.WithSubscribe(nil)}, streamHWM...))
	syncSubs(b, pub, sub)
	delivered := benchStream(b, pub, []*gozmq.Socket{sub}, size, drainWait)
	b.ReportMetric(float64(delivered), "msgs-delivered")
}

// benchXPubXSubOn is benchXPubXSubThroughput over a pairMaker.
func benchXPubXSubOn(b *testing.B, mk pairMaker, size int) {
	b.StopTimer()
	xpub, xsub := mk(b, gozmq.XPub, gozmq.XSub, streamHWM, streamHWM)
	subscribeAll(b, xpub, xsub)
	warmStream(b, xpub, size, xsub)
	benchStream(b, xpub, []*gozmq.Socket{xsub}, size, drainWait)
}

func BenchmarkPubSubThroughput_IPC(b *testing.B) {
	forEachSize(b, smallSizes, ipcTransport(b), benchPubSub)
}
func BenchmarkPubSubThroughput_TLS(b *testing.B) {
	forEachSizeOn(b, smallSizes, newTLSPair, benchPubSubOn)
}

// BenchmarkPubSubFanout_TCP and _IPC are BenchmarkPubSubFanout_Inproc (one
// PUB, three SUBs) over the two socket transports, where the PUB writes
// each message once per subscriber connection.
func BenchmarkPubSubFanout_TCP(b *testing.B) {
	forEachSize(b, sizes, tcpEndpoint, func(b *testing.B, endpoint string, size int) {
		benchPubSubFanout(b, endpoint, size, 3)
	})
}
func BenchmarkPubSubFanout_IPC(b *testing.B) {
	forEachSize(b, ipcSizes, ipcTransport(b), func(b *testing.B, endpoint string, size int) {
		benchPubSubFanout(b, endpoint, size, 3)
	})
}

// BenchmarkPubSubCurve_TCP is BenchmarkPubSubThroughput_TCP with CURVE.
func BenchmarkPubSubCurve_TCP(b *testing.B) {
	serverPub, serverSec := testutil.CurveKeys(b)
	clientPub, clientSec := testutil.CurveKeys(b)
	forEachSize(b, curveSizes, tcpEndpoint, func(b *testing.B, endpoint string, size int) {
		benchPubSubOn(b, func(b *testing.B, st, ct gozmq.SocketType, so, co []gozmq.Option) (*gozmq.Socket, *gozmq.Socket) {
			so = append(so, gozmq.WithCurveServer(serverSec[:]))
			co = append(co, gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:]))
			return newPair(b, st, ct, so, co, endpoint)
		}, size)
	})
}

func BenchmarkXPubXSubThroughput_TCP(b *testing.B) {
	forEachSize(b, sizes, tcpEndpoint, benchXPubXSubThroughput)
}
func BenchmarkXPubXSubThroughput_IPC(b *testing.B) {
	forEachSize(b, ipcSizes, ipcTransport(b), benchXPubXSubThroughput)
}
func BenchmarkXPubXSubThroughput_TLS(b *testing.B) {
	forEachSizeOn(b, sizes, newTLSPair, benchXPubXSubOn)
}

// --- STREAM (TCP only) ---------------------------------------------------

// BenchmarkStreamRecv_TCP writes b.N size-byte chunks from a plain TCP
// connection into a bound STREAM socket, which receives [identity, data]
// in whatever chunks the reads return; the timer covers the writes plus
// the STREAM side's drain of every byte. SetBytes makes MB/s the figure.
func BenchmarkStreamRecv_TCP(b *testing.B) {
	forEachSize(b, sizes, tcpEndpoint, func(b *testing.B, endpoint string, size int) {
		b.StopTimer()
		srv := bind(b, gozmq.Stream, endpoint)
		conn, err := net.Dial("tcp", strings.TrimPrefix(srv.sock.LastEndpoint(), "tcp://"))
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		b.Cleanup(func() { _ = conn.Close() })

		total := int64(b.N) * int64(size)
		ctx, cancel := context.WithTimeout(context.Background(), benchTimeout)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			var got int64
			for got < total {
				m, err := srv.sock.RecvMsg(ctx)
				if err != nil {
					done <- err
					return
				}
				if fr := m.Frames(); len(fr) == 2 {
					got += int64(len(fr[1]))
				}
				m.Release()
			}
			done <- nil
		}()
		chunk := streamChunk(size)
		b.SetBytes(int64(size))
		b.ResetTimer()
		b.StartTimer()
		writeChunks(b, conn, chunk, size, b.N)
		if err := <-done; err != nil {
			b.Fatalf("stream recv: %v", err)
		}
		b.StopTimer()
	})
}

// BenchmarkStreamSend_TCP sends b.N [identity, data] messages from a
// connected STREAM socket to a plain TCP listener that reads every byte;
// the timer covers the sends plus the reader's drain.
func BenchmarkStreamSend_TCP(b *testing.B) {
	for _, sz := range sizes {
		b.Run(sz.name, func(b *testing.B) {
			b.StopTimer()
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatalf("listen: %v", err)
			}
			b.Cleanup(func() { _ = l.Close() })
			zctx := gozmq.NewContext()
			b.Cleanup(func() { _ = zctx.Close() })
			cli := newSocket(b, zctx, gozmq.Stream, streamHWM)
			if err := cli.Connect("tcp://" + l.Addr().String()); err != nil {
				b.Fatalf("connect: %v", err)
			}
			conn, err := l.Accept()
			if err != nil {
				b.Fatalf("accept: %v", err)
			}
			b.Cleanup(func() { _ = conn.Close() })
			// The STREAM side learns the connection's identity from the
			// first bytes the peer sends.
			if _, err := conn.Write([]byte("go")); err != nil {
				b.Fatalf("write go: %v", err)
			}
			ctx := testutil.Ctx(b, benchTimeout)
			m, err := cli.RecvMsg(ctx)
			if err != nil {
				b.Fatalf("stream recv go: %v", err)
			}
			id := append([]byte(nil), m.Frames()[0]...)
			m.Release()

			total := int64(b.N) * int64(sz.n)
			done := make(chan error, 1)
			go func() {
				_, err := io.CopyN(io.Discard, conn, total)
				done <- err
			}()
			payload := newPayload(sz.n)
			b.SetBytes(int64(sz.n))
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				if err := cli.SendMsg(ctx, gozmq.NewMsgFromFrames(id, payload)); err != nil {
					b.Fatalf("send[%d]: %v", i, err)
				}
			}
			if err := <-done; err != nil {
				b.Fatalf("raw read: %v", err)
			}
			b.StopTimer()
		})
	}
}

// streamChunk returns about 256 KiB of size-byte payloads back to back.
func streamChunk(size int) []byte {
	per := max(1, (256<<10)/size)
	p := newPayload(size)
	chunk := make([]byte, 0, per*size)
	for range per {
		chunk = append(chunk, p...)
	}
	return chunk
}

// writeChunks writes n size-byte payloads to w using chunk.
func writeChunks(b *testing.B, w io.Writer, chunk []byte, size, n int) {
	per := len(chunk) / size
	for left := n; left > 0; {
		k := min(left, per)
		if _, err := w.Write(chunk[:k*size]); err != nil {
			b.Fatalf("raw write: %v", err)
		}
		left -= k
	}
}
