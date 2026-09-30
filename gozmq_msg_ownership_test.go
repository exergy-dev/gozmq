package gozmq_test

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/exergy-dev/gozmq"
)

// Message-ownership regressions. Received frames used to be backed by a
// refcounted buffer pool with implicit ownership, which corrupted data
// whenever the frames outlived the pool's idea of their lifetime. Frames are
// now plain memory by default, and pooled only with WithRecvBufferPool,
// under an explicit Release contract. Every pattern that used to break runs
// in both modes; run them with -race.

// recvModes are the two receive-buffer modes; the options go on every
// socket of a test (pooling only affects receiving).
var recvModes = []struct {
	name string
	opts []gozmq.Option
}{
	{"plain", nil},
	{"pooled", []gozmq.Option{gozmq.WithRecvBufferPool(true)}},
}

// forEachRecvMode runs fn as a subtest per receive-buffer mode, checking
// for leaked goroutines once the subtest's sockets are closed.
func forEachRecvMode(t *testing.T, fn func(t *testing.T, opts []gozmq.Option)) {
	for _, mode := range recvModes {
		t.Run(mode.name, func(t *testing.T) {
			verifyNoLeaks(t)
			fn(t, mode.opts)
		})
	}
}

// verifyNoLeaks checks, after every other cleanup (the sockets' Close) has
// run, that the test left no goroutines behind.
func verifyNoLeaks(t *testing.T) {
	t.Helper()
	ignore := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, ignore) })
}

// largePayload returns a distinct >4 KiB payload for message i.
func largePayload(i, size int) []byte {
	b := make([]byte, size)
	for j := range b {
		b[j] = byte(i) ^ byte(j)
	}
	return b
}

// TestMsgOwnership_ProxyLargeFrameRoundTrip: a received large-frame *Msg
// re-sent verbatim through a second TCP hop must arrive byte-exact. Nothing
// is released, which in pooled mode is the forgotten-Release case: safe,
// the buffers are just left to the GC.
func TestMsgOwnership_ProxyLargeFrameRoundTrip(t *testing.T) {
	forEachRecvMode(t, func(t *testing.T, opts []gozmq.Option) {
		// Two PUSH(bind)→PULL(connect) hops. PUSH blocks until its peer
		// attaches, so no handshake settle is needed before traffic.
		_, source, proxyIn := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, "tcp://127.0.0.1:0", opts, opts)
		_, proxyOut, sink := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, "tcp://127.0.0.1:0", opts, opts)

		const (
			n    = 60
			size = 64 * 1024
		)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		proxyDone := make(chan error, 1)
		go func() {
			for i := 0; i < n; i++ {
				m, err := proxyIn.RecvMsg(ctx)
				if err != nil {
					proxyDone <- err
					return
				}
				if err := proxyOut.SendMsg(ctx, m); err != nil {
					proxyDone <- err
					return
				}
			}
			proxyDone <- nil
		}()

		sendDone := make(chan error, 1)
		go func() {
			for i := 0; i < n; i++ {
				if err := source.SendMsg(ctx, gozmq.NewMsgFromBytes(largePayload(i, size))); err != nil {
					sendDone <- err
					return
				}
			}
			sendDone <- nil
		}()

		for i := 0; i < n; i++ {
			m, err := sink.RecvMsg(ctx)
			require.NoErrorf(t, err, "sink recv %d", i)
			require.Equalf(t, largePayload(i, size), m.Bytes(), "payload %d corrupted in proxy/writer path", i)
		}
		require.NoError(t, <-sendDone, "sender")
		require.NoError(t, <-proxyDone, "proxy")
	})
}

// TestMsgOwnership_ForwardThenRelease: RecvMsg → SendMsg → Release. In the
// old implicit model Release handed the buffers back to the pool while the
// writev still referenced them (most forwarded frames arrived corrupted);
// SendMsg now holds its own reference until the write completes.
func TestMsgOwnership_ForwardThenRelease(t *testing.T) {
	for _, size := range []int{8 << 10, 256 << 10} {
		t.Run(fmt.Sprintf("%dKiB", size>>10), func(t *testing.T) {
			forEachRecvMode(t, func(t *testing.T, opts []gozmq.Option) {
				_, mid, src := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", opts, opts)
				_, dst, out := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", opts, opts)

				const n = 2000
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()

				var wg sync.WaitGroup
				defer func() { cancel(); wg.Wait() }()
				wg.Add(2)
				go func() {
					defer wg.Done()
					for i := 0; i < n; i++ {
						if src.SendBytes(ctx, bytes.Repeat([]byte{byte(i)}, size)) != nil {
							return
						}
					}
				}()
				go func() {
					defer wg.Done()
					for i := 0; i < n; i++ {
						m, err := mid.RecvMsg(ctx)
						if err != nil {
							return
						}
						if out.SendMsg(ctx, m) != nil {
							return
						}
						m.Release()
					}
				}()

				bad := 0
				for i := 0; i < n; i++ {
					b, err := dst.RecvBytes(ctx)
					require.NoErrorf(t, err, "recv %d", i)
					if !bytes.Equal(b, bytes.Repeat([]byte{byte(i)}, size)) {
						bad++
					}
				}
				require.Zerof(t, bad, "%d of %d forwarded messages arrived corrupted", bad, n)
			})
		})
	}
}

// TestMsgOwnership_ReleaseBeforeFlush: every forwarded message is released
// while it is still queued for the outbound writer — the receiver of the
// second hop does not read until all of them are forwarded, and its RecvHWM
// of 1 keeps the backlog on the sending side — while further receives on
// the first hop keep recycling pool buffers. The queued messages must still
// go out intact.
func TestMsgOwnership_ReleaseBeforeFlush(t *testing.T) {
	forEachRecvMode(t, func(t *testing.T, opts []gozmq.Option) {
		const (
			n    = 300
			size = 64 << 10
		)
		_, mid, src := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", opts, opts)
		dstOpts := append([]gozmq.Option{gozmq.WithRecvHWM(1)}, opts...)
		outOpts := append([]gozmq.Option{gozmq.WithSendHWM(n)}, opts...)
		_, dst, out := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", dstOpts, outOpts)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		go func() {
			for i := 0; i < n; i++ {
				if src.SendMsg(ctx, gozmq.NewMsgFromBytes(largePayload(i, size))) != nil {
					return
				}
			}
		}()
		for i := 0; i < n; i++ {
			m, err := mid.RecvMsg(ctx)
			require.NoErrorf(t, err, "mid recv %d", i)
			require.NoErrorf(t, out.SendMsg(ctx, m), "forward %d", i)
			m.Release()
		}
		for i := 0; i < n; i++ {
			m, err := dst.RecvMsg(ctx)
			require.NoErrorf(t, err, "dst recv %d", i)
			require.Truef(t, bytes.Equal(largePayload(i, size), m.Bytes()), "message %d corrupted", i)
			m.Release()
		}
	})
}

// TestMsgOwnership_RouterForwardThenRelease: a broker re-addresses each
// message received on one ROUTER and routes it out of another, releasing
// both messages right after SendMsg. The re-addressed message is derived
// from the received one, and the outbound ROUTER's identity strip derives
// again, so the frames must stay valid until the outbound write. As in
// TestMsgOwnership_ReleaseBeforeFlush, the sink reads only after the whole
// batch is forwarded, so most writes happen after the releases, while the
// inbound side recycles pool buffers.
func TestMsgOwnership_RouterForwardThenRelease(t *testing.T) {
	forEachRecvMode(t, func(t *testing.T, opts []gozmq.Option) {
		const n, size = 300, 64 << 10
		sinkID := []byte("sink")
		_, front, client := newSocketPairTypes(t, gozmq.Router, gozmq.Dealer, "tcp://127.0.0.1:0", opts, opts)
		_, back, sink := newSocketPairTypes(t, gozmq.Router, gozmq.Dealer, "tcp://127.0.0.1:0",
			append([]gozmq.Option{gozmq.WithRouterMandatory(true), gozmq.WithSendHWM(n)}, opts...),
			append([]gozmq.Option{gozmq.WithIdentity(sinkID), gozmq.WithRecvHWM(1)}, opts...))

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Wait for back to see the sink, so routed sends cannot miss it.
		require.NoError(t, sink.SendBytes(ctx, []byte("hello")))
		hello, err := back.RecvMsg(ctx)
		require.NoError(t, err)
		hello.Release()

		go func() {
			for i := 0; i < n; i++ {
				if client.SendMsg(ctx, gozmq.NewMsgFromBytes(largePayload(i, size))) != nil {
					return
				}
			}
		}()
		for i := 0; i < n; i++ {
			m, err := front.RecvMsg(ctx) // [client id, body]
			require.NoErrorf(t, err, "front recv %d", i)
			fwd := m.Derive(sinkID, m.Frames()[1])
			m.Release()
			require.NoErrorf(t, back.SendMsg(ctx, fwd), "forward %d", i)
			fwd.Release()
		}
		for i := 0; i < n; i++ {
			m, err := sink.RecvMsg(ctx)
			require.NoErrorf(t, err, "sink recv %d", i)
			require.Truef(t, bytes.Equal(largePayload(i, size), m.Bytes()), "message %d corrupted", i)
			m.Release()
		}
	})
}

// TestMsgOwnership_RetainedFrameSurvivesGC: a frame kept after its *Msg is
// dropped (without Release) must not change when the GC runs and more
// large frames arrive. A finalizer on the *Msg used to recycle the buffer
// under the live frame; there is none, so in pooled mode the forgotten
// buffer is simply collected.
func TestMsgOwnership_RetainedFrameSurvivesGC(t *testing.T) {
	forEachRecvMode(t, func(t *testing.T, opts []gozmq.Option) {
		_, push, pull := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, "tcp://127.0.0.1:0", opts, opts)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		const size = 64 * 1024
		want := bytes.Repeat([]byte{'A'}, size)
		for i := 0; i < 20; i++ {
			require.NoError(t, push.SendMsg(ctx, gozmq.NewMsgFromBytes(bytes.Clone(want))))
			m, err := pull.RecvMsg(ctx)
			require.NoError(t, err)
			frame := m.Frames()[0] // keep only the bytes; the *Msg is dropped
			runtime.GC()
			runtime.GC()
			for j := 0; j < 4; j++ {
				require.NoError(t, push.SendMsg(ctx, gozmq.NewMsgFromBytes(bytes.Repeat([]byte{'B'}, size))))
				m, err := pull.RecvMsg(ctx)
				require.NoError(t, err)
				m.Release()
			}
			require.Truef(t, bytes.Equal(want, frame), "retained frame changed (iteration %d)", i)
		}
	})
}

// TestMsgOwnership_EchoDeferRelease: forwarding a received message to an
// inproc peer and releasing it on return must not corrupt what the peer
// receives, even as more frames arrive: the inproc receiver's message holds
// its own reference, dropped by its Release.
func TestMsgOwnership_EchoDeferRelease(t *testing.T) {
	forEachRecvMode(t, func(t *testing.T, opts []gozmq.Option) {
		_, push, pull := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, "tcp://127.0.0.1:0", opts, opts)
		_, fwd, sink := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, "inproc://msg-ownership-echo", opts, opts)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		const size = 64 * 1024
		want := bytes.Repeat([]byte{'A'}, size)
		for i := 0; i < 20; i++ {
			require.NoError(t, push.SendMsg(ctx, gozmq.NewMsgFromBytes(bytes.Clone(want))))
			func() {
				m, err := pull.RecvMsg(ctx)
				require.NoError(t, err)
				defer m.Release()
				require.NoError(t, fwd.SendMsg(ctx, m))
			}()
			require.NoError(t, push.SendMsg(ctx, gozmq.NewMsgFromBytes(bytes.Repeat([]byte{'B'}, size))))
			m, err := pull.RecvMsg(ctx)
			require.NoError(t, err)
			m.Release()
			got, err := sink.RecvMsg(ctx)
			require.NoError(t, err)
			require.Truef(t, bytes.Equal(want, got.Frames()[0]), "forwarded frame corrupted (iteration %d)", i)
			got.Release()
		}
	})
}

// TestMsgOwnership_ProxyRouterCaptureRace: a ROUTER frontend with a capture
// socket hands the same *Msg to the backend and to the capture tap. ROUTER
// Send used to move pool ownership out of the caller's message, racing the
// tap's concurrent read of it (-race report); engines now derive instead.
func TestMsgOwnership_ProxyRouterCaptureRace(t *testing.T) {
	forEachRecvMode(t, func(t *testing.T, opts []gozmq.Option) {
		_, front, client := newSocketPairTypes(t, gozmq.Router, gozmq.Dealer, "tcp://127.0.0.1:0", opts, opts)
		_, back, worker := newSocketPairTypes(t, gozmq.Dealer, gozmq.Dealer, "tcp://127.0.0.1:0", opts, opts)
		_, capture := newTestSocket(t, gozmq.Pub, opts...)
		require.NoError(t, capture.Bind("inproc://msg-ownership-capture"))

		pctx, pcancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- gozmq.Proxy(pctx, front, back, capture) }()
		defer func() { pcancel(); <-done }()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		workerDone := make(chan struct{})
		defer func() { cancel(); <-workerDone }()
		go func() { // echo worker
			defer close(workerDone)
			for {
				m, err := worker.RecvMsg(ctx)
				if err != nil {
					return
				}
				err = worker.SendMsg(ctx, m)
				m.Release()
				if err != nil {
					return
				}
			}
		}()
		body := bytes.Repeat([]byte{'z'}, 64*1024)
		for i := 0; i < 50; i++ {
			require.NoError(t, client.SendMsg(ctx, gozmq.NewMsgFromBytes(body)))
			m, err := client.RecvMsg(ctx)
			require.NoError(t, err)
			require.Truef(t, bytes.Equal(body, m.Frames()[0]), "reply %d corrupted", i)
			m.Release()
		}
	})
}

// TestRecvBufferPool_LentOnlyForLargeFrames: only frames beyond the
// reader's 4 KiB scratch on a pooled socket are lent.
func TestRecvBufferPool_LentOnlyForLargeFrames(t *testing.T) {
	for _, mode := range recvModes {
		t.Run(mode.name, func(t *testing.T) {
			_, push, pull := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, "tcp://127.0.0.1:0", nil, mode.opts)
			ctx := testCtx(t)
			for _, size := range []int{100, 4 << 10, 4<<10 + 1, 1 << 20} {
				require.NoError(t, push.SendMsg(ctx, gozmq.NewMsgFromBytes(largePayload(size, size))))
				m, err := pull.RecvMsg(ctx)
				require.NoError(t, err)
				require.Equal(t, largePayload(size, size), m.Bytes())
				require.Equalf(t, mode.opts != nil && size > 4<<10, m.Lent(), "Lent for %d bytes", size)
				m.Release()
			}
		})
	}
}

// TestRecvBufferPool_RetainedUntilRelease: a lent frame stays intact while
// any reference is held, however many receives recycle other buffers
// meanwhile; after the final Release its buffer goes back to the pool and
// is handed to a later receive.
func TestRecvBufferPool_RetainedUntilRelease(t *testing.T) {
	verifyNoLeaks(t)
	pool := []gozmq.Option{gozmq.WithRecvBufferPool(true)}
	_, push, pull := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, "tcp://127.0.0.1:0", nil, pool)
	ctx := testCtx(t)

	const size = 64 << 10
	send := func(i int) {
		t.Helper()
		require.NoError(t, push.SendMsg(ctx, gozmq.NewMsgFromBytes(largePayload(i, size))))
	}
	recvRelease := func() (first *byte) {
		t.Helper()
		m, err := pull.RecvMsg(ctx)
		require.NoError(t, err)
		first = &m.Bytes()[0]
		m.Release()
		return first
	}

	send(0)
	kept, err := pull.RecvMsg(ctx)
	require.NoError(t, err)
	require.True(t, kept.Lent())
	frame := kept.Bytes()
	kept.Retain() // two references now

	churn := func() {
		for i := 1; i <= 50; i++ {
			send(i)
			require.NotSame(t, &frame[0], recvRelease(), "a buffer still referenced was handed out again")
		}
		require.Equal(t, largePayload(0, size), frame, "retained frame changed")
	}
	churn()
	kept.Release()
	churn() // one reference left
	kept.Release()

	// Released: the buffer may now be reused. sync.Pool gives no guarantee
	// (and drops some Puts under -race), so allow many tries.
	reused := false
	for i := 0; i < 500 && !reused; i++ {
		send(i)
		reused = recvRelease() == &frame[0]
	}
	require.True(t, reused, "released buffer never came back from the pool")
}

// TestRecvBufferPool_OverRelease: releasing a received message more often
// than it was retained panics, and cannot hand back the reference SendMsg
// took for a forward still in flight.
func TestRecvBufferPool_OverRelease(t *testing.T) {
	verifyNoLeaks(t)
	pool := []gozmq.Option{gozmq.WithRecvBufferPool(true)}
	_, push, pull := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, "tcp://127.0.0.1:0", nil, pool)
	_, dst, out := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", pool, nil)
	ctx := testCtx(t)

	const n, size = 20, 64 << 10
	for i := 0; i < n; i++ {
		require.NoError(t, push.SendMsg(ctx, gozmq.NewMsgFromBytes(largePayload(i, size))))
		m, err := pull.RecvMsg(ctx)
		require.NoError(t, err)
		require.NoError(t, out.SendMsg(ctx, m))
		m.Release()
		require.Panics(t, m.Release, "over-release must panic")
		require.Panics(t, m.Retain, "Retain after the final Release must panic")
	}
	for i := 0; i < n; i++ {
		m, err := dst.RecvMsg(ctx)
		require.NoError(t, err)
		require.Truef(t, bytes.Equal(largePayload(i, size), m.Bytes()), "forwarded message %d corrupted", i)
		m.Release()
	}
}

// TestRecvBufferPool_PubFanout: a pooled message published to several
// subscribers — three over TCP, one inproc — and released by the publisher
// right after SendMsg reaches every subscriber intact. Each subscriber's
// copy carries its own reference.
func TestRecvBufferPool_PubFanout(t *testing.T) {
	verifyNoLeaks(t)
	pool := []gozmq.Option{gozmq.WithRecvBufferPool(true)}
	_, mid, src := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", pool, nil)
	zctx, pub := newTestSocket(t, gozmq.Pub, gozmq.WithSendHWM(0))
	require.NoError(t, pub.Bind("tcp://127.0.0.1:0"))
	tcpEP := pub.LastEndpoint()
	require.NoError(t, pub.Bind("inproc://msg-ownership-fanout"))

	var subs []*gozmq.Socket
	for i, ep := range []string{tcpEP, tcpEP, tcpEP, "inproc://msg-ownership-fanout"} {
		syncTopic := fmt.Appendf(nil, "S%d", i)
		sub, err := zctx.NewSocket(gozmq.Sub, append([]gozmq.Option{
			gozmq.WithRecvHWM(0), gozmq.WithSubscribe([]byte("D")), gozmq.WithSubscribe(syncTopic),
		}, pool...)...)
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Close() })
		require.NoError(t, sub.Connect(ep))
		// Subscriptions travel in order, so once the sync topic is live
		// the data topic is too.
		syncSub(t, pub, sub, syncTopic)
		subs = append(subs, sub)
	}

	const n, size = 300, 64 << 10
	payload := func(i int) []byte {
		b := largePayload(i, size)
		b[0] = 'D'
		return b
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errs := make([]error, len(subs))
	for s, sub := range subs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				m, err := sub.RecvMsg(ctx)
				if err != nil {
					errs[s] = err
					return
				}
				ok := bytes.Equal(payload(i), m.Bytes())
				m.Release()
				if !ok {
					errs[s] = fmt.Errorf("subscriber %d: message %d corrupted", s, i)
					return
				}
			}
		}()
	}
	go func() {
		for i := 0; i < n; i++ {
			if src.SendMsg(ctx, gozmq.NewMsgFromBytes(payload(i))) != nil {
				return
			}
		}
	}()
	for i := 0; i < n; i++ {
		m, err := mid.RecvMsg(ctx)
		require.NoError(t, err)
		require.True(t, m.Lent())
		require.NoError(t, pub.SendMsg(ctx, m))
		m.Release()
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
}

// TestRecvBufferPool_RepLentEnvelope: REP keeps a request's envelope for
// the reply after handing the body to the application. When the envelope
// holds a lent (>4 KiB) frame, the reply must still carry it intact even
// though the application released the request before replying and other
// receives recycled pool buffers meanwhile.
func TestRecvBufferPool_RepLentEnvelope(t *testing.T) {
	verifyNoLeaks(t)
	pool := []gozmq.Option{gozmq.WithRecvBufferPool(true)}
	_, rep, dealer := newSocketPairTypes(t, gozmq.Rep, gozmq.Dealer, "tcp://127.0.0.1:0", pool, pool)
	_, churnIn, churnOut := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", pool, nil)
	ctx := testCtx(t)

	const size = 64 << 10
	for i := 0; i < 20; i++ {
		// [route, delimiter, body]: route is part of REP's envelope.
		route := largePayload(i, 8<<10)
		body := largePayload(i+1, size)
		require.NoError(t, dealer.SendMsg(ctx, gozmq.NewMsgFromFrames(route, nil, body)))

		req, err := rep.RecvMsg(ctx)
		require.NoError(t, err)
		require.Equal(t, body, req.Bytes())
		reply := gozmq.NewMsgFromBytes(bytes.Clone(req.Bytes()))
		req.Release()
		for j := 0; j < 20; j++ {
			require.NoError(t, churnOut.SendMsg(ctx, gozmq.NewMsgFromBytes(largePayload(j, 8<<10))))
			m, err := churnIn.RecvMsg(ctx)
			require.NoError(t, err)
			m.Release()
		}
		require.NoError(t, rep.SendMsg(ctx, reply))

		got, err := dealer.RecvMsg(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, got.NumFrames())
		require.Truef(t, bytes.Equal(route, got.Frames()[0]), "envelope frame corrupted (iteration %d)", i)
		require.Truef(t, bytes.Equal(body, got.Frames()[2]), "reply body corrupted (iteration %d)", i)
		got.Release()
	}
}

// TestRecvBufferPool_StreamRejected: STREAM sockets have no ZMTP framing to
// pool.
func TestRecvBufferPool_StreamRejected(t *testing.T) {
	zctx := gozmq.NewContext()
	t.Cleanup(func() { _ = zctx.Close() })
	_, err := zctx.NewSocket(gozmq.Stream, gozmq.WithRecvBufferPool(true))
	require.ErrorContains(t, err, "WithRecvBufferPool")
}
