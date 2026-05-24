package transport

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInproc_RoundTrip is the smoke: bind, dial, write, read in both
// directions. Documents the chosen "1 Write = 1 frame, 1 Read returns up
// to one frame's worth" contract.
func TestInproc_RoundTrip(t *testing.T) {
	tr := NewInprocTransport(0)
	l, err := tr.Listen(context.Background(), "inproc://round-trip")
	require.NoError(t, err, "Listen")
	defer l.Close()

	ctx := testCtx(t, 2*time.Second)

	type acceptResult struct {
		c   inprocConnish
		err error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			acceptCh <- acceptResult{nil, err}
			return
		}
		acceptCh <- acceptResult{c.(inprocConnish), nil}
	}()

	cli, err := tr.Dial(ctx, "inproc://round-trip")
	require.NoError(t, err, "Dial")
	defer cli.Close()

	r := <-acceptCh
	require.NoError(t, r.err, "Accept")
	defer r.c.Close()

	want := []byte("ping")
	_, err = cli.Write(want)
	require.NoError(t, err, "Write client")
	got := make([]byte, len(want))
	n, err := r.c.Read(got)
	require.NoError(t, err, "Read server")
	require.Equal(t, len(want), n, "Read server byte count")
	require.Equal(t, want, got, "server read data")

	want2 := []byte("pong")
	_, err = r.c.Write(want2)
	require.NoError(t, err, "Write server")
	got2 := make([]byte, len(want2))
	n, err = cli.Read(got2)
	require.NoError(t, err, "Read client")
	require.Equal(t, len(want2), n, "Read client byte count")
	require.Equal(t, want2, got2, "client read data")
}

// inprocConnish is the minimal subset we need from iface.Conn in tests.
type inprocConnish interface {
	io.Reader
	io.Writer
	io.Closer
	LocalEndpoint() string
	RemoteEndpoint() string
}

// TestInproc_FrameBoundary documents the chunk-boundary contract: a Read
// returns at most ONE Write's worth of bytes (it never merges two Writes).
// If the Read buffer is smaller than the Write, leftover bytes flow out on
// subsequent Reads.
func TestInproc_FrameBoundary(t *testing.T) {
	tr := NewInprocTransport(0)
	l, err := tr.Listen(context.Background(), "inproc://framing")
	require.NoError(t, err, "Listen")
	defer l.Close()

	ctx := testCtx(t, 2*time.Second)

	acceptCh := make(chan inprocConnish, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			assert.NoError(t, err, "Accept")
			close(acceptCh)
			return
		}
		acceptCh <- c.(inprocConnish)
	}()

	cli, err := tr.Dial(ctx, "inproc://framing")
	require.NoError(t, err, "Dial")
	defer cli.Close()

	srv := <-acceptCh
	require.NotNil(t, srv, "accept failed")
	defer srv.Close()

	// Two distinct Writes.
	_, err = cli.Write([]byte("aaa"))
	require.NoError(t, err)
	_, err = cli.Write([]byte("bbb"))
	require.NoError(t, err)

	// First Read with a small buffer pulls "aaa" only — it MUST NOT include
	// any bytes from the second Write.
	got := make([]byte, 16)
	n, err := srv.Read(got)
	require.NoError(t, err, "Read 1")
	require.Equal(t, "aaa", string(got[:n]), "Read 1 frame boundary violated")

	n, err = srv.Read(got)
	require.NoError(t, err, "Read 2")
	require.Equal(t, "bbb", string(got[:n]), "Read 2")

	// Short-buffer leftover: write 6 bytes, read into a 3-byte buffer twice.
	_, err = cli.Write([]byte("ABCDEF"))
	require.NoError(t, err)
	short := make([]byte, 3)
	n, err = srv.Read(short)
	require.NoError(t, err, "short Read 1")
	require.Equal(t, "ABC", string(short[:n]), "short Read 1 data")
	n, err = srv.Read(short)
	require.NoError(t, err, "short Read 2")
	require.Equal(t, "DEF", string(short[:n]), "short Read 2 data")
}

// TestInproc_ConnectBeforeBind_Queues asserts that a Dial can race ahead
// of Listen and still resolve, provided the Listen lands within the queue
// window.
func TestInproc_ConnectBeforeBind_Queues(t *testing.T) {
	tr := NewInprocTransport(500 * time.Millisecond)

	// Kick off Dial first; nothing is listening yet.
	dialDone := make(chan struct {
		c   io.Closer
		err error
	}, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		c, err := tr.Dial(ctx, "inproc://race")
		dialDone <- struct {
			c   io.Closer
			err error
		}{c, err}
	}()

	// Listen lands well within the queue window.
	time.Sleep(50 * time.Millisecond)
	l, err := tr.Listen(context.Background(), "inproc://race")
	require.NoError(t, err, "Listen")
	defer l.Close()

	// Listener side accepts so the server-side conn drains.
	go func() {
		c, _ := l.Accept(context.Background())
		if c != nil {
			c.Close()
		}
	}()

	select {
	case r := <-dialDone:
		require.NoError(t, r.err, "queued Dial")
		defer r.c.Close()
	case <-time.After(2 * time.Second):
		require.Fail(t, "queued Dial never resolved")
	}
}

// TestInproc_ConnectBeforeBind_Window asserts that after the window
// elapses, Dial returns an error wrapping ErrEndpointInvalid.
func TestInproc_ConnectBeforeBind_Window(t *testing.T) {
	tr := NewInprocTransport(50 * time.Millisecond)

	ctx := testCtx(t, 1*time.Second)
	_, err := tr.Dial(ctx, "inproc://no-bind-ever")
	require.Error(t, err, "Dial unexpectedly succeeded")
	require.ErrorIs(t, err, zmqerr.ErrEndpointInvalid)
}

// TestInproc_Dial_Honors_Ctx asserts ctx cancellation wakes a queued Dial.
func TestInproc_Dial_Honors_Ctx(t *testing.T) {
	tr := NewInprocTransport(10 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := tr.Dial(ctx, "inproc://never-bound")
		errCh <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		require.Fail(t, "Dial did not wake within 2s of ctx cancel")
	}
}

// TestInproc_Listen_DuplicateName errors on second bind to the same name.
func TestInproc_Listen_DuplicateName(t *testing.T) {
	tr := NewInprocTransport(0)
	l1, err := tr.Listen(context.Background(), "inproc://dup")
	require.NoError(t, err)
	defer l1.Close()

	_, err = tr.Listen(context.Background(), "inproc://dup")
	require.ErrorIs(t, err, zmqerr.ErrEndpointInvalid)
}

// TestInproc_BadEndpoint covers the parse failures.
func TestInproc_BadEndpoint(t *testing.T) {
	tr := NewInprocTransport(0)
	cases := []string{
		"inproc://", // empty name
		"tcp://x:1", // wrong scheme
		"no-scheme", // missing scheme
	}
	for _, ep := range cases {
		_, err := tr.Listen(context.Background(), ep)
		assert.ErrorIs(t, err, zmqerr.ErrEndpointInvalid, "Listen(%q)", ep)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, err = tr.Dial(ctx, ep)
		cancel()
		if !errors.Is(err, zmqerr.ErrEndpointInvalid) && !errors.Is(err, context.DeadlineExceeded) {
			assert.Failf(t, "unexpected Dial error", "Dial(%q) err = %v, want wraps ErrEndpointInvalid (or ctx)", ep, err)
		}
	}
}

// TestInproc_Listener_Close_Wakes_Accept asserts Accept wakes promptly on
// Close.
func TestInproc_Listener_Close_Wakes_Accept(t *testing.T) {
	tr := NewInprocTransport(0)
	l, err := tr.Listen(context.Background(), "inproc://close-wake")
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		_, err := l.Accept(context.Background())
		errCh <- err
	}()
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, l.Close())
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, zmqerr.ErrEndpointInvalid)
	case <-time.After(2 * time.Second):
		require.Fail(t, "Accept did not wake on Close")
	}
}

// TestInproc_Endpoint reports the bound endpoint string.
func TestInproc_Endpoint(t *testing.T) {
	tr := NewInprocTransport(0)
	l, err := tr.Listen(context.Background(), "inproc://my-name")
	require.NoError(t, err)
	defer l.Close()
	require.Equal(t, "inproc://my-name", l.Endpoint())
}

// TestInproc_Conn_Endpoints asserts the LocalEndpoint / RemoteEndpoint
// adapters report the bound name.
func TestInproc_Conn_Endpoints(t *testing.T) {
	tr := NewInprocTransport(0)
	l, err := tr.Listen(context.Background(), "inproc://eps")
	require.NoError(t, err)
	defer l.Close()

	ctx := testCtx(t, 2*time.Second)
	acceptCh := make(chan inprocConnish, 1)
	go func() {
		c, _ := l.Accept(ctx)
		acceptCh <- c.(inprocConnish)
	}()

	c, err := tr.Dial(ctx, "inproc://eps")
	require.NoError(t, err)
	defer c.Close()
	srv := <-acceptCh
	defer srv.Close()

	cc := c.(inprocConnish)
	assert.Equal(t, "inproc://eps", cc.RemoteEndpoint(), "client Remote")
	assert.Equal(t, "inproc://eps", srv.LocalEndpoint(), "server Local")
}

// TestInproc_ConcurrentDialsStress fires 50 concurrent Dials with random
// timeouts, then a Listen lands 100ms in. Every Dial whose timeout comfortably
// exceeds the Listen latency must succeed (no spurious ErrEndpointInvalid
// caused by the waiter-vs-notifier race). Every Dial whose timeout is below
// the Listen latency is allowed to return ctx.DeadlineExceeded or
// ErrEndpointInvalid (the queueWindow path) — but MUST NOT return any other
// error. Run under -race -count=20 to flush the race window repeatedly.
func TestInproc_ConcurrentDialsStress(t *testing.T) {
	const (
		N            = 50
		listenDelay  = 100 * time.Millisecond
		minTimeout   = 50 * time.Millisecond
		maxTimeout   = 500 * time.Millisecond
		queueWindow  = 2 * time.Second // plenty — we gate with the ctx
		safetyMargin = 30 * time.Millisecond
	)

	tr := NewInprocTransport(queueWindow)

	// Drain all accepts so dialIn doesn't block. We start the acceptor
	// after Listen; for now, just allocate the worker state.
	rng := rand.New(rand.NewSource(0xC0FFEE))
	timeouts := make([]time.Duration, N)
	for i := range timeouts {
		timeouts[i] = minTimeout + time.Duration(rng.Int63n(int64(maxTimeout-minTimeout)))
	}

	type result struct {
		idx     int
		timeout time.Duration
		err     error
		success bool
	}
	results := make(chan result, N)

	var wg sync.WaitGroup
	wg.Add(N)
	start := time.Now()
	for i := 0; i < N; i++ {
		i, to := i, timeouts[i]
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), to)
			defer cancel()
			c, err := tr.Dial(ctx, "inproc://race-stress")
			if c != nil {
				_ = c.Close()
			}
			results <- result{idx: i, timeout: to, err: err, success: err == nil}
		}()
	}

	// Listen lands 100ms later.
	time.Sleep(listenDelay)
	l, err := tr.Listen(context.Background(), "inproc://race-stress")
	require.NoError(t, err, "Listen")
	defer l.Close()

	// Drain accepts in the background so dialIn can place server-side conns.
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			c, err := l.Accept(context.Background())
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()

	wg.Wait()
	close(results)

	var succeeded, failedShort, failedLong int
	for r := range results {
		elapsed := r.timeout // upper bound; actual cancel time ≈ timeout from start
		_ = elapsed
		if r.success {
			succeeded++
			continue
		}
		// Any non-nil error: classify.
		if !errors.Is(r.err, context.DeadlineExceeded) && !errors.Is(r.err, zmqerr.ErrEndpointInvalid) {
			assert.Fail(t, "unexpected error", "dial[%d] timeout=%v: unexpected err = %v", r.idx, r.timeout, r.err)
			continue
		}
		// If the timeout strictly exceeded listenDelay + safety, the Listen
		// should have arrived in time for this Dial to succeed.
		if r.timeout > listenDelay+safetyMargin {
			assert.Fail(t, "spurious failure", "dial[%d] with timeout=%v (> listen@%v + %v) failed spuriously: %v",
				r.idx, r.timeout, listenDelay, safetyMargin, r.err)
			failedLong++
		} else {
			failedShort++
		}
	}
	t.Logf("stress: N=%d succeeded=%d failedShort=%d failedLong=%d elapsed=%v",
		N, succeeded, failedShort, failedLong, time.Since(start))
	require.NotZero(t, succeeded, "no Dial succeeded — suspicious")

	// Tear down the acceptor goroutine.
	_ = l.Close()
	<-acceptDone
}

// TestInproc_TimeoutRaceDoesNotDropListener is the focused regression test
// for the Track-5B race: D1 has a short timeout whose expiry coincides with
// D2's expected Listen wake. Before the fix, D1's cancel could win the
// pending-slice removal while Listen concurrently notified, causing either
// D1 to be both removed and "notified" (listener dropped into an orphaned
// channel) or, in some orderings, D2 to miss the wake.
//
// With the CAS-arbitrated waiter, this can no longer happen:
//   - If D1's timeout wins the CAS, D1 reports timeout and Listen skips it
//     when iterating; D2's waiter is untouched and Listen's CAS wins there,
//     so D2 receives its listener.
//   - If Listen wins D1's CAS before the timeout, D1 finds the listener on
//     its resultCh (via cancelWaiter's drain) and succeeds anyway.
//
// Run with -race -count=20.
func TestInproc_TimeoutRaceDoesNotDropListener(t *testing.T) {
	const iterations = 50
	for iter := 0; iter < iterations; iter++ {
		tr := NewInprocTransport(2 * time.Second)

		var d1Err atomic.Value // error (may be nil)
		var d1Succeeded atomic.Bool
		var d2Err atomic.Value
		var d2Succeeded atomic.Bool

		var wg sync.WaitGroup
		wg.Add(2)

		// D1: short timeout — engineered to fire around the same time Listen
		// lands.
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			c, err := tr.Dial(ctx, "inproc://race-narrow")
			if c != nil {
				_ = c.Close()
				d1Succeeded.Store(true)
			}
			if err != nil {
				d1Err.Store(err)
			}
		}()

		// D2: long timeout — MUST succeed regardless of D1's fate.
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			c, err := tr.Dial(ctx, "inproc://race-narrow")
			if c != nil {
				_ = c.Close()
				d2Succeeded.Store(true)
			}
			if err != nil {
				d2Err.Store(err)
			}
		}()

		// Listen at ~50ms — intentionally racing D1's timeout.
		time.Sleep(50 * time.Millisecond)
		runtimeGosched()
		l, err := tr.Listen(context.Background(), "inproc://race-narrow")
		require.NoError(t, err, "iter %d: Listen", iter)
		// Drain accepts so dialIn hand-offs complete.
		acceptDone := make(chan struct{})
		go func() {
			defer close(acceptDone)
			for {
				c, err := l.Accept(context.Background())
				if err != nil {
					return
				}
				_ = c.Close()
			}
		}()

		wg.Wait()

		// D2 MUST have succeeded. This is the invariant the race broke.
		if !d2Succeeded.Load() {
			var e error
			if v := d2Err.Load(); v != nil {
				e = v.(error)
			}
			require.True(t, d2Succeeded.Load(), "iter %d: D2 failed — listener was dropped due to race: err=%v", iter, e)
		}
		// D1 may have succeeded (Listen arrived before its timeout) or
		// returned ctx.DeadlineExceeded / ErrEndpointInvalid. No other
		// error is acceptable.
		if !d1Succeeded.Load() {
			if v := d1Err.Load(); v != nil {
				e := v.(error)
				ok := errors.Is(e, context.DeadlineExceeded) || errors.Is(e, zmqerr.ErrEndpointInvalid)
				require.True(t, ok, "iter %d: D1 unexpected err = %v", iter, e)
			}
		}

		_ = l.Close()
		<-acceptDone
	}
}

// runtimeGosched is a thin wrapper so the test file doesn't need to import
// "runtime" directly (the import would be unused in most tests).
func runtimeGosched() {
	// Yield to let the dialing goroutines progress toward their select.
	time.Sleep(time.Microsecond)
}

// TestInproc_ManyConcurrentDials sanity-checks the listener handles
// several concurrent Dials. The acceptCh buffer is 16; we drive 32 dials
// with a worker draining Accept in parallel.
func TestInproc_ManyConcurrentDials(t *testing.T) {
	tr := NewInprocTransport(0)
	l, err := tr.Listen(context.Background(), "inproc://many")
	require.NoError(t, err)
	defer l.Close()

	const N = 32
	var wg sync.WaitGroup
	// Acceptor.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			c, err := l.Accept(context.Background())
			if err != nil {
				assert.NoError(t, err, "Accept[%d]", i)
				return
			}
			c.Close()
		}
	}()
	// Dialers.
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			c, err := tr.Dial(context.Background(), "inproc://many")
			if err != nil {
				assert.NoError(t, err, "Dial")
				return
			}
			c.Close()
		}()
	}
	wg.Wait()
}
