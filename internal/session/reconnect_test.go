package session

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/transport"
)

// --------------------------------------------------------------------------
// Reconnect test doubles. We don't exercise the real Session machinery
// here — the goal is to verify Reconnector's state flow (dial → start →
// run ends → backoff → reconnect) in isolation. Interop tests exercise
// reconnection end-to-end against a real peer.
// --------------------------------------------------------------------------

// stubRunner is a Runner whose lifecycle is driven by test code: end()
// simulates the connection ending on its own, Stop a local teardown.
type stubRunner struct {
	attached bool
	done     chan struct{}
	once     sync.Once
	stops    atomic.Int32
	stopErr  error
}

func newStubRunner(attached bool) *stubRunner {
	return &stubRunner{attached: attached, done: make(chan struct{})}
}

func (r *stubRunner) end()                  { r.once.Do(func() { close(r.done) }) }
func (r *stubRunner) Done() <-chan struct{} { return r.done }
func (r *stubRunner) Attached() bool        { return r.attached }
func (r *stubRunner) Stop() error {
	r.stops.Add(1)
	r.end()
	return r.stopErr
}

// endedRunner returns a stub that has already ended, unattached — a failed
// handshake or a refused attach, as the Reconnector sees them.
func endedRunner() *stubRunner {
	r := newStubRunner(false)
	r.end()
	return r
}

// stubConn is a no-op transport.Conn: Reconnector never touches its data
// methods; it just passes the value to Start.
type stubConn struct{}

func (stubConn) Read([]byte) (int, error)    { return 0, errors.New("stub") }
func (stubConn) Write(p []byte) (int, error) { return len(p), nil }
func (stubConn) Close() error                { return nil }
func (stubConn) LocalEndpoint() string       { return "stub://local" }
func (stubConn) RemoteEndpoint() string      { return "stub://remote" }
func (stubConn) SetDeadline(time.Time) error { return nil }

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

// TestReconnector_DialRetriesThenSucceeds exercises the full
// dial-fails-then-succeeds-then-disconnects-then-reconnects cycle. We
// fail the first two dials, succeed the third, end that run to simulate
// a disconnect, then verify the loop reconnects (fourth dial, second run).
func TestReconnector_DialRetriesThenSucceeds(t *testing.T) {
	var (
		dialN     atomic.Int32
		dialTimes = make([]time.Time, 0, 8)
		dialMu    sync.Mutex
	)
	dialErr := errors.New("connection refused")
	started := make(chan *stubRunner, 4)

	cfg := ReconnectConfig{
		InitialBackoff: 5 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		JitterFraction: 0, // deterministic for backoff timing assertions
		Dial: func(ctx context.Context) (transport.Conn, error) {
			dialMu.Lock()
			dialTimes = append(dialTimes, time.Now())
			dialMu.Unlock()
			if dialN.Add(1) <= 2 {
				return nil, dialErr
			}
			return stubConn{}, nil
		},
		Start: func(transport.Conn) (Runner, error) {
			r := newStubRunner(true)
			started <- r
			return r, nil
		},
	}

	r := StartReconnector(context.Background(), cfg)
	defer r.Stop()

	// Wait for the first run (after two dial failures).
	var first *stubRunner
	select {
	case first = <-started:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "no run after 2s", "dials=%d", dialN.Load())
	}
	require.Equal(t, int32(3), dialN.Load(), "expected 3 dials before first run")

	// Verify backoff grew: dial2 should happen ≥ 5ms after dial1,
	// dial3 ≥ 10ms after dial2.
	dialMu.Lock()
	times := append([]time.Time(nil), dialTimes...)
	dialMu.Unlock()
	require.GreaterOrEqual(t, len(times), 3, "got too few dial timestamps")
	gap1 := times[1].Sub(times[0])
	gap2 := times[2].Sub(times[1])
	// Backoff schedule: 5ms, then 10ms. Allow slack for scheduler.
	assert.GreaterOrEqual(t, gap1, 4*time.Millisecond, "gap1")
	assert.GreaterOrEqual(t, gap2, 8*time.Millisecond, "gap2")
	assert.Greater(t, gap2, gap1/2, "backoff did not grow: gap1=%v, gap2=%v", gap1, gap2)

	// Simulate peer disconnect; expect a fresh dial and a second run.
	first.end()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "no reconnect after 2s", "dials=%d", dialN.Load())
	}
	assert.Equal(t, int32(4), dialN.Load(), "dials")
}

// TestReconnector_ContextCancellationStopsLoop verifies that an external
// ctx cancel surfaces promptly and the loop exits.
func TestReconnector_ContextCancellationStopsLoop(t *testing.T) {
	dialCh := make(chan struct{}, 16)
	cfg := ReconnectConfig{
		InitialBackoff: time.Millisecond,
		JitterFraction: 0,
		Dial: func(ctx context.Context) (transport.Conn, error) {
			dialCh <- struct{}{}
			// Respect ctx so a cancelled Reconnector doesn't block here.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * time.Millisecond):
				return nil, errors.New("still refused")
			}
		},
		Start: func(transport.Conn) (Runner, error) { return newStubRunner(true), nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := StartReconnector(ctx, cfg)

	// Wait for at least one dial attempt, then cancel the external ctx.
	select {
	case <-dialCh:
	case <-time.After(time.Second):
		require.FailNow(t, "no dial within 1s")
	}
	cancel()

	stopped := make(chan error, 1)
	go func() { stopped <- r.Stop() }()
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.FailNow(t, "Stop did not return within 1s after ctx cancel")
	}
}

// TestReconnector_StopWakesSleepingBackoff: force a long backoff, then
// Stop and assert return is far under the backoff interval.
func TestReconnector_StopWakesSleepingBackoff(t *testing.T) {
	cfg := ReconnectConfig{
		InitialBackoff: 10 * time.Second, // huge — Stop must wake this
		JitterFraction: 0,
		Dial: func(ctx context.Context) (transport.Conn, error) {
			return nil, errors.New("always refused")
		},
		Start: func(transport.Conn) (Runner, error) { return newStubRunner(true), nil },
	}

	r := StartReconnector(context.Background(), cfg)

	// Give the loop a moment to fail its first dial and enter the
	// backoff sleep.
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	require.NoError(t, r.Stop())
	elapsed := time.Since(start)
	assert.LessOrEqual(t, elapsed, time.Second, "Stop expected prompt wake from 10s backoff")
}

// TestReconnector_StopStopsLiveRunner: Stop tears down the live run and
// returns its Stop error.
func TestReconnector_StopStopsLiveRunner(t *testing.T) {
	run := newStubRunner(true)
	run.stopErr = errors.New("close failed")
	started := make(chan struct{})
	cfg := ReconnectConfig{
		Dial: func(context.Context) (transport.Conn, error) { return stubConn{}, nil },
		Start: func(transport.Conn) (Runner, error) {
			close(started)
			return run, nil
		},
	}
	r := StartReconnector(context.Background(), cfg)
	waitClosed(t, started, time.Second, "no run started")
	require.ErrorIs(t, r.Stop(), run.stopErr)
	require.Equal(t, int32(1), run.stops.Load(), "live run not stopped exactly once")
}

// TestReconnector_UnattachedRunAdvancesBackoff: a run that ends without
// attaching — a failed handshake, or a pipe the engine refused — counts
// as a failed attempt: the loop redials, and the attempt counter keeps
// advancing instead of resetting as it does after an attached run.
func TestReconnector_UnattachedRunAdvancesBackoff(t *testing.T) {
	var starts atomic.Int32
	retries := make(chan int, 16)
	cfg := ReconnectConfig{
		InitialBackoff: time.Millisecond,
		Dial:           func(ctx context.Context) (transport.Conn, error) { return stubConn{}, nil },
		Start: func(transport.Conn) (Runner, error) {
			starts.Add(1)
			return endedRunner(), nil
		},
		OnRetry: func(attempt int, _ time.Duration) {
			select {
			case retries <- attempt:
			default:
			}
		},
	}
	r := StartReconnector(context.Background(), cfg)
	defer r.Stop()

	for want := 1; want <= 3; want++ {
		select {
		case got := <-retries:
			require.Equal(t, want, got, "attempt counter reset by an unattached run")
		case <-time.After(time.Second):
			require.FailNow(t, "no retry after unattached run", "want attempt %d", want)
		}
	}
	require.GreaterOrEqual(t, starts.Load(), int32(3), "loop did not redial")
}

// TestReconnector_AttachedRunResetsBackoff: after an attached run ends,
// the next retry is attempt 1 at the initial backoff again.
func TestReconnector_AttachedRunResetsBackoff(t *testing.T) {
	var starts atomic.Int32
	type retry struct {
		attempt int
		backoff time.Duration
	}
	retries := make(chan retry, 16)
	cfg := ReconnectConfig{
		InitialBackoff: time.Millisecond,
		Dial:           func(ctx context.Context) (transport.Conn, error) { return stubConn{}, nil },
		Start: func(transport.Conn) (Runner, error) {
			// Two failed runs, then one that attached, then failures.
			if starts.Add(1) == 3 {
				r := newStubRunner(true)
				r.end()
				return r, nil
			}
			return endedRunner(), nil
		},
		OnRetry: func(attempt int, backoff time.Duration) {
			select {
			case retries <- retry{attempt, backoff}:
			default:
			}
		},
	}
	r := StartReconnector(context.Background(), cfg)
	defer r.Stop()

	for _, want := range []retry{{1, time.Millisecond}, {2, 2 * time.Millisecond}, {1, time.Millisecond}} {
		select {
		case got := <-retries:
			require.Equal(t, want, got)
		case <-time.After(time.Second):
			require.FailNow(t, "no retry", "want %+v", want)
		}
	}
}

// TestReconnector_QuiesceDuringDialBackoff: Quiesce arriving while the
// loop sleeps off a dial failure must prevent the NEXT dial attempt —
// otherwise a closing socket would get a brand-new session attached.
func TestReconnector_QuiesceDuringDialBackoff(t *testing.T) {
	var dials atomic.Int32
	dialed := make(chan struct{}, 16)
	cfg := ReconnectConfig{
		InitialBackoff: 20 * time.Millisecond,
		JitterFraction: 0,
		Dial: func(ctx context.Context) (transport.Conn, error) {
			dials.Add(1)
			select {
			case dialed <- struct{}{}:
			default:
			}
			return nil, errors.New("refused")
		},
		Start: func(transport.Conn) (Runner, error) {
			assert.Fail(t, "Start must not run after Quiesce")
			return endedRunner(), nil
		},
	}

	r := StartReconnector(context.Background(), cfg)
	defer r.Stop()

	// Wait for the first (failing) dial; the loop is now in (or headed
	// for) its backoff sleep. Quiesce before the sleep elapses.
	waitClosed(t, dialed, time.Second, "no dial within 1s")
	r.Quiesce()

	// The loop must exit at the top of the next iteration instead of
	// dialing again. r.done is in-package; observing it directly proves
	// the loop exited rather than parked.
	waitClosed(t, r.done, time.Second, "loop did not exit after Quiesce during backoff")
	require.LessOrEqual(t, dials.Load(), int32(2), "loop kept dialing after Quiesce")
}

// TestApplyJitter_WithinBand samples the jitter helper many times and
// asserts results land inside the expected symmetric window.
func TestApplyJitter_WithinBand(t *testing.T) {
	base := 100 * time.Millisecond
	frac := 0.2
	const N = 4096

	var minV, maxV time.Duration = math.MaxInt64, math.MinInt64
	var sum float64
	for i := 0; i < N; i++ {
		d := applyJitter(base, frac)
		if d < minV {
			minV = d
		}
		if d > maxV {
			maxV = d
		}
		sum += float64(d)
	}

	lo := time.Duration(float64(base) * (1 - frac))
	hi := time.Duration(float64(base) * (1 + frac))
	assert.GreaterOrEqual(t, minV, lo-time.Microsecond, "jitter min out of band: min=%v [want ≥%v]", minV, lo)
	assert.LessOrEqual(t, maxV, hi+time.Microsecond, "jitter max out of band: max=%v [want ≤%v]", maxV, hi)

	// With N=4096 samples and ±20% window, the empirical mean should be
	// well within 1% of base. Centered uniform has low variance at this N.
	mean := time.Duration(sum / float64(N))
	drift := mean - base
	if drift < 0 {
		drift = -drift
	}
	assert.LessOrEqual(t, drift, time.Duration(float64(base)*0.02),
		"mean drifted too far: mean=%v base=%v drift=%v", mean, base, drift)

	// And at least one sample should have landed in the lower half and
	// one in the upper half — the distribution is symmetric.
	assert.Less(t, minV, base, "no sample below base; jitter not applied symmetrically")
	assert.Greater(t, maxV, base, "no sample above base; jitter not applied symmetrically")
}

// TestApplyJitter_ZeroFractionIsIdentity: frac=0 must return base as-is.
func TestApplyJitter_ZeroFractionIsIdentity(t *testing.T) {
	base := 250 * time.Millisecond
	for i := 0; i < 16; i++ {
		assert.Equal(t, base, applyJitter(base, 0), "applyJitter(base, 0)")
	}
}

// TestApplyJitter_ClampedExtreme: a wildly out-of-range JitterFraction
// (5.0) must be clamped by ReconnectConfig.normalize to 1.0. After
// normalize, applyJitter samples are bounded by [0, 2*base].
//
// We call normalize directly rather than StartReconnector because we only
// need the clamped fraction value — standing up a full Reconnector would
// pull in Dial/NewSession stubs for no benefit.
func TestApplyJitter_ClampedExtreme(t *testing.T) {
	cfg := ReconnectConfig{JitterFraction: 5.0}
	cfg.normalize()
	require.Equal(t, 1.0, cfg.JitterFraction, "normalize clamped fraction")
	// Negative values clamp to 0.
	neg := ReconnectConfig{JitterFraction: -3.0}
	neg.normalize()
	require.Equal(t, 0.0, neg.JitterFraction, "normalize clamped negative")

	base := 100 * time.Millisecond
	const N = 10000
	for i := 0; i < N; i++ {
		d := applyJitter(base, cfg.JitterFraction)
		require.GreaterOrEqual(t, d, time.Duration(0), "sample %d: negative duration %v", i, d)
		require.LessOrEqual(t, d, 2*base, "sample %d: %v exceeds 2*base=%v", i, d, 2*base)
	}
}

// TestApplyJitter_NeverNegative: the smallest meaningful base (1 ns)
// with maximum fraction (1.0) must never produce a negative duration
// across 10k samples. Exercises the integer-arithmetic boundary where
// the float-based formula was prone to return small negatives.
func TestApplyJitter_NeverNegative(t *testing.T) {
	base := time.Duration(1) // 1 ns, smallest meaningful positive base
	const N = 10000
	for i := 0; i < N; i++ {
		d := applyJitter(base, 1.0)
		require.GreaterOrEqual(t, d, time.Duration(0),
			"sample %d: negative duration %v for base=%v frac=1.0", i, d, base)
	}

	// Also check a few larger bases at frac=1.0 — the entire range
	// [0, 2*base) must be non-negative.
	for _, b := range []time.Duration{
		10 * time.Nanosecond,
		1 * time.Microsecond,
		1 * time.Millisecond,
		1 * time.Second,
	} {
		for i := 0; i < 1000; i++ {
			require.GreaterOrEqual(t, applyJitter(b, 1.0), time.Duration(0),
				"base=%v sample=%d: negative", b, i)
		}
	}
}

// TestApplyJitter_InRange: fraction=0.2, base=100ms, 10k samples must
// all land in [80ms, 120ms]. Verifies the integer-arithmetic window is
// correct.
func TestApplyJitter_InRange(t *testing.T) {
	base := 100 * time.Millisecond
	frac := 0.2
	lo := base - time.Duration(float64(base)*frac) // 80ms
	hi := base + time.Duration(float64(base)*frac) // 120ms

	const N = 10000
	var sawBelow, sawAbove bool
	for i := 0; i < N; i++ {
		d := applyJitter(base, frac)
		require.GreaterOrEqual(t, d, lo, "sample %d: %v below [%v, %v]", i, d, lo, hi)
		require.LessOrEqual(t, d, hi, "sample %d: %v above [%v, %v]", i, d, lo, hi)
		if d < base {
			sawBelow = true
		}
		if d > base {
			sawAbove = true
		}
	}
	// Symmetry sanity: with 10k samples we should see both sides.
	assert.True(t, sawBelow, "no sample below base; distribution not symmetric")
	assert.True(t, sawAbove, "no sample above base; distribution not symmetric")
}

// TestNextBackoff covers the exponential-doubling and cap logic.
func TestNextBackoff(t *testing.T) {
	cases := []struct {
		name     string
		base     time.Duration
		maxCap   time.Duration
		wantNext time.Duration
	}{
		{"below cap", 10 * time.Millisecond, 100 * time.Millisecond, 20 * time.Millisecond},
		{"at cap", 60 * time.Millisecond, 100 * time.Millisecond, 100 * time.Millisecond},
		{"above cap", 200 * time.Millisecond, 100 * time.Millisecond, 100 * time.Millisecond},
		{"no cap", 50 * time.Millisecond, 0, 100 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := nextBackoff(tc.base, tc.maxCap)
			assert.Equal(t, tc.wantNext, got, "nextBackoff(%v,%v)", tc.base, tc.maxCap)
		})
	}
}
