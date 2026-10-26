package session

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/transport"
)

// Reconnect test doubles. We don't exercise the real Session machinery
// here — the goal is to verify Reconnector's state flow (dial → start →
// run ends → backoff → reconnect) in isolation. Interop tests exercise
// reconnection end-to-end against a real peer.

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

// TestReconnector_DialRetriesThenSucceeds exercises the full
// dial-fails-then-succeeds-then-disconnects-then-reconnects cycle. We
// fail the first two dials, succeed the third, end that run to simulate
// a disconnect, then verify the loop reconnects (fourth dial, second run).
//
// Not parallel: it compares real backoff gaps (gap2 > gap1/2), which a
// loaded scheduler can invert.
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
	first := testutil.RequireRecv(t, started, 2*time.Second, "no run after 2s, dials=%d", dialN.Load())
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
	testutil.RequireRecv(t, started, 2*time.Second, "no reconnect after 2s, dials=%d", dialN.Load())
	assert.Equal(t, int32(4), dialN.Load(), "dials")
}

// TestReconnector_ContextCancellationStopsLoop verifies that an external
// ctx cancel surfaces promptly and the loop exits.
func TestReconnector_ContextCancellationStopsLoop(t *testing.T) {
	t.Parallel()
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
	testutil.RequireRecv(t, dialCh, time.Second, "no dial within 1s")
	cancel()

	stopped := testutil.Async(r.Stop)
	require.NoError(t, testutil.RequireRecv(t, stopped, time.Second, "Stop did not return within 1s after ctx cancel"))
}

// TestReconnector_StopWakesSleepingBackoff: force a long backoff, then
// Stop and assert return is far under the backoff interval.
func TestReconnector_StopWakesSleepingBackoff(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
		got := testutil.RequireRecv(t, retries, time.Second, "no retry after unattached run, want attempt %d", want)
		require.Equal(t, want, got, "attempt counter reset by an unattached run")
	}
	require.GreaterOrEqual(t, starts.Load(), int32(3), "loop did not redial")
}

// TestReconnector_AttachedRunResetsBackoff: after an attached run ends,
// the next retry is attempt 1 at the initial backoff again.
func TestReconnector_AttachedRunResetsBackoff(t *testing.T) {
	t.Parallel()
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
		require.Equal(t, want, testutil.RequireRecv(t, retries, time.Second, "no retry, want %+v", want))
	}
}

// TestReconnector_QuiesceDuringDialBackoff: Quiesce arriving while the
// loop sleeps off a dial failure must prevent the NEXT dial attempt —
// otherwise a closing socket would get a brand-new session attached.
func TestReconnector_QuiesceDuringDialBackoff(t *testing.T) {
	t.Parallel()
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

// TestApplyJitter: every sample lands in [base-base*frac, base+base*frac],
// never below zero, the distribution is symmetric about base, and
// normalize clamps the fraction into [0, 1] first.
func TestApplyJitter(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		base      time.Duration
		frac      float64 // as configured; normalize clamps it
		symmetric bool    // expect samples on both sides of base
	}{
		{"20 percent", 100 * time.Millisecond, 0.2, true},
		{"zero is identity", 250 * time.Millisecond, 0, false},
		{"full fraction at 1ns", time.Nanosecond, 1.0, false},
		{"clamped above", 100 * time.Millisecond, 5.0, true},
		{"clamped below", 100 * time.Millisecond, -3.0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := ReconnectConfig{JitterFraction: tc.frac}
			cfg.normalize()
			frac := min(max(tc.frac, 0), 1)
			require.Equal(t, frac, cfg.JitterFraction, "normalize clamp")
			lo, hi := tc.base-time.Duration(float64(tc.base)*frac), tc.base+time.Duration(float64(tc.base)*frac)
			var sum float64
			var sawBelow, sawAbove bool
			const n = 10000
			for i := 0; i < n; i++ {
				d := applyJitter(tc.base, cfg.JitterFraction)
				require.GreaterOrEqual(t, d, max(lo, 0), "sample %d", i)
				require.LessOrEqual(t, d, hi, "sample %d", i)
				sum += float64(d)
				sawBelow, sawAbove = sawBelow || d < tc.base, sawAbove || d > tc.base
			}
			if tc.symmetric {
				require.True(t, sawBelow && sawAbove, "samples on both sides of base")
				mean := time.Duration(sum / n)
				require.Less(t, (mean - tc.base).Abs(), time.Duration(float64(tc.base)*0.02), "mean drift")
			}
		})
	}
}

// TestNextBackoff covers the exponential-doubling and cap logic.
func TestNextBackoff(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			got := nextBackoff(tc.base, tc.maxCap)
			assert.Equal(t, tc.wantNext, got, "nextBackoff(%v,%v)", tc.base, tc.maxCap)
		})
	}
}
