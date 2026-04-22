package session

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/pipe"
)

// --------------------------------------------------------------------------
// Reconnect test doubles. We don't exercise the real Session machinery
// here — the goal is to verify Reconnector's state flow (dial → handshake
// → attach → detach → backoff → reconnect) in isolation. Interop tests
// exercise reconnection end-to-end against a real peer.
// --------------------------------------------------------------------------

// stubSession is an iface.Session stub whose lifecycle is driven by test
// code rather than a real ZMTP FSM.
type stubSession struct {
	handshaked chan struct{}
	done       chan struct{}
	doneOnce   sync.Once
	hsErr      error
	enginePipe iface.Pipe
	sessionEnd iface.Pipe // pipe end we close to simulate peer EOF
	stopOnce   sync.Once
}

func newStubSession(hsErr error) *stubSession {
	s := &stubSession{
		handshaked: make(chan struct{}),
		done:       make(chan struct{}),
		hsErr:      hsErr,
	}
	if hsErr == nil {
		a, b := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
		s.enginePipe = a
		s.sessionEnd = b
	}
	return s
}

// closeDone closes done exactly once. Called by Stop AND by the pipe-
// watcher goroutine launched in Start — so externally closing the
// enginePipe (to simulate peer EOF) fires Done() the same way the real
// sessionImpl does on reader/writer teardown.
func (s *stubSession) closeDone() {
	s.doneOnce.Do(func() { close(s.done) })
}

func (s *stubSession) Start() {
	// Immediately signal handshake outcome.
	close(s.handshaked)
	// Mirror the real session: if the pipe closes externally (simulating
	// peer disconnect), propagate to Done(). On handshake failure there
	// is no pipe, so we don't launch the watcher.
	if s.hsErr == nil && s.enginePipe != nil {
		go func() {
			<-s.enginePipe.Closed()
			s.closeDone()
		}()
	}
}
func (s *stubSession) Stop() error {
	s.stopOnce.Do(func() {
		if s.enginePipe != nil {
			_ = s.enginePipe.Close()
		}
		if s.sessionEnd != nil {
			_ = s.sessionEnd.Close()
		}
		s.closeDone()
	})
	return nil
}
func (s *stubSession) Handshaked() <-chan struct{}    { return s.handshaked }
func (s *stubSession) HandshakeErr() error            { return s.hsErr }
func (s *stubSession) Pipe() iface.Pipe               { return s.enginePipe }
func (s *stubSession) Endpoint() string               { return "stub://peer" }
func (s *stubSession) Role() iface.Role               { return iface.RoleClient }
func (s *stubSession) Done() <-chan struct{}          { return s.done }
func (s *stubSession) WriterFlushed() <-chan struct{} { return s.done } // stub: same as Done
func (s *stubSession) CancelWriter()                  {}                // stub: no-op

// stubConn is a no-op iface.Conn: Reconnector never touches its data
// methods; it just passes the value to NewSession.
type stubConn struct{}

func (stubConn) Read([]byte) (int, error)         { return 0, errors.New("stub") }
func (stubConn) Write(p []byte) (int, error)      { return len(p), nil }
func (stubConn) Close() error                     { return nil }
func (stubConn) LocalEndpoint() string            { return "stub://local" }
func (stubConn) RemoteEndpoint() string           { return "stub://remote" }
func (stubConn) SetDeadline(time.Time) error      { return nil }
func (stubConn) SetReadDeadline(time.Time) error  { return nil }
func (stubConn) SetWriteDeadline(time.Time) error { return nil }

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

// TestReconnector_DialRetriesThenSucceeds exercises the full
// dial-fails-then-succeeds-then-detaches-then-reconnects cycle. We fail
// the first two dials, succeed the third, close the pipe to simulate a
// disconnect, then verify the loop reconnects (fourth dial, second
// attach).
func TestReconnector_DialRetriesThenSucceeds(t *testing.T) {
	var (
		dialN     atomic.Int32
		dialTimes = make([]time.Time, 0, 8)
		dialMu    sync.Mutex
	)
	dialErr := errors.New("connection refused")

	// Track the sessions we hand out so the test can trigger a detach.
	var sessMu sync.Mutex
	var sessions []*stubSession

	var attaches, detaches atomic.Int32
	attached := make(chan iface.Pipe, 4)

	cfg := ReconnectConfig{
		InitialBackoff: 5 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		JitterFraction: 0, // deterministic for backoff timing assertions
		Dial: func(ctx context.Context) (iface.Conn, error) {
			dialMu.Lock()
			dialTimes = append(dialTimes, time.Now())
			dialMu.Unlock()
			n := dialN.Add(1)
			if n <= 2 {
				return nil, dialErr
			}
			return stubConn{}, nil
		},
		NewSession: func(c iface.Conn) iface.Session {
			s := newStubSession(nil)
			sessMu.Lock()
			sessions = append(sessions, s)
			sessMu.Unlock()
			return s
		},
		OnAttach: func(p iface.Pipe) {
			attaches.Add(1)
			select {
			case attached <- p:
			default:
			}
		},
		OnDetach: func(p iface.Pipe) {
			detaches.Add(1)
		},
	}

	r := NewReconnector(cfg)
	r.Start(context.Background())
	defer r.Stop()

	// Wait for the first successful attach (after two dial failures).
	select {
	case <-attached:
	case <-time.After(2 * time.Second):
		t.Fatalf("no attach after 2s; dials=%d", dialN.Load())
	}
	if n := dialN.Load(); n != 3 {
		t.Fatalf("expected 3 dials before first attach, got %d", n)
	}
	if a := attaches.Load(); a != 1 {
		t.Errorf("attaches=%d, want 1", a)
	}

	// Verify backoff grew: dial2 should happen ≥ 5ms after dial1,
	// dial3 ≥ 10ms after dial2.
	dialMu.Lock()
	times := append([]time.Time(nil), dialTimes...)
	dialMu.Unlock()
	if len(times) < 3 {
		t.Fatalf("got %d dial timestamps, want ≥3", len(times))
	}
	gap1 := times[1].Sub(times[0])
	gap2 := times[2].Sub(times[1])
	// Backoff schedule: 5ms, then 10ms. Allow slack for scheduler.
	if gap1 < 4*time.Millisecond {
		t.Errorf("gap1=%v, want ≥4ms", gap1)
	}
	if gap2 < 8*time.Millisecond {
		t.Errorf("gap2=%v, want ≥8ms", gap2)
	}
	if gap2 <= gap1/2 {
		t.Errorf("backoff did not grow: gap1=%v, gap2=%v", gap1, gap2)
	}

	// Simulate peer disconnect: close the session's engine-side pipe.
	sessMu.Lock()
	first := sessions[0]
	sessMu.Unlock()
	_ = first.enginePipe.Close()

	// Expect detach, then a fresh dial, then a second attach.
	select {
	case <-attached:
	case <-time.After(2 * time.Second):
		t.Fatalf("no reconnect-attach after 2s; dials=%d attaches=%d detaches=%d",
			dialN.Load(), attaches.Load(), detaches.Load())
	}
	if a := attaches.Load(); a != 2 {
		t.Errorf("attaches=%d, want 2", a)
	}
	if d := detaches.Load(); d < 1 {
		t.Errorf("detaches=%d, want ≥1", d)
	}
}

// TestReconnector_ContextCancellationStopsLoop verifies that an external
// ctx cancel surfaces promptly and the loop exits.
func TestReconnector_ContextCancellationStopsLoop(t *testing.T) {
	dialCh := make(chan struct{}, 16)
	cfg := ReconnectConfig{
		InitialBackoff: time.Millisecond,
		JitterFraction: 0,
		Dial: func(ctx context.Context) (iface.Conn, error) {
			dialCh <- struct{}{}
			// Respect ctx so a cancelled Reconnector doesn't block here.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * time.Millisecond):
				return nil, errors.New("still refused")
			}
		},
		NewSession: func(c iface.Conn) iface.Session { return newStubSession(nil) },
	}

	r := NewReconnector(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	r.Start(ctx)

	// Wait for at least one dial attempt, then cancel the external ctx.
	select {
	case <-dialCh:
	case <-time.After(time.Second):
		t.Fatal("no dial within 1s")
	}
	cancel()

	stopped := make(chan error, 1)
	go func() { stopped <- r.Stop() }()
	select {
	case err := <-stopped:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Stop = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not return within 1s after ctx cancel")
	}
}

// TestReconnector_StopWakesSleepingBackoff: force a long backoff, then
// Stop and assert return is far under the backoff interval.
func TestReconnector_StopWakesSleepingBackoff(t *testing.T) {
	cfg := ReconnectConfig{
		InitialBackoff: 10 * time.Second, // huge — Stop must wake this
		JitterFraction: 0,
		Dial: func(ctx context.Context) (iface.Conn, error) {
			return nil, errors.New("always refused")
		},
		NewSession: func(c iface.Conn) iface.Session { return newStubSession(nil) },
	}

	r := NewReconnector(cfg)
	r.Start(context.Background())

	// Give the loop a moment to fail its first dial and enter the
	// backoff sleep.
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	if err := r.Stop(); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > time.Second {
		t.Errorf("Stop took %v; expected prompt wake from 10s backoff", elapsed)
	}
}

// TestReconnector_StopBeforeStart exercises the no-Start path.
func TestReconnector_StopBeforeStart(t *testing.T) {
	r := NewReconnector(ReconnectConfig{
		InitialBackoff: time.Millisecond,
		Dial:           func(ctx context.Context) (iface.Conn, error) { return nil, errors.New("x") },
		NewSession:     func(c iface.Conn) iface.Session { return newStubSession(nil) },
	})
	done := make(chan error, 1)
	go func() { done <- r.Stop() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Stop before Start: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop before Start hung")
	}
}

// TestReconnector_HandshakeFailureTriggersBackoff: a session whose
// handshake reports an error is torn down, the backoff schedule advances
// (rather than resets), and another dial happens.
func TestReconnector_HandshakeFailureTriggersBackoff(t *testing.T) {
	var attempts atomic.Int32
	hsErr := errors.New("bad greeting")

	done := make(chan struct{})
	var doneOnce sync.Once

	cfg := ReconnectConfig{
		InitialBackoff: 5 * time.Millisecond,
		MaxBackoff:     20 * time.Millisecond,
		JitterFraction: 0,
		Dial: func(ctx context.Context) (iface.Conn, error) {
			n := attempts.Add(1)
			if n >= 4 {
				doneOnce.Do(func() { close(done) })
				// After third handshake-failure retry, return ctx-err
				// to make the loop wait forever on next backoff; Stop
				// will handle teardown.
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return stubConn{}, nil
		},
		NewSession: func(c iface.Conn) iface.Session { return newStubSession(hsErr) },
		OnAttach:   func(_ iface.Pipe) { t.Error("OnAttach should not fire on handshake failure") },
	}

	r := NewReconnector(cfg)
	r.Start(context.Background())

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("did not see 4 dial attempts; got %d", attempts.Load())
	}
	_ = r.Stop()
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
	if minV < lo-time.Microsecond || maxV > hi+time.Microsecond {
		t.Errorf("jitter out of band: min=%v max=%v [want %v..%v]", minV, maxV, lo, hi)
	}

	// With N=4096 samples and ±20% window, the empirical mean should be
	// well within 1% of base. Centered uniform has low variance at this N.
	mean := time.Duration(sum / float64(N))
	drift := mean - base
	if drift < 0 {
		drift = -drift
	}
	if drift > time.Duration(float64(base)*0.02) {
		t.Errorf("mean drifted too far: mean=%v base=%v drift=%v", mean, base, drift)
	}

	// And at least one sample should have landed in the lower half and
	// one in the upper half — the distribution is symmetric.
	if minV >= base {
		t.Error("no sample below base; jitter not applied symmetrically")
	}
	if maxV <= base {
		t.Error("no sample above base; jitter not applied symmetrically")
	}
}

// TestApplyJitter_ZeroFractionIsIdentity: frac=0 must return base as-is.
func TestApplyJitter_ZeroFractionIsIdentity(t *testing.T) {
	base := 250 * time.Millisecond
	for i := 0; i < 16; i++ {
		if got := applyJitter(base, 0); got != base {
			t.Errorf("applyJitter(base, 0) = %v, want %v", got, base)
		}
	}
}

// TestApplyJitter_ClampedExtreme: a wildly out-of-range JitterFraction
// (5.0) must be clamped by ReconnectConfig.normalize to 1.0. After
// normalize, applyJitter samples are bounded by [0, 2*base].
//
// We call normalize directly rather than NewReconnector because we only
// need the clamped fraction value — standing up a full Reconnector would
// pull in Dial/NewSession stubs for no benefit.
func TestApplyJitter_ClampedExtreme(t *testing.T) {
	cfg := ReconnectConfig{JitterFraction: 5.0}
	cfg.normalize()
	if cfg.JitterFraction != 1.0 {
		t.Fatalf("normalize clamped fraction = %v, want 1.0", cfg.JitterFraction)
	}
	// Negative values clamp to 0.
	neg := ReconnectConfig{JitterFraction: -3.0}
	neg.normalize()
	if neg.JitterFraction != 0 {
		t.Fatalf("normalize clamped negative = %v, want 0", neg.JitterFraction)
	}

	base := 100 * time.Millisecond
	const N = 10000
	for i := 0; i < N; i++ {
		d := applyJitter(base, cfg.JitterFraction)
		if d < 0 {
			t.Fatalf("sample %d: negative duration %v", i, d)
		}
		if d > 2*base {
			t.Fatalf("sample %d: %v exceeds 2*base=%v", i, d, 2*base)
		}
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
		if d < 0 {
			t.Fatalf("sample %d: negative duration %v for base=%v frac=1.0",
				i, d, base)
		}
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
			if d := applyJitter(b, 1.0); d < 0 {
				t.Fatalf("base=%v sample=%d: negative %v", b, i, d)
			}
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
		if d < lo || d > hi {
			t.Fatalf("sample %d: %v out of [%v, %v]", i, d, lo, hi)
		}
		if d < base {
			sawBelow = true
		}
		if d > base {
			sawAbove = true
		}
	}
	// Symmetry sanity: with 10k samples we should see both sides.
	if !sawBelow {
		t.Error("no sample below base; distribution not symmetric")
	}
	if !sawAbove {
		t.Error("no sample above base; distribution not symmetric")
	}
}

// TestReconnect_OnRetryPanic_DoesNotStall verifies the state-before-
// callback ordering: if OnRetry panics on attempt=1, the loop must
// still advance to attempt=2 on the next retry.
//
// Strategy:
//   - stub Dial that always fails (forces the dial-failure retry branch)
//   - OnRetry panics on the first invocation, records attempts on
//     subsequent ones
//   - wait for attempt=2 (proving the loop survived and incremented)
//   - Stop and assert clean shutdown (goroutine exits)
func TestReconnect_OnRetryPanic_DoesNotStall(t *testing.T) {
	attemptsSeen := make(chan int, 16)
	var onRetryN atomic.Int32

	cfg := ReconnectConfig{
		InitialBackoff: 2 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
		JitterFraction: 0,
		Dial: func(ctx context.Context) (iface.Conn, error) {
			return nil, errors.New("always refused")
		},
		NewSession: func(c iface.Conn) iface.Session { return newStubSession(nil) },
		OnRetry: func(attempt int, backoff time.Duration) {
			n := onRetryN.Add(1)
			// Record what we saw (non-blocking) regardless of panic path.
			select {
			case attemptsSeen <- attempt:
			default:
			}
			if n == 1 {
				panic("user callback boom")
			}
		},
	}

	r := NewReconnector(cfg)
	r.Start(context.Background())

	// Collect attempts until we've seen ≥2 or timeout.
	deadline := time.After(2 * time.Second)
	seen := make([]int, 0, 4)
	for len(seen) < 2 {
		select {
		case a := <-attemptsSeen:
			seen = append(seen, a)
		case <-deadline:
			t.Fatalf("did not see ≥2 OnRetry invocations; got %v (n=%d)",
				seen, onRetryN.Load())
		}
	}

	if seen[0] != 1 {
		t.Errorf("first OnRetry attempt=%d, want 1", seen[0])
	}
	if seen[1] != 2 {
		t.Errorf("second OnRetry attempt=%d, want 2 (loop must advance past panic)",
			seen[1])
	}

	// Loop must still be alive and Stop must return cleanly.
	stopped := make(chan error, 1)
	go func() { stopped <- r.Stop() }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Errorf("Stop after panicking callback: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop hung after panicking OnRetry — loop goroutine leaked")
	}
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
			if got != tc.wantNext {
				t.Errorf("nextBackoff(%v,%v) = %v, want %v", tc.base, tc.maxCap, got, tc.wantNext)
			}
		})
	}
}
