package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/pipe"
)

// makeLingerPipes returns n connected Pipe pairs. The "engine" ends are
// returned in the first slice; the "session" ends (which DrainOutbound
// never sees directly) are returned in the second — the test drives
// Close on those to simulate a session finishing its drain.
func makeLingerPipes(n int) (engine, session []iface.Pipe) {
	engine = make([]iface.Pipe, n)
	session = make([]iface.Pipe, n)
	for i := 0; i < n; i++ {
		a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
		engine[i] = a
		session[i] = b
	}
	return engine, session
}

// closeAll closes every pipe, ignoring errors. Used in defer.
func closeAll(ps ...iface.Pipe) {
	for _, p := range ps {
		if p != nil {
			_ = p.Close()
		}
	}
}

// TestDrainOutbound_LingerZeroReturnsImmediately: linger=0 means drop,
// so DrainOutbound should return essentially instantly regardless of
// queued messages.
func TestDrainOutbound_LingerZeroReturnsImmediately(t *testing.T) {
	engine, session := makeLingerPipes(3)
	defer closeAll(engine...)
	defer closeAll(session...)

	start := time.Now()
	if err := DrainOutbound(context.Background(), engine, 0); err != nil {
		t.Fatalf("DrainOutbound(linger=0) = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("linger=0 took %v; expected near-zero", elapsed)
	}
}

// TestDrainOutbound_LingerBoundedReturnsOnTimeout: a pipe that never
// closes should cause DrainOutbound to return DeadlineExceeded once the
// linger elapses.
func TestDrainOutbound_LingerBoundedReturnsOnTimeout(t *testing.T) {
	engine, session := makeLingerPipes(1)
	defer closeAll(engine...)
	defer closeAll(session...)

	linger := 80 * time.Millisecond
	start := time.Now()
	err := DrainOutbound(context.Background(), engine, linger)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	// We should have waited approximately `linger`. Allow schedule slack
	// on the upper bound but the lower bound is tight — a premature
	// return would be a real bug.
	if elapsed < 60*time.Millisecond {
		t.Errorf("returned after %v; expected ≥%v", elapsed, linger-20*time.Millisecond)
	}
	if elapsed > 400*time.Millisecond {
		t.Errorf("returned after %v; expected ≤400ms", elapsed)
	}
}

// TestDrainOutbound_DrainCompletesBeforeLinger: if the pipe closes
// before linger elapses, we should return promptly with nil.
func TestDrainOutbound_DrainCompletesBeforeLinger(t *testing.T) {
	engine, session := makeLingerPipes(2)
	defer closeAll(engine...)
	defer closeAll(session...)

	// Trigger both pipes to close after 30ms. DrainOutbound with a 1s
	// linger should return within ~30-100ms.
	go func() {
		time.Sleep(30 * time.Millisecond)
		for _, p := range engine {
			_ = p.Close()
		}
	}()

	linger := time.Second
	start := time.Now()
	if err := DrainOutbound(context.Background(), engine, linger); err != nil {
		t.Fatalf("DrainOutbound = %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 20*time.Millisecond {
		t.Errorf("returned in %v; expected ≥20ms (drain delay)", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("returned in %v; expected <500ms (drain should have completed)", elapsed)
	}
}

// TestDrainOutbound_NegativeLingerWaitsForCtx: linger<0 means wait
// forever, bounded only by ctx. The cancellation path is the only exit.
func TestDrainOutbound_NegativeLingerWaitsForCtx(t *testing.T) {
	engine, session := makeLingerPipes(1)
	defer closeAll(engine...)
	defer closeAll(session...)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := DrainOutbound(ctx, engine, -1)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded from ctx", err)
	}
	if elapsed < 40*time.Millisecond {
		t.Errorf("returned after %v; expected ≥40ms", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("returned after %v; expected <500ms", elapsed)
	}
}

// TestDrainOutbound_NegativeLingerCompletesWhenPipeCloses: the
// "wait-forever" branch still honors a pipe that drains.
func TestDrainOutbound_NegativeLingerCompletesWhenPipeCloses(t *testing.T) {
	engine, session := makeLingerPipes(1)
	defer closeAll(engine...)
	defer closeAll(session...)

	go func() {
		time.Sleep(25 * time.Millisecond)
		_ = engine[0].Close()
	}()

	start := time.Now()
	err := DrainOutbound(context.Background(), engine, -1)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("DrainOutbound = %v; want nil", err)
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("returned after %v; expected <300ms", elapsed)
	}
}

// TestDrainOutbound_EmptyPipes: no pipes means nothing to drain.
func TestDrainOutbound_EmptyPipes(t *testing.T) {
	if err := DrainOutbound(context.Background(), nil, time.Second); err != nil {
		t.Errorf("DrainOutbound(nil) = %v, want nil", err)
	}
	if err := DrainOutbound(context.Background(), []iface.Pipe{}, -1); err != nil {
		t.Errorf("DrainOutbound([]) = %v, want nil", err)
	}
}

// TestDrainOutbound_MultiplePipesAllMustDrain: return-nil requires
// every pipe to close; if one lags past linger, we time out.
func TestDrainOutbound_MultiplePipesAllMustDrain(t *testing.T) {
	engine, session := makeLingerPipes(3)
	defer closeAll(engine...)
	defer closeAll(session...)

	// Close two of the three pipes promptly; the third never closes.
	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = engine[0].Close()
		_ = engine[1].Close()
	}()

	linger := 80 * time.Millisecond
	start := time.Now()
	err := DrainOutbound(context.Background(), engine, linger)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded (one pipe did not drain)", err)
	}
	if elapsed < 60*time.Millisecond {
		t.Errorf("returned after %v; expected ≥%v", elapsed, linger-20*time.Millisecond)
	}
}

// TestDrainOutbound_NilPipeInSliceIsSkipped: robustness against a
// caller that hands us a slice with a nil entry (an engine whose attach
// never landed, say). We should treat nil as "already drained" rather
// than waiting forever on it.
func TestDrainOutbound_NilPipeInSliceIsSkipped(t *testing.T) {
	engine, session := makeLingerPipes(1)
	defer closeAll(engine...)
	defer closeAll(session...)

	pipes := []iface.Pipe{nil, engine[0]}
	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = engine[0].Close()
	}()

	if err := DrainOutbound(context.Background(), pipes, time.Second); err != nil {
		t.Errorf("DrainOutbound with nil entry = %v, want nil", err)
	}
}
