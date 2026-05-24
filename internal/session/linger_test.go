package session

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, DrainOutbound(context.Background(), engine, 0), "DrainOutbound(linger=0)")
	assert.LessOrEqual(t, time.Since(start), 50*time.Millisecond, "linger=0 expected near-zero")
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
	require.ErrorIs(t, err, context.DeadlineExceeded)
	// We should have waited approximately `linger`. Allow schedule slack
	// on the upper bound but the lower bound is tight — a premature
	// return would be a real bug.
	assert.GreaterOrEqual(t, elapsed, 60*time.Millisecond, "returned too early; expected ≥%v", linger-20*time.Millisecond)
	assert.LessOrEqual(t, elapsed, 400*time.Millisecond, "returned too late; expected ≤400ms")
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
	require.NoError(t, DrainOutbound(context.Background(), engine, linger))
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 20*time.Millisecond, "returned too early; expected ≥20ms (drain delay)")
	assert.LessOrEqual(t, elapsed, 500*time.Millisecond, "returned too late; expected <500ms (drain should have completed)")
}

// TestDrainOutbound_NegativeLingerWaitsForCtx: linger<0 means wait
// forever, bounded only by ctx. The cancellation path is the only exit.
func TestDrainOutbound_NegativeLingerWaitsForCtx(t *testing.T) {
	engine, session := makeLingerPipes(1)
	defer closeAll(engine...)
	defer closeAll(session...)

	ctx := testCtx(t, 60*time.Millisecond)

	start := time.Now()
	err := DrainOutbound(ctx, engine, -1)
	elapsed := time.Since(start)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, elapsed, 40*time.Millisecond, "returned too early; expected ≥40ms")
	assert.LessOrEqual(t, elapsed, 500*time.Millisecond, "returned too late; expected <500ms")
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
	require.NoError(t, err, "DrainOutbound")
	assert.LessOrEqual(t, elapsed, 300*time.Millisecond, "returned too late; expected <300ms")
}

// TestDrainOutbound_EmptyPipes: no pipes means nothing to drain.
func TestDrainOutbound_EmptyPipes(t *testing.T) {
	assert.NoError(t, DrainOutbound(context.Background(), nil, time.Second), "DrainOutbound(nil)")
	assert.NoError(t, DrainOutbound(context.Background(), []iface.Pipe{}, -1), "DrainOutbound([])")
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
	require.ErrorIs(t, err, context.DeadlineExceeded, "one pipe did not drain")
	assert.GreaterOrEqual(t, elapsed, 60*time.Millisecond, "returned too early; expected ≥%v", linger-20*time.Millisecond)
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

	assert.NoError(t, DrainOutbound(context.Background(), pipes, time.Second), "DrainOutbound with nil entry")
}
