package session

// Reconnector is the client-side dial-and-run loop for a connected Socket.
// It dials via a user-supplied Dial, starts a Runner (a Session) on the
// resulting Conn, waits for it to end (handshake failure, refused attach,
// post-handshake disconnect, or ctx cancellation), and then reconnects
// with exponential backoff + jitter up to MaxBackoff.
//
// The Reconnector is intentionally decoupled from the concrete
// session/transport/options types: it takes factory functions so the
// Socket.Connect path (and tests) can plug in different dial/run
// implementations. Attach, detach and monitor events belong to the
// Session's callbacks; the loop only needs to know when a run ended and
// whether it attached.

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/exergy-dev/gozmq/transport"
)

// Runner is the Reconnector's view of a started connection; *Session
// implements it.
type Runner interface {
	// Done closes when the connection has fully ended.
	Done() <-chan struct{}
	// Stop tears the connection down and waits for Done.
	Stop() error
	// Attached reports whether the connection reached the engine, which
	// resets the backoff schedule.
	Attached() bool
}

// ReconnectConfig bundles the inputs to StartReconnector. Dial and
// Start are required; zero durations are interpreted as documented.
type ReconnectConfig struct {
	// Dial opens a new Conn to the remote endpoint. Respects ctx
	// cancellation. Called once per reconnect attempt.
	Dial func(ctx context.Context) (transport.Conn, error)

	// Start runs a connection over conn, taking ownership of it (closing
	// it on error). An error is treated like a dial failure: the loop
	// backs off and retries.
	Start func(conn transport.Conn) (Runner, error)

	// InitialBackoff is the base delay between reconnect attempts. Also
	// the value the schedule resets to after a successful attach.
	InitialBackoff time.Duration

	// MaxBackoff caps the exponential schedule. Zero means no cap —
	// doubling continues unbounded (subject to the context's lifetime).
	MaxBackoff time.Duration

	// JitterFraction is the symmetric randomness applied to each delay,
	// e.g. 0.2 → ±20%. Zero disables jitter.
	JitterFraction float64

	// OnRetry fires once per backoff cycle, immediately before the
	// loop sleeps. attempt counts 1-based consecutive failures since
	// the last successful attach; it resets to 1 after success.
	// backoff is the sleep duration about to be applied (pre-jitter).
	OnRetry func(attempt int, backoff time.Duration)

	// Logger sinks diagnostic events. nil defaults to slog.Default().
	Logger *slog.Logger
}

// Reconnector runs the dial loop in a dedicated goroutine. Create one with
// StartReconnector; tear it down with Stop.
type Reconnector struct {
	cfg ReconnectConfig

	ctx    context.Context
	cancel context.CancelFunc

	// done closes when the loop goroutine exits, so Stop can wait.
	done chan struct{}
	// stopErr is the live Runner's Stop error when the loop was
	// cancelled; written before done closes.
	stopErr error

	// quiesced is set by Quiesce to prevent loop() from dialing a new
	// connection after the current one ends. Used by Socket.Close to
	// honor LINGER: the current session drains, then everything stops.
	quiesced atomic.Bool
}

// normalize applies defaults and clamps ranges on the config.
//
// JitterFraction is clamped to [0.0, 1.0]:
//   - values < 0 clamp to 0 (no jitter)
//   - values > 1 clamp to 1 (jitter can at most halve or double the delay,
//     never produce a negative result downstream in applyJitter)
//
// The upper clamp is what makes the integer-arithmetic path in
// applyJitter safe: base + delta where |delta| ≤ base ⇒ out ≥ 0 without
// relying on float rounding at the boundary.
func (c *ReconnectConfig) normalize() {
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = 100 * time.Millisecond
	}
	c.JitterFraction = min(max(c.JitterFraction, 0), 1)
}

// StartReconnector launches the dial loop in a new goroutine. The loop
// runs until parent is cancelled or Stop is called.
func StartReconnector(parent context.Context, cfg ReconnectConfig) *Reconnector {
	cfg.normalize()
	ctx, cancel := context.WithCancel(parent)
	r := &Reconnector{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go r.loop()
	return r
}

// Quiesce flips a flag so that when the current connection ends, loop()
// does not dial a new one. The current connection keeps running. Idempotent.
func (r *Reconnector) Quiesce() {
	r.quiesced.Store(true)
}

// Stop cancels the loop, which stops any live Runner, and blocks until
// the loop has exited. It returns the live Runner's Stop error, if any.
// Safe to call more than once.
func (r *Reconnector) Stop() error {
	r.cancel()
	<-r.done
	return r.stopErr
}

// retryAfterBackoff advances the attempt counter and backoff tier, fires
// OnRetry, then sleeps for the pre-advance backoff. Returns false, without
// firing OnRetry, if the Reconnector is stopped or quiesced (Socket.Close
// drains the final session without spawning another), or if Stop
// interrupts the sleep; the caller must then return.
func (r *Reconnector) retryAfterBackoff(attempt *int, backoff *time.Duration) bool {
	if r.ctx.Err() != nil || r.quiesced.Load() {
		return false
	}
	*attempt++
	thisBackoff := *backoff
	*backoff = nextBackoff(*backoff, r.cfg.MaxBackoff)
	if r.cfg.OnRetry != nil {
		r.cfg.OnRetry(*attempt, thisBackoff)
	}
	return r.sleep(thisBackoff)
}

// loop is the top-level dial/run/backoff goroutine. It exits when the
// Reconnector's context is cancelled (Stop, or the parent ctx).
func (r *Reconnector) loop() {
	defer close(r.done)

	backoff := r.cfg.InitialBackoff
	attempt := 0 // consecutive failures since last successful attach

	for {
		// Cancellation/quiesce check before each attempt. The quiesce
		// check matters on the dial-failure continue path too: Quiesce
		// during a backoff sleep must not let the next iteration attach
		// a brand-new session to a closing socket.
		if r.ctx.Err() != nil || r.quiesced.Load() {
			return
		}

		if conn, err := r.cfg.Dial(r.ctx); err != nil {
			r.cfg.Logger.Debug("gozmq/reconnect: dial failed", "err", err, "backoff", backoff)
		} else if run, err := r.cfg.Start(conn); err != nil {
			r.cfg.Logger.Debug("gozmq/reconnect: session start failed", "err", err, "backoff", backoff)
		} else {
			// Done covers every way the run ends — peer disconnect,
			// heartbeat timeout, failed handshake, refused attach; ctx
			// covers a local Stop.
			select {
			case <-run.Done():
			case <-r.ctx.Done():
				r.stopErr = run.Stop()
				return
			}
			if run.Attached() {
				// A successful attach resets the schedule; the redial
				// below is a (small, jittered) delay so a peer that
				// immediately EOFs us is not hot-looped.
				backoff = r.cfg.InitialBackoff
				attempt = 0
			}
		}
		if !r.retryAfterBackoff(&attempt, &backoff) {
			return
		}
	}
}

// sleep waits for the given delay with jitter, returning false if the
// Reconnector was stopped during the wait. A zero or negative delay
// returns true immediately (modulo ctx check).
func (r *Reconnector) sleep(base time.Duration) bool {
	if r.ctx.Err() != nil {
		return false
	}
	d := applyJitter(base, r.cfg.JitterFraction)
	if d <= 0 {
		return r.ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return r.ctx.Err() == nil
	case <-r.ctx.Done():
		return false
	}
}

// nextBackoff doubles base, capped at maxCap if maxCap > 0.
func nextBackoff(base, maxCap time.Duration) time.Duration {
	n := base * 2
	if n < base { // overflow guard
		n = base
	}
	if maxCap > 0 && n > maxCap {
		return maxCap
	}
	return n
}

// applyJitter returns base plus a delta uniform in [-base*fraction,
// +base*fraction). With fraction in [0, 1], which normalize guarantees, the
// result is never negative.
func applyJitter(base time.Duration, fraction float64) time.Duration {
	maxDelta := time.Duration(float64(base) * fraction)
	if maxDelta <= 0 {
		return base
	}
	return base + rand.N(2*maxDelta) - maxDelta
}
