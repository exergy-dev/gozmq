package session

// Reconnector is the client-side dial-and-run loop for a connected Socket.
// It dials via a user-supplied Dial, constructs a Session with the
// resulting Conn, runs the session until it ends (handshake failure,
// post-handshake disconnect, or ctx cancellation), and on failure
// reconnects with exponential backoff + jitter up to MaxBackoff.
//
// The Reconnector is intentionally decoupled from the concrete
// session/transport/options types: it takes factory functions so the
// Socket.Connect path (and tests) can plug in different dial/session
// implementations.
//
// Session end detection uses Session.Done(): that channel closes
// when every session goroutine has exited (reader, writer, optional
// heartbeat, plus the run goroutine that spawns them). Because Done()
// fires on all teardown paths — post-handshake peer disconnect,
// heartbeat timeout, explicit Stop, or pre-pipe handshake failure — it
// is a single, unambiguous signal that also catches sessions that never
// reach the traffic phase.

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/transport"
)

// ReconnectConfig bundles the inputs to StartReconnector. Dial and
// NewSession are required; zero durations are interpreted as documented.
type ReconnectConfig struct {
	// Dial opens a new Conn to the remote endpoint. Respects ctx
	// cancellation. Called once per reconnect attempt.
	Dial func(ctx context.Context) (transport.Conn, error)

	// NewSession constructs a session around the given Conn. The
	// Reconnector calls Start on the returned session. An error closes
	// the conn and is treated like a dial failure: the loop backs off
	// and retries.
	NewSession func(conn transport.Conn) (Session, error)

	// InitialBackoff is the base delay between reconnect attempts. Also
	// the value the schedule resets to after a successful attach.
	InitialBackoff time.Duration

	// MaxBackoff caps the exponential schedule. Zero means no cap —
	// doubling continues unbounded (subject to the context's lifetime).
	MaxBackoff time.Duration

	// JitterFraction is the symmetric randomness applied to each delay,
	// e.g. 0.2 → ±20%. Zero disables jitter.
	JitterFraction float64

	// OnAttach fires once per successful handshake with the engine-facing
	// pipe end. The Socket engine uses it to attach the pipe. An error
	// (the engine refused the pipe) is treated like a handshake failure:
	// the session is stopped, the pipe closed, and the loop backs off.
	OnAttach func(*pipe.End) error

	// OnDetach fires once per session end, with the pipe that OnAttach
	// accepted. It is NOT called for sessions that never attached — the
	// loop just backs off and redials.
	OnDetach func(*pipe.End)

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

	// sessMu guards currentSess so Stop can tear down whichever session
	// is live at the time.
	sessMu      sync.Mutex
	currentSess Session

	// quiesced is set by Quiesce to prevent loop() from dialing a new
	// session after the current one detaches. Used by Socket.Close to
	// honor LINGER: close the engine-facing pipe, let the writer drain,
	// then hard-stop.
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
	if c.JitterFraction < 0 {
		c.JitterFraction = 0
	}
	if c.JitterFraction > 1 {
		c.JitterFraction = 1
	}
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

// CurrentSession returns the session currently owned by the Reconnector,
// or nil if none is live. Used by Socket.Close to drain outbound queues
// before tearing the reconnector down.
func (r *Reconnector) CurrentSession() Session {
	r.sessMu.Lock()
	defer r.sessMu.Unlock()
	return r.currentSess
}

// Quiesce flips a flag so that when the current session ends, loop() does
// not dial a new one. The current session keeps running; callers typically
// then close its pipe and wait for Done() before hard-stopping with Stop.
// Idempotent.
func (r *Reconnector) Quiesce() {
	r.quiesced.Store(true)
}

// Stop cancels the loop, tears down any live session, and blocks until
// the loop has exited. It returns the live session's Stop error, if any.
// Safe to call more than once.
func (r *Reconnector) Stop() error {
	// cancel() unblocks any in-flight dial or backoff sleep, and makes
	// setSession refuse new sessions, so the one read below is the last.
	// Session.Stop is idempotent, so racing the loop's own call is fine.
	r.cancel()
	var err error
	if sess := r.CurrentSession(); sess != nil {
		err = sess.Stop()
	}
	<-r.done
	return err
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

// setSession records sess as the live session, or reports false (leaving
// it unrecorded) if the Reconnector was stopped meanwhile.
func (r *Reconnector) setSession(sess Session) bool {
	r.sessMu.Lock()
	defer r.sessMu.Unlock()
	if sess != nil && r.ctx.Err() != nil {
		return false
	}
	r.currentSess = sess
	return true
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

		conn, err := r.cfg.Dial(r.ctx)
		if err != nil {
			r.cfg.Logger.Debug("gozmq/reconnect: dial failed", "err", err, "backoff", backoff)
			if !r.retryAfterBackoff(&attempt, &backoff) {
				return
			}
			continue
		}

		sess, err := r.cfg.NewSession(conn)
		if err != nil {
			_ = conn.Close()
			r.cfg.Logger.Debug("gozmq/reconnect: session construction failed", "err", err, "backoff", backoff)
			if !r.retryAfterBackoff(&attempt, &backoff) {
				return
			}
			continue
		}
		if !r.setSession(sess) {
			// Stopped while we were dialing. Drop everything and exit.
			_ = sess.Stop() // also closes conn
			return
		}

		sess.Start()

		// Wait for handshake or ctx cancellation.
		select {
		case <-sess.Handshaked():
		case <-r.ctx.Done():
			_ = sess.Stop()
			return
		}

		p := sess.Pipe()
		err = sess.HandshakeErr()
		if err == nil && r.cfg.OnAttach != nil && p != nil {
			if err = r.cfg.OnAttach(p); err != nil {
				_ = p.Close() // the engine never saw it
			}
		}
		if err != nil {
			r.cfg.Logger.Debug("gozmq/reconnect: handshake or attach failed", "err", err, "backoff", backoff)
			_ = sess.Stop()
			r.setSession(nil)
			if !r.retryAfterBackoff(&attempt, &backoff) {
				return
			}
			continue
		}

		// Attached: reset the backoff schedule and wait for detach.
		backoff = r.cfg.InitialBackoff
		attempt = 0

		// Session.Done() closes when every session goroutine has exited,
		// covering peer disconnects; ctx covers local Stop.
		select {
		case <-sess.Done():
		case <-r.ctx.Done():
		}

		_ = sess.Stop()
		if r.cfg.OnDetach != nil {
			r.cfg.OnDetach(p)
		}
		r.setSession(nil)

		// Reconnect after a (small, post-success-reset) delay with
		// jitter. This prevents a hot-reconnect loop against a peer that
		// immediately EOFs us.
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

// applyJitter returns base + delta where delta is uniform in
// [-maxDelta, +maxDelta) and maxDelta = base * fraction. fraction is
// assumed to be in [0, 1] — ReconnectConfig.normalize clamps it before
// StartReconnector runs the loop, so we do not re-check here.
//
// This is integer-only arithmetic on time.Duration (which is int64 ns),
// so there is no float rounding at the boundary. With fraction ≤ 1 the
// output is mathematically guaranteed to be ≥ 0: the minimum delta is
// -maxDelta = -base*fraction ≥ -base, so base + delta ≥ 0.
//
// The trailing "if out < 0" guard is kept as defense-in-depth in case
// future callers bypass normalize() and pass an unclamped fraction; with
// a clamped fraction it is unreachable.
func applyJitter(base time.Duration, fraction float64) time.Duration {
	if fraction <= 0 || base <= 0 {
		return base
	}
	// Max jitter magnitude in nanoseconds. Truncation from the float
	// multiply can only reduce magnitude, never flip the sign.
	maxDelta := time.Duration(float64(base) * fraction)
	if maxDelta <= 0 {
		return base
	}
	// Pick delta ∈ [-maxDelta, +maxDelta) from a uniform integer draw.
	// rand.Int64N is v2's replacement for Int63n and returns [0, n).
	delta := time.Duration(rand.Int64N(int64(2*maxDelta))) - maxDelta
	out := base + delta
	if out < 0 {
		// Defense-in-depth: unreachable when fraction ∈ [0, 1] because
		// normalize() clamps, but keep the guard so the function can't
		// ever return a negative time.Duration regardless of input.
		return 0
	}
	return out
}
