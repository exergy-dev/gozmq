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
// implementations. The main agent's Socket.Connect wires this up.
//
// Session end detection uses iface.Session.Done(): that channel closes
// when every session goroutine has exited (reader, writer, optional
// heartbeat, plus the run goroutine that spawns them). Because Done()
// fires on all teardown paths — post-handshake peer disconnect,
// heartbeat timeout, explicit Stop, or pre-pipe handshake failure — it
// is a single, unambiguous signal that also catches sessions that never
// reach the traffic phase. (Earlier revisions polled Pipe().Closed() as
// a workaround when Done() did not exist; that path is gone.)

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/rkm/gozmq/internal/iface"
)

// ReconnectConfig bundles the inputs to NewReconnector. All function
// fields are required; zero durations are interpreted as documented.
type ReconnectConfig struct {
	// Dial opens a new Conn to the remote endpoint. Respects ctx
	// cancellation. Called once per reconnect attempt.
	Dial func(ctx context.Context) (iface.Conn, error)

	// NewSession constructs a session around the given Conn. The
	// Reconnector calls Start on the returned session.
	NewSession func(conn iface.Conn) iface.Session

	// InitialBackoff is the base delay between reconnect attempts. Also
	// the value the schedule resets to after a successful handshake.
	InitialBackoff time.Duration

	// MaxBackoff caps the exponential schedule. Zero means no cap —
	// doubling continues unbounded (subject to the context's lifetime).
	MaxBackoff time.Duration

	// JitterFraction is the symmetric randomness applied to each delay,
	// e.g. 0.2 → ±20%. Zero disables jitter.
	JitterFraction float64

	// OnAttach fires once per successful handshake with the engine-facing
	// pipe end. The Socket engine uses it to attach the pipe.
	OnAttach func(iface.Pipe)

	// OnDetach fires once per session end, with the pipe that was
	// previously attached. nil pipes are passed if the session never
	// reached the attach point (handshake failed); callers should guard.
	OnDetach func(iface.Pipe)

	// OnRetry fires once per backoff cycle, immediately before the
	// loop sleeps. attempt counts 1-based consecutive failures since
	// the last successful handshake; it resets to 1 after success.
	// backoff is the sleep duration about to be applied (pre-jitter).
	//
	// Additive: leaving OnRetry nil is fully supported (it's the
	// pre-C1 behavior). C1 wires this up for EventConnectDelayed /
	// EventConnectRetried emission.
	OnRetry func(attempt int, backoff time.Duration)

	// Logger sinks diagnostic events. nil defaults to slog.Default().
	Logger *slog.Logger
}

// Reconnector runs the dial loop in a dedicated goroutine. Construct via
// NewReconnector; drive with Start and Stop.
type Reconnector struct {
	cfg ReconnectConfig

	ctx    context.Context
	cancel context.CancelFunc

	startOnce sync.Once
	stopOnce  sync.Once

	// stateMu guards the started/stopped transition so Start and Stop
	// cannot race on whether close(done) is owned by loop() or Stop.
	stateMu sync.Mutex
	started bool
	stopped bool
	stopErr error

	// done closes when the Start goroutine exits, so Stop can wait.
	done chan struct{}

	// wake unblocks an in-flight backoff sleep (used by Stop, and by
	// future "reconnect now" hooks).
	wakeMu sync.Mutex
	wake   chan struct{}

	// sessMu guards currentSess so Stop can tear down whichever session
	// is live at the time.
	sessMu      sync.Mutex
	currentSess iface.Session

	// quiesced is set by Quiesce to prevent loop() from dialing a new
	// session after the current one detaches. Used by Socket.Close to
	// honor LINGER: close the engine-facing pipe, let the writer drain,
	// then hard-stop.
	quiesceMu sync.Mutex
	quiesced  bool
}

// normalize applies defaults and clamps ranges on the config. It is
// called from NewReconnector and kept exported-in-package-only so tests
// can exercise the clamping independently of the constructor.
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

// NewReconnector constructs a Reconnector. The returned value is inert
// until Start is called.
func NewReconnector(cfg ReconnectConfig) *Reconnector {
	cfg.normalize()
	ctx, cancel := context.WithCancel(context.Background())
	return &Reconnector{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
		wake:   make(chan struct{}),
	}
}

// Start launches the dial loop in a new goroutine. Subsequent calls are
// no-ops. The supplied ctx, if non-nil, is merged with the Reconnector's
// internal context — cancelling either stops the loop.
func (r *Reconnector) Start(ctx context.Context) {
	r.startOnce.Do(func() {
		r.stateMu.Lock()
		if r.stopped {
			// Already stopped; don't launch the loop. done is already
			// closed by Stop.
			r.stateMu.Unlock()
			return
		}
		r.started = true
		r.stateMu.Unlock()
		if ctx != nil && ctx != context.Background() {
			// Propagate external cancellation into our internal context
			// without leaking a goroutine after Stop: the watcher exits
			// when either ctx fires or r.ctx is cancelled by Stop.
			go func() {
				select {
				case <-ctx.Done():
					r.cancel()
				case <-r.ctx.Done():
				}
			}()
		}
		go r.loop()
	})
}

// CurrentSession returns the session currently owned by the Reconnector,
// or nil if none is live. Used by Socket.Close to drain outbound queues
// before tearing the reconnector down.
func (r *Reconnector) CurrentSession() iface.Session {
	r.sessMu.Lock()
	defer r.sessMu.Unlock()
	return r.currentSess
}

// Quiesce flips a flag so that when the current session ends, loop() does
// not dial a new one. The current session keeps running; callers typically
// then close its pipe and wait for Done() before hard-stopping with Stop.
// Idempotent.
func (r *Reconnector) Quiesce() {
	r.quiesceMu.Lock()
	r.quiesced = true
	r.quiesceMu.Unlock()
}

func (r *Reconnector) isQuiesced() bool {
	r.quiesceMu.Lock()
	defer r.quiesceMu.Unlock()
	return r.quiesced
}

// Stop cancels the loop and tears down any live session. It blocks until
// the loop has exited. Idempotent and safe to call before Start (it
// simply marks the Reconnector as stopped).
func (r *Reconnector) Stop() error {
	r.stopOnce.Do(func() {
		r.cancel()
		// Wake a sleeping backoff.
		r.wakeMu.Lock()
		select {
		case <-r.wake:
			// already closed
		default:
			close(r.wake)
		}
		r.wakeMu.Unlock()

		// Tear down the current session, if any. Do this outside any
		// goroutine the loop might be running in: the loop's Session.Stop
		// call (when it reaches it) is idempotent.
		r.sessMu.Lock()
		sess := r.currentSess
		r.sessMu.Unlock()
		if sess != nil {
			if e := sess.Stop(); e != nil {
				r.stateMu.Lock()
				r.stopErr = e
				r.stateMu.Unlock()
			}
		}

		// If the loop was never started, there's no goroutine to wait
		// for; close done ourselves for symmetry with the started path.
		r.stateMu.Lock()
		needClose := !r.started
		r.stopped = true
		r.stateMu.Unlock()
		if needClose {
			close(r.done)
		}
	})
	// Block until the loop (if any) finishes.
	<-r.done
	r.stateMu.Lock()
	err := r.stopErr
	r.stateMu.Unlock()
	return err
}

// loop is the top-level dial/run/backoff goroutine. It exits when the
// Reconnector's context is cancelled (Stop, or external ctx).
func (r *Reconnector) loop() {
	defer close(r.done)

	backoff := r.cfg.InitialBackoff
	attempt := 0 // consecutive failures since last handshake success

	for {
		// Cancellation check before each attempt.
		if r.ctx.Err() != nil {
			return
		}

		conn, err := r.cfg.Dial(r.ctx)
		if err != nil {
			r.cfg.Logger.Debug("gozmq/reconnect: dial failed", "err", err, "backoff", backoff)
			// Advance state BEFORE invoking user code: if OnRetry panics,
			// the next iteration must still see an incremented attempt
			// counter and the next backoff tier. fireOnRetry recovers.
			attempt++
			thisBackoff := backoff
			backoff = nextBackoff(backoff, r.cfg.MaxBackoff)
			r.fireOnRetry(attempt, thisBackoff)
			if !r.sleep(thisBackoff) {
				return
			}
			continue
		}

		sess := r.cfg.NewSession(conn)
		r.sessMu.Lock()
		if r.ctx.Err() != nil {
			// Stopped while we were dialing. Drop everything and exit.
			r.sessMu.Unlock()
			_ = sess.Stop() // also closes conn
			return
		}
		r.currentSess = sess
		r.sessMu.Unlock()

		sess.Start()

		// Wait for handshake or ctx cancellation.
		select {
		case <-sess.Handshaked():
		case <-r.ctx.Done():
			_ = sess.Stop()
			return
		}

		hsErr := sess.HandshakeErr()
		if hsErr != nil {
			r.cfg.Logger.Debug("gozmq/reconnect: handshake failed", "err", hsErr, "backoff", backoff)
			_ = sess.Stop()
			r.sessMu.Lock()
			r.currentSess = nil
			r.sessMu.Unlock()
			// Same ordering rule as the dial-failure path: advance state
			// first so an OnRetry panic cannot stall the schedule.
			attempt++
			thisBackoff := backoff
			backoff = nextBackoff(backoff, r.cfg.MaxBackoff)
			r.fireOnRetry(attempt, thisBackoff)
			if !r.sleep(thisBackoff) {
				return
			}
			continue
		}

		// Handshake succeeded: reset the backoff schedule, announce the
		// attach, and wait for detach.
		backoff = r.cfg.InitialBackoff
		attempt = 0
		p := sess.Pipe()
		if r.cfg.OnAttach != nil && p != nil {
			r.cfg.OnAttach(p)
		}

		// Detach waiter: observe Session.Done(), which closes when every
		// session goroutine has exited (reader, writer, optional
		// heartbeat). Combined with ctx cancellation, this covers both
		// peer-initiated disconnects and local Stop. No polling; the
		// select wakes immediately on teardown.
		select {
		case <-sess.Done():
		case <-r.ctx.Done():
		}

		_ = sess.Stop()
		if r.cfg.OnDetach != nil {
			r.cfg.OnDetach(p)
		}
		r.sessMu.Lock()
		r.currentSess = nil
		r.sessMu.Unlock()

		// If the reason we exited was Stop/ctx cancel, bail.
		if r.ctx.Err() != nil {
			return
		}
		// Quiesce was set — don't redial; exit cleanly. Socket.Close uses
		// this path to drain the final session without spawning another.
		if r.isQuiesced() {
			return
		}

		// Otherwise reconnect: apply a (small, post-success-reset) delay
		// with jitter. This prevents a hot-reconnect loop against a peer
		// that immediately EOFs us.
		//
		// Same state-before-callback ordering as the other retry paths:
		// a panicking OnRetry must not stall the reconnect schedule.
		attempt++
		thisBackoff := backoff
		backoff = nextBackoff(backoff, r.cfg.MaxBackoff)
		r.fireOnRetry(attempt, thisBackoff)
		if !r.sleep(thisBackoff) {
			return
		}
	}
}

// fireOnRetry invokes the OnRetry callback if one is configured.
// Recovers from panics in user code so a buggy callback cannot take
// down the reconnect loop. A panic in user code is an error condition
// (not verbose noise), so we log at Error level; the reconnect loop
// itself continues unaffected because attempt/backoff state has already
// been advanced by the caller before we're invoked.
func (r *Reconnector) fireOnRetry(attempt int, backoff time.Duration) {
	if r.cfg.OnRetry == nil {
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			r.cfg.Logger.Error("gozmq/reconnect: OnRetry panic",
				"recover", rec, "attempt", attempt, "backoff", backoff)
		}
	}()
	r.cfg.OnRetry(attempt, backoff)
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
	// Snapshot the wake channel under the mutex so Stop's close-then-
	// replace pattern (not used today but room for future "reconnect now")
	// doesn't race with our select.
	r.wakeMu.Lock()
	wake := r.wake
	r.wakeMu.Unlock()
	select {
	case <-t.C:
		return r.ctx.Err() == nil
	case <-wake:
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
// NewReconnector returns, so we do not re-check here.
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
