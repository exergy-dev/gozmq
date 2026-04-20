package session

// DrainOutbound implements the LINGER close semantics (design §6.4): on
// Socket.Close, wait up to `linger` for each supplied pipe to finish
// draining queued outbound messages before tearing down the engine.
//
// Semantics:
//
//   - linger == 0: return immediately; the caller should then close the
//     engine/sessions, discarding whatever was still in the outbound
//     queues.
//   - linger > 0: wait up to that duration for every pipe to drain.
//     Returns nil on successful drain, context.DeadlineExceeded (from the
//     internal timer) on timeout. If ctx is cancelled before the timer
//     fires, its error is returned.
//   - linger < 0: wait forever (until every pipe drains or ctx cancels).
//     This matches libzmq's -1 ("infinite") linger semantic, exposed by
//     WithLinger(d) where d < 0.
//
// Drain is observed externally — the Pipe interface does not publish a
// queue length directly, so we poll Closed(): a pipe is "drained" once
// its peer end has closed (meaning the session has pushed everything it
// had or tore down under us). In practice the Socket.Close path closes
// the sessions' pipes after this helper returns, which is what flips the
// Closed signal; DrainOutbound's job is to give the engine/pipe
// machinery the opportunity to push whatever is queued before that
// happens. The tests assert the observable contract (timing bounds).
//
// DrainOutbound is goroutine-leak-clean: the polling goroutine per pipe
// exits when its pipe closes or the context fires.

import (
	"context"
	"sync"
	"time"

	"github.com/rkm/gozmq/internal/iface"
)

// DrainOutbound waits for each pipe in pipes to drain (its Closed signal
// fires), bounded by `linger` and ctx. See package comment for
// linger == 0, linger > 0, linger < 0 semantics.
func DrainOutbound(ctx context.Context, pipes []iface.Pipe, linger time.Duration) error {
	if len(pipes) == 0 {
		return nil
	}
	if linger == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Build a cancellable child context so the bounded-wait path can
	// stop the polling goroutines regardless of which branch trips the
	// deadline.
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// For linger > 0, install a timer that cancels subCtx when it fires.
	// For linger < 0 we defer to ctx alone.
	var timeoutFired chan struct{}
	if linger > 0 {
		timeoutFired = make(chan struct{})
		t := time.AfterFunc(linger, func() {
			close(timeoutFired)
			cancel()
		})
		defer t.Stop()
	}

	// Wait for every pipe to signal Closed, OR for subCtx to cancel.
	var wg sync.WaitGroup
	wg.Add(len(pipes))
	for _, p := range pipes {
		p := p
		go func() {
			defer wg.Done()
			if p == nil {
				return
			}
			select {
			case <-p.Closed():
			case <-subCtx.Done():
			}
		}()
	}

	// Wait for all observers to exit. They will exit on their own if
	// every pipe closes; otherwise subCtx.Done() (fired by the timer or
	// by ctx cancellation) will wake them.
	doneAll := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneAll)
	}()

	select {
	case <-doneAll:
		// If the timer fired while we were waiting and happened to race
		// with every pipe draining at exactly the wire, prefer the
		// "timed out" answer — the caller asked for strict bounds.
		if timeoutFired != nil {
			select {
			case <-timeoutFired:
				return context.DeadlineExceeded
			default:
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	case <-subCtx.Done():
		// The timer or parent ctx fired. Wait for observers to finish
		// so we don't leave goroutines behind after return.
		<-doneAll
		if timeoutFired != nil {
			select {
			case <-timeoutFired:
				return context.DeadlineExceeded
			default:
			}
		}
		return ctx.Err()
	}
}
