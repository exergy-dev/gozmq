package gozmq

import (
	"context"
	"errors"
	"time"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
)

// Close tears the socket down. Idempotent: repeated calls return nil,
// matching Context.Close and the io.Closer convention.
//
// Close is equivalent to CloseCtx(context.Background()) and therefore cannot
// interrupt the linger drain. Callers using WithLinger(-1) (infinite) should
// use CloseCtx with a cancellable context instead.
//
// Teardown order (LINGER drain, design §4.7):
//  1. Quiesce reconnectors — stop re-dialing, but KEEP current sessions
//     alive so their outbound queues can drain to the wire.
//  2. Close listeners — no new accepts.
//  3. Close the engine — nothing new is queued.
//  4. Drain every link — session writers flush their queues to the conn
//     and exit, leaving it open — and wait up to cfg.linger for them.
//  5. Cancel the run context, then stop the reconnectors and links.
//  6. Wait for the accept loops.
//
// LINGER semantics:
//   - linger == 0: skip step 4 → queued messages may be lost.
//   - linger  > 0: bounded wait; messages dropped after deadline.
//   - linger  < 0: wait forever for drain (matches libzmq's infinite);
//     cancellable only via CloseCtx.
func (s *Socket) Close() error {
	return s.CloseCtx(context.Background())
}

// CloseCtx releases the socket and its peer connections. It drains
// outbound queues per WithLinger, but honors ctx.Done so callers with
// WithLinger(-1) can cancel an otherwise-infinite wait. Returns the
// first non-context error encountered during teardown; ctx.Err() is
// returned if cancellation interrupts the drain.
//
// Cancellation semantics: if ctx fires while the linger drain is
// pending, the drain is abandoned but the remainder of teardown
// (session Stop, engine Close, runCancel, goroutine joins) still
// runs — callers observe a fully-closed Socket on return. Idempotent:
// a second call returns nil.
func (s *Socket) CloseCtx(ctx context.Context) error {
	if ctx == nil {
		// A nil ctx must not panic half-way through teardown (after closed
		// is set but before the sessions are stopped), which would leak
		// every connection with no way to close the socket again.
		ctx = context.Background()
	}
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	// Take everything: with closed set, nothing new can be added, and the
	// links taken are ours to stop.
	s.mu.Lock()
	reconnectors, listeners, links := s.reconnectors, s.listeners, s.links
	s.reconnectors, s.listeners, s.links = nil, nil, nil
	s.mu.Unlock()

	var errs []error
	addErr := func(err error) {
		if err == nil || zmqerr.IsShutdown(err) {
			return
		}
		errs = append(errs, err)
	}

	for _, entry := range reconnectors {
		entry.rc.Quiesce()
	}
	for _, l := range listeners {
		addErr(l.Close())
	}
	s.ctx.inproc.unbindAll(s)

	// engine.Close comes first so everything it accepted is already in the
	// pipes when the drains start. runCancel must wait for the drain: it
	// stops the reconnectors' sessions, closing their conns mid-flush.
	_ = s.engine.Close()
	var ctxErr error
	if s.cfg.linger != 0 {
		drains := make([]<-chan struct{}, 0, len(links))
		for l := range links {
			drains = append(drains, l.peer.Drain())
		}
		ctxErr = waitDrained(ctx, drains, s.cfg.linger)
	}

	// Teardown goes on even if ctx cut the drain short.
	s.runCancel()
	for _, entry := range reconnectors {
		addErr(entry.rc.Stop())
	}
	for l := range links {
		addErr(l.peer.Stop())
	}
	s.acceptWG.Wait()

	// Drop the Context's reference to this socket. Without this a
	// long-lived Context — the documented one-per-process pattern —
	// accumulates every socket ever created on it, retaining each
	// socket's config, engine, and engine-owned maps and channels for
	// the life of the process.
	s.ctx.removeSocket(s)

	s.emit(EventClosed, s.LastEndpoint(), "")

	if joined := errors.Join(errs...); joined != nil {
		return joined
	}
	return ctxErr
}

// waitDrained blocks until every channel in drains closes, bounded by
// linger and cancellable via ctx. linger < 0 waits indefinitely
// (ctx-cancellable); linger > 0 caps the wait. Returns ctx.Err() if ctx
// fires before the drain completes, nil otherwise. Nil channels are
// skipped.
func waitDrained(ctx context.Context, drains []<-chan struct{}, linger time.Duration) error {
	if len(drains) == 0 || linger == 0 {
		return nil
	}

	waitCtx := ctx
	cancel := func() {}
	if linger > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, linger)
	}
	defer cancel()

	for _, done := range drains {
		if done == nil {
			continue
		}
		select {
		case <-done:
		case <-waitCtx.Done():
			if linger > 0 && errors.Is(waitCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
				return nil
			}
			return waitCtx.Err()
		}
	}
	return nil
}
