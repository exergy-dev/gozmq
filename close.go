package gozmq

import (
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/rkm/gozmq/internal/iface"
)

// Close tears the socket down. Idempotent: repeated calls return nil,
// matching Context.Close and the io.Closer convention.
//
// Close is equivalent to CloseCtx(context.Background()) and therefore cannot
// interrupt the linger drain. Callers using WithLinger(-1) (infinite) should
// use CloseCtx with a cancellable context instead.
//
// Teardown order (LINGER drain, design §6.4):
//  1. Quiesce reconnectors — stop re-dialing, but KEEP current sessions
//     alive so their outbound queues can drain to the wire.
//  2. Close listeners — no new accepts.
//  3. Collect every live session pipe (server-accept and client-reconnect).
//  4. Close each engine-facing pipe — signals session writer goroutines
//     to drain their channel and flush to conn, then exit.
//  5. Wait up to cfg.linger for every session's Done() to fire — writer
//     has finished flushing to TCP.
//  6. Stop reconnectors fully + stop any still-running sessions +
//     close engine.
//  7. Wait for accept/connect bookkeeping goroutines.
//
// LINGER semantics:
//   - linger == 0: skip step 5 → queued messages may be lost.
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
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	s.mu.Lock()
	reconnectors := slices.Clone(s.reconnectors)
	s.reconnectors = nil
	listeners := s.listeners
	s.listeners = nil
	links := s.links
	s.links = nil
	s.mu.Unlock()

	var errs []error
	addErr := func(err error) {
		if err == nil || isExpectedTeardownErr(err) {
			return
		}
		errs = append(errs, err)
	}

	// 1. Quiesce reconnectors so loop() does not dial a new session after
	// the current one drains.
	for _, entry := range reconnectors {
		entry.rc.Quiesce()
	}
	// 2. Close listeners so acceptLoop exits.
	for _, l := range listeners {
		addErr(l.Close())
	}

	// 3. Add each reconnector's current client session as a link (client
	// sessions are owned by their reconnector, not the links slice).
	for _, entry := range reconnectors {
		if sess := entry.rc.CurrentSession(); sess != nil {
			links = append(links, sessionLink{sess})
		}
	}

	// 4–5. LINGER drain (design §6.4):
	//
	//   engine.Close()  → no new engine.Sends enqueue messages
	//   beginDrain()    → session writers drain queued messages to conn and
	//                     exit (conn stays open); direct/stream: no-op
	//   drained()       → wait up to linger for each link's flush to finish
	//                     (session WriterFlushed; direct pipe peer-close;
	//                     stream: immediate)
	//   runCancel()     → cascades into reconnector ctx → loop exits
	//   stop()          → conn.Close / pipe.Close → readers exit, Done fires
	//
	// We call engine.Close BEFORE beginDrain so any messages the engine had
	// queued before Close are fully in the pipe channel — none can arrive
	// after engine.Close returns.
	//
	// CRUCIAL: runCancel MUST NOT fire before the drain finishes. The
	// Reconnector watches s.runCtx, and when it cancels, the loop's
	// `<-r.ctx.Done()` branch calls sess.Stop() — which closes the conn
	// and aborts the writer mid-drain. Holding runCancel until after the
	// drain keeps the reconnector loop parked until the writer finishes.
	_ = s.engine.Close()

	var ctxErr error
	if s.cfg.linger != 0 {
		for _, l := range links {
			l.beginDrain()
		}
		ctxErr = waitChannels(ctx, links, peerLink.drained, s.cfg.linger)
	}

	// 6. Now hard-stop: cancel the reconnector/accept context, then stop
	// each reconnector and link. Teardown continues even if the linger
	// drain was cancelled — callers observe a fully-closed Socket on return.
	s.runCancel()
	for _, entry := range reconnectors {
		addErr(entry.rc.Stop())
	}
	for _, l := range links {
		addErr(l.stop())
	}

	// 7. Wait on bookkeeping goroutines (accept loops, server-side
	// attach watchers in startSession).
	s.acceptWG.Wait()
	s.connectWG.Wait()
	s.emit(EventClosed, s.LastEndpoint(), "")

	if joined := errors.Join(errs...); joined != nil {
		return joined
	}
	return ctxErr
}

// isExpectedTeardownErr reports whether err is one of the
// "already-shutting-down" errors that routinely fire when Close races
// with the session's own teardown — closing a conn that was closed by
// the reader, closing a pipe that was closed by the peer, etc. These
// are normal and MUST NOT be surfaced from Close as a user-visible
// failure.
func isExpectedTeardownErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	// net.OpError unwraps via Is on its Err; the sentinel above matches
	// "use of closed network connection" from net.Listener.Close and
	// net.Conn.Close in go1.16+. Belt-and-braces: match the message too
	// in case some path wraps it differently.
	if strings.Contains(err.Error(), "use of closed network connection") {
		return true
	}
	return false
}

// waitChannels blocks until ch(item) fires for every item, bounded by
// linger and cancellable via ctx. linger < 0 waits indefinitely
// (ctx-cancellable); linger > 0 caps the wait. Returns ctx.Err() if ctx
// fires before drain completes, nil otherwise. A nil channel from ch
// skips that item.
func waitChannels[T any](ctx context.Context, items []T, ch func(T) <-chan struct{}, linger time.Duration) error {
	if len(items) == 0 || linger == 0 {
		return nil
	}

	waitCtx := ctx
	cancel := func() {}
	if linger > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, linger)
	}
	defer cancel()

	for _, item := range items {
		done := ch(item)
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

// watchPipeClose spawns a goroutine that waits for p to close (or the
// socket's run context to cancel), then detaches p, runs cleanup, and
// emits EventDisconnected. Used by startStreamAdapter, connectDirectPipe,
// and acceptDirectPipeLoop.
func (s *Socket) watchPipeClose(p iface.Pipe, endpoint string, cleanup func()) {
	s.connectWG.Add(1)
	go func() {
		defer s.connectWG.Done()
		select {
		case <-p.Closed():
		case <-s.runCtx.Done():
		}
		_ = s.engine.DetachPipe(p)
		cleanup()
		s.emit(EventDisconnected, endpoint, "")
	}()
}
