package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/pipeiface"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// DefaultInprocQueueWindow is the time a Dial waits for a matching Listen
// to land before returning ErrEndpointInvalid. This diverges from libzmq:
// since 4.2, libzmq allows an inproc connect to precede the bind
// indefinitely (the pipe simply stays unattached until the bind happens),
// whereas gozmq fails the Dial after this window. The bounded window is a
// deliberate tightening — it still absorbs race-y init code (Connect
// goroutine starting just before the Bind goroutine) while surfacing a
// never-bound endpoint as a prompt error instead of a silent stall.
const DefaultInprocQueueWindow = 500 * time.Millisecond

// inprocTransport implements Transport for the in-process scheme.
//
// Important scope note: the registry of bound names is kept on the
// inprocTransport instance itself. The public Context wires one
// inprocTransport into each registry, so two Contexts have independent
// inproc namespaces.
//
// Concurrency: all methods are safe for concurrent use.
type inprocTransport struct {
	queueWindow time.Duration

	mu        sync.Mutex
	listeners map[string]*inprocListener
	// pending tracks Dials parked waiting for a Listen on a name that does
	// not yet exist. Each pending entry is a self-contained waiter with a
	// single-fire resultCh and a CAS-guarded `done` flag: whichever side
	// (Listen notifier or Dial timeout/cancel cleanup) wins the CAS takes
	// the action, and the other side is a no-op. This eliminates the race
	// where a waiter could be simultaneously removed by its own cancel and
	// notified by Listen (or, symmetrically, where a remaining waiter could
	// miss a Listen wake because the notifier already dropped the slice).
	pending map[string][]*inprocWaiter
}

// inprocWaiter is a parked Dial awaiting a Listen. The `done` flag is
// CAS-checked by both the Listen notifier and the Dial cancel/timeout
// cleanup path; the first writer wins and performs its action, the loser
// is a no-op.
type inprocWaiter struct {
	resultCh chan *inprocListener // buffered cap 1 — single-fire
	done     atomic.Bool          // set exactly once by the winning side
}

func newInprocWaiter() *inprocWaiter {
	return &inprocWaiter{resultCh: make(chan *inprocListener, 1)}
}

// NewInprocTransport returns the inproc Transport. queueWindow controls how
// long a Dial waits for a matching Listen before erroring; pass 0 to use
// DefaultInprocQueueWindow.
//
// The concrete type is unexported: its only method beyond the Transport
// contract is the direct-pipe handoff the socket layer probes for
// internally, which is not part of the public API.
func NewInprocTransport(queueWindow time.Duration) Transport {
	if queueWindow <= 0 {
		queueWindow = DefaultInprocQueueWindow
	}
	return &inprocTransport{
		queueWindow: queueWindow,
		listeners:   make(map[string]*inprocListener),
		pending:     make(map[string][]*inprocWaiter),
	}
}

// Scheme reports "inproc".
func (it *inprocTransport) Scheme() string { return "inproc" }

// Dial connects to the named endpoint. If no Listen has happened yet, the
// call queues for up to queueWindow (or until ctx cancels) and resolves
// when a matching Listen arrives.
func (it *inprocTransport) Dial(ctx context.Context, endpoint string) (Conn, error) {
	name, err := parseInprocName(endpoint)
	if err != nil {
		return nil, err
	}

	it.mu.Lock()
	if l, ok := it.listeners[name]; ok {
		it.mu.Unlock()
		return l.dialIn(ctx, name)
	}
	// No listener yet; park a waiter.
	w := newInprocWaiter()
	it.pending[name] = append(it.pending[name], w)
	it.mu.Unlock()

	// Wait up to queueWindow / ctx for a listener.
	timer := time.NewTimer(it.queueWindow)
	defer timer.Stop()

	select {
	case l := <-w.resultCh:
		return l.dialIn(ctx, name)
	case <-ctx.Done():
		if l := it.cancelWaiter(name, w); l != nil {
			// Listen won the CAS concurrently; honor the delivered listener
			// rather than spuriously failing this Dial. ctx is already done,
			// so detach it or the handoff would fail at random.
			return l.dialIn(context.WithoutCancel(ctx), name)
		}
		return nil, ctx.Err()
	case <-timer.C:
		if l := it.cancelWaiter(name, w); l != nil {
			// Listen won the CAS concurrently; honor the delivered listener.
			return l.dialIn(ctx, name)
		}
		return nil, fmt.Errorf("%w: inproc endpoint %q not bound within %v", zmqerr.ErrEndpointInvalid, endpoint, it.queueWindow)
	}
}

// Listen binds the named endpoint. Returns ErrEndpointInvalid if the name
// is already bound (libzmq same-Context behavior).
func (it *inprocTransport) Listen(_ context.Context, endpoint string) (Listener, error) {
	name, err := parseInprocName(endpoint)
	if err != nil {
		return nil, err
	}

	l := newInprocListener(it, name)

	it.mu.Lock()
	if _, exists := it.listeners[name]; exists {
		it.mu.Unlock()
		return nil, fmt.Errorf("%w: inproc endpoint %q already bound", zmqerr.ErrEndpointInvalid, endpoint)
	}
	it.listeners[name] = l
	// Snapshot any parked Dials and drop the pending slot. We wake them
	// outside the lock because the sends are non-blocking (buffered cap-1
	// resultCh). Each waiter is woken only if Listen wins its `done` CAS —
	// a waiter whose Dial has already timed out (and won the CAS first) is
	// skipped so the registered listener is not wasted on a dead waiter.
	waiters := it.pending[name]
	delete(it.pending, name)
	it.mu.Unlock()

	for _, w := range waiters {
		if w.done.CompareAndSwap(false, true) {
			// Non-blocking send: resultCh has cap 1 and is single-fire.
			w.resultCh <- l
		}
	}
	return l, nil
}

// LookupDirectAcceptor returns the DirectPipeAcceptor for a bound inproc
// endpoint, or nil if no listener exists. Used by Socket.Connect to bypass
// the session layer for inproc connections.
func (it *inprocTransport) LookupDirectAcceptor(endpoint string) pipeiface.DirectPipeAcceptor {
	name, err := parseInprocName(endpoint)
	if err != nil {
		return nil
	}
	it.mu.Lock()
	l, ok := it.listeners[name]
	it.mu.Unlock()
	if !ok {
		return nil
	}
	return l
}

// removeListener is called by inprocListener.Close.
func (it *inprocTransport) removeListener(name string, l *inprocListener) {
	it.mu.Lock()
	if cur, ok := it.listeners[name]; ok && cur == l {
		delete(it.listeners, name)
	}
	it.mu.Unlock()
}

// cancelWaiter is invoked by Dial when ctx cancels or the queue window
// expires. It CAS-sets the waiter's `done` flag:
//
//   - If this side wins the CAS, the waiter is removed from pending and
//     nil is returned: Dial reports the ctx / timeout error.
//   - If Listen raced and won the CAS first, a listener is already sitting
//     on resultCh. We drain it and return it so Dial can complete rather
//     than spuriously failing the user.
//
// This is the crux of the fix: the `done` CAS is the single arbiter — only
// one side can win, so no notification is ever lost and no waiter is ever
// both "cancelled" and "notified" ambiguously.
func (it *inprocTransport) cancelWaiter(name string, w *inprocWaiter) *inprocListener {
	if !w.done.CompareAndSwap(false, true) {
		// Listen won the CAS first: there is a listener in resultCh.
		// Receive it so the caller can complete the Dial.
		return <-w.resultCh
	}
	// We won: remove the waiter from pending so Listen won't try to send.
	it.mu.Lock()
	defer it.mu.Unlock()
	q := it.pending[name]
	if i := slices.Index(q, w); i >= 0 {
		it.pending[name] = slices.Delete(q, i, i+1)
	}
	if len(it.pending[name]) == 0 {
		delete(it.pending, name)
	}
	return nil
}

// parseInprocName extracts the name from "inproc://name" and validates it
// (non-empty).
func parseInprocName(endpoint string) (string, error) {
	scheme, addr, err := splitEndpoint(endpoint)
	if err != nil {
		return "", err
	}
	if scheme != "inproc" {
		return "", fmt.Errorf("%w: scheme %q passed to inproc transport", zmqerr.ErrEndpointInvalid, scheme)
	}
	if addr == "" {
		return "", fmt.Errorf("%w: inproc endpoint requires a non-empty name", zmqerr.ErrEndpointInvalid)
	}
	return addr, nil
}

// --------------------------------------------------------------------------
// inprocListener — Listener over an in-memory accept channel.
// --------------------------------------------------------------------------

// inprocListener accepts in-process Dial pairings. Dial pushes the
// server-side end of a fresh pipe pair onto acceptCh; Accept dequeues. We
// keep a small buffer so a few concurrent Dials don't immediately block on
// the Accept goroutine.
type inprocListener struct {
	t        *inprocTransport
	name     string
	endpoint string

	acceptCh     chan *inprocConn
	directPipeCh chan *pipeiface.DirectPipeOffer

	closeCh   chan struct{}
	closeOnce sync.Once
}

func newInprocListener(t *inprocTransport, name string) *inprocListener {
	return &inprocListener{
		t:        t,
		name:     name,
		endpoint: "inproc://" + name,
		// Buffer of 16 is plausible for short connect bursts; if the bind
		// side is slow to Accept, additional Dials block (correct behavior
		// for backpressure).
		acceptCh:     make(chan *inprocConn, 16),
		directPipeCh: make(chan *pipeiface.DirectPipeOffer, 16),
		closeCh:      make(chan struct{}),
	}
}

func (l *inprocListener) Endpoint() string { return l.endpoint }

func (l *inprocListener) Accept(ctx context.Context) (Conn, error) {
	select {
	case c := <-l.acceptCh:
		return c, nil
	case <-l.closeCh:
		return nil, listenerClosedErr()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// AcceptDirectPipe blocks until a direct-pipe offer arrives or ctx cancels.
func (l *inprocListener) AcceptDirectPipe(ctx context.Context) (*pipeiface.DirectPipeOffer, error) {
	select {
	case offer := <-l.directPipeCh:
		return offer, nil
	case <-l.closeCh:
		return nil, listenerClosedErr()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// OfferDirectPipe pushes a direct-pipe offer to the bind side. It blocks
// while the offer queue is full; the DirectPipeAcceptor interface carries
// no ctx, so only Close interrupts that wait.
func (l *inprocListener) OfferDirectPipe(offer *pipeiface.DirectPipeOffer) error {
	return enqueue(context.Background(), l.directPipeCh, l.closeCh, offer, closeOffer)
}

func (l *inprocListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.closeCh)
		l.t.removeListener(l.name, l)
		// Drain & close any pending accepts and direct-pipe offers so their
		// dialing peers fail promptly. A handoff racing this drain re-drains
		// itself (see enqueue).
		drainChan(l.acceptCh, closeInprocConn)
		drainChan(l.directPipeCh, closeOffer)
	})
	return nil
}

func closeInprocConn(c *inprocConn)           { _ = c.Close() }
func closeOffer(o *pipeiface.DirectPipeOffer) { _ = o.BindPipe.Close() }

// enqueue hands v to a listener queue, honoring ctx and closeCh while the
// queue is full. Once closeCh is closed nothing receives from ch, and select
// picks at random when the send and closeCh are both ready, so v can land
// after Close has drained ch. After a successful send we therefore re-check
// closeCh and, if closed, drain ch ourselves (discarding each item). If v
// was still queued, the handoff failed. If Close's drain got it first, v
// was discarded there and the caller sees its peer close promptly.
func enqueue[T comparable](ctx context.Context, ch chan T, closeCh <-chan struct{}, v T, discard func(T)) error {
	select {
	case <-closeCh:
		return listenerClosedErr()
	default:
	}
	select {
	case ch <- v:
	case <-closeCh:
		return listenerClosedErr()
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-closeCh:
	default:
		return nil
	}
	stranded := false
	drainChan(ch, func(x T) {
		stranded = stranded || x == v
		discard(x)
	})
	if stranded {
		return listenerClosedErr()
	}
	return nil
}

// drainChan non-blockingly receives every immediately-available value from ch,
// applying fn to each, and returns once ch has nothing buffered. Callers hold
// whatever invariant prevents new sends (here: closeCh is closed first).
func drainChan[T any](ch <-chan T, fn func(T)) {
	for {
		select {
		case v := <-ch:
			fn(v)
		default:
			return
		}
	}
}

// dialIn is the Dial-side hand-off into an established Listener. We forge
// a fresh pipe pair — one end becomes the dialer's Conn, the other is
// queued onto the listener's acceptCh.
func (l *inprocListener) dialIn(ctx context.Context, name string) (Conn, error) {
	select {
	case <-l.closeCh:
		return nil, listenerClosedErr()
	default:
	}

	endpoint := "inproc://" + name
	// Pipe HWM here only governs the inproc transport-level buffer. Real
	// HWM lives one layer up in the engine pipes; we just need a small
	// buffer that does not deadlock on a single-frame round-trip.
	const transportPipeHWM = 64
	clientPipe, serverPipe := pipe.NewPair(transportPipeHWM, transportPipeHWM,
		pipe.PeerInfo{},
		pipe.PeerInfo{},
	)

	clientConn := newInprocConn(clientPipe, endpoint, endpoint)
	serverConn := newInprocConn(serverPipe, endpoint, endpoint)

	// Hand the server-side conn to the listener. If the listener is
	// closing (or ctx ends while the queue is full) we drop both ends.
	if err := enqueue(ctx, l.acceptCh, l.closeCh, serverConn, closeInprocConn); err != nil {
		_ = serverConn.Close()
		_ = clientConn.Close()
		return nil, err
	}
	return clientConn, nil
}

// --------------------------------------------------------------------------
// inprocConn — Conn over a Pipe pair.
//
// The Conn contract is io.Reader + io.Writer. Inproc bypasses ZMTP framing
// so there is nothing to "frame" at the transport level — but the iface
// nevertheless promises a byte stream. We choose the following contract:
//
//   - Each Write enqueues exactly one frame onto the pipe.
//   - Each Read pulls one frame from the pipe and returns up to len(buf)
//     bytes from it. If the frame is larger than the buffer, the leftover
//     bytes are stashed and returned by subsequent Reads (i.e. Read will
//     concatenate a single Write across multiple short Reads, but it will
//     NEVER merge two Writes into one Read).
//
// The "Read returns one Write per call (modulo short-buffer re-entry)"
// contract is documented here so tests and higher layers have an
// unambiguous reference.
// --------------------------------------------------------------------------

// inprocConn satisfies Conn by delegating to a pipe.Pipe. Because
// inproc bypasses ZMTP framing entirely, this adapter exists so the
// Conn contract remains uniform across all transports.
//
// Deadline semantics follow net.Conn: SetReadDeadline/SetWriteDeadline
// apply to in-flight calls too, so setting a deadline (including one in
// the past) wakes a currently parked Read/Write. That is implemented with
// per-direction "poke" channels: a blocked call captures the current poke
// channel, and SetXDeadline closes-and-replaces it, cancelling the parked
// call's context so it re-evaluates the new deadline.
type inprocConn struct {
	p             pipeiface.Pipe
	local, remote string
	mu            sync.Mutex
	leftover      []byte // unread bytes from the last frame
	readDeadline  time.Time
	writeDeadline time.Time
	readPoke      chan struct{} // closed+replaced by SetReadDeadline
	writePoke     chan struct{} // closed+replaced by SetWriteDeadline
	closed        bool
}

func newInprocConn(p pipeiface.Pipe, local, remote string) *inprocConn {
	return &inprocConn{
		p:         p,
		local:     local,
		remote:    remote,
		readPoke:  make(chan struct{}),
		writePoke: make(chan struct{}),
	}
}

func (c *inprocConn) LocalEndpoint() string  { return c.local }
func (c *inprocConn) RemoteEndpoint() string { return c.remote }

// errDeadlinePoked is an internal sentinel reported by runWithDeadline when
// a concurrent SetXDeadline replaced the deadline while the call was parked.
// The caller loops and re-evaluates the new deadline.
var errDeadlinePoked = errors.New("gozmq/inproc: deadline updated")

// runWithDeadline executes op under a context derived from deadline,
// additionally cancelling it when poke fires (a concurrent SetXDeadline on
// this direction). Returns &timeoutError{} when the deadline has expired —
// including a deadline already in the past, per the net.Conn contract —
// and errDeadlinePoked when the call must be retried under the new
// deadline.
func runWithDeadline(deadline time.Time, poke <-chan struct{}, op func(context.Context) error) error {
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return &timeoutError{}
	}
	ctx := context.Background()
	var cancel context.CancelFunc
	if deadline.IsZero() {
		ctx, cancel = context.WithCancel(ctx)
	} else {
		ctx, cancel = context.WithDeadline(ctx, deadline)
	}
	defer cancel()

	// Watcher: translate a deadline poke into ctx cancellation; exits when
	// op finishes (stop closed) so no goroutine outlives the call.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-poke:
			cancel()
		case <-stop:
		}
	}()

	err := op(ctx)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.DeadlineExceeded):
		return &timeoutError{}
	case errors.Is(err, context.Canceled):
		// The only canceller is the poke watcher above.
		return errDeadlinePoked
	default:
		return err
	}
}

func (c *inprocConn) Read(buf []byte) (int, error) {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return 0, io.ErrClosedPipe
		}
		if len(c.leftover) > 0 {
			n := copy(buf, c.leftover)
			c.leftover = c.leftover[n:]
			c.mu.Unlock()
			return n, nil
		}
		deadline := c.readDeadline
		poke := c.readPoke
		c.mu.Unlock()

		var data []byte
		err := runWithDeadline(deadline, poke, func(ctx context.Context) error {
			m, err := c.p.Recv(ctx)
			if err != nil {
				return err
			}
			// Concatenate the frames (typically a single frame; multipart
			// can be represented by callers writing multiple times — but we
			// chose the "1 Write = 1 frame, 1 Read = up to 1 frame"
			// contract above).
			if m.NumFrames() == 1 {
				data = m.Frames()[0]
			} else {
				data = bytes.Join(m.Frames(), nil)
			}
			return nil
		})
		if errors.Is(err, errDeadlinePoked) {
			continue // deadline replaced mid-call; re-evaluate it
		}
		if err != nil {
			return 0, err
		}
		n := copy(buf, data)
		if n < len(data) {
			c.mu.Lock()
			c.leftover = append(c.leftover, data[n:]...)
			c.mu.Unlock()
		}
		return n, nil
	}
}

func (c *inprocConn) Write(buf []byte) (int, error) {
	// Copy once so the caller may safely reuse buf after Write returns,
	// including across poke-induced retries.
	body := bytes.Clone(buf)
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return 0, io.ErrClosedPipe
		}
		deadline := c.writeDeadline
		poke := c.writePoke
		c.mu.Unlock()

		err := runWithDeadline(deadline, poke, func(ctx context.Context) error {
			return c.p.Send(ctx, msg.FromBytes(body))
		})
		if errors.Is(err, errDeadlinePoked) {
			continue // deadline replaced mid-call; re-evaluate it
		}
		if err != nil {
			return 0, err
		}
		return len(buf), nil
	}
}

func (c *inprocConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	return c.p.Close()
}

func (c *inprocConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.writeDeadline = t
	c.pokeReadLocked()
	c.pokeWriteLocked()
	c.mu.Unlock()
	return nil
}

func (c *inprocConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.pokeReadLocked()
	c.mu.Unlock()
	return nil
}

func (c *inprocConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.writeDeadline = t
	c.pokeWriteLocked()
	c.mu.Unlock()
	return nil
}

// pokeReadLocked / pokeWriteLocked wake any Read/Write parked on the old
// poke channel and install a fresh one for subsequent calls. Caller holds
// c.mu.
func (c *inprocConn) pokeReadLocked() {
	close(c.readPoke)
	c.readPoke = make(chan struct{})
}

func (c *inprocConn) pokeWriteLocked() {
	close(c.writePoke)
	c.writePoke = make(chan struct{})
}

// timeoutError implements net.Error so io callers (and Conn consumers
// expecting net-conn semantics) can detect timeouts via the standard
// Timeout() interface.
type timeoutError struct{}

func (e *timeoutError) Error() string   { return "gozmq/inproc: i/o timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }

// Compile-time interface checks.
var (
	_ Transport                    = (*inprocTransport)(nil)
	_ pipeiface.DirectPipeDialer   = (*inprocTransport)(nil)
	_ Listener                     = (*inprocListener)(nil)
	_ pipeiface.DirectPipeAcceptor = (*inprocListener)(nil)
	_ Conn                         = (*inprocConn)(nil)
	_ net.Error                    = (*timeoutError)(nil)
)
