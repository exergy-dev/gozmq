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

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/pipe"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// DefaultInprocQueueWindow is the time a Dial waits for a matching Listen
// to land before returning ErrEndpointInvalid. Per design §7.3 we relax
// libzmq's strict bind-before-connect rule with this small window so that
// race-y init code (Connect goroutine starts before Bind goroutine) does
// not flake.
const DefaultInprocQueueWindow = 500 * time.Millisecond

// InprocTransport implements iface.Transport for the in-process scheme.
//
// Important scope note: the registry of bound names is kept on the
// InprocTransport instance itself. The public Context wires one
// InprocTransport into each registry, so two Contexts have independent
// inproc namespaces.
//
// Concurrency: all exported methods are safe for concurrent use.
type InprocTransport struct {
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

// NewInprocTransport returns an InprocTransport. queueWindow controls how
// long a Dial waits for a matching Listen before erroring; pass 0 to use
// DefaultInprocQueueWindow.
func NewInprocTransport(queueWindow time.Duration) *InprocTransport {
	if queueWindow <= 0 {
		queueWindow = DefaultInprocQueueWindow
	}
	return &InprocTransport{
		queueWindow: queueWindow,
		listeners:   make(map[string]*inprocListener),
		pending:     make(map[string][]*inprocWaiter),
	}
}

// Scheme reports "inproc".
func (it *InprocTransport) Scheme() string { return "inproc" }

// Dial connects to the named endpoint. If no Listen has happened yet, the
// call queues for up to queueWindow (or until ctx cancels) and resolves
// when a matching Listen arrives.
func (it *InprocTransport) Dial(ctx context.Context, endpoint string) (iface.Conn, error) {
	name, err := parseInprocName(endpoint)
	if err != nil {
		return nil, err
	}

	it.mu.Lock()
	if l, ok := it.listeners[name]; ok {
		it.mu.Unlock()
		return l.dialIn(name)
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
		if l == nil {
			// closed without resolution (defensive — should not happen).
			return nil, fmt.Errorf("%w: inproc dial cancelled before Listen", zmqerr.ErrEndpointInvalid)
		}
		return l.dialIn(name)
	case <-ctx.Done():
		if l := it.cancelWaiter(name, w); l != nil {
			// Listen won the CAS concurrently; honor the delivered listener
			// rather than spuriously failing this Dial.
			return l.dialIn(name)
		}
		return nil, ctx.Err()
	case <-timer.C:
		if l := it.cancelWaiter(name, w); l != nil {
			// Listen won the CAS concurrently; honor the delivered listener.
			return l.dialIn(name)
		}
		return nil, fmt.Errorf("%w: inproc endpoint %q not bound within %v", zmqerr.ErrEndpointInvalid, endpoint, it.queueWindow)
	}
}

// Listen binds the named endpoint. Returns ErrEndpointInvalid if the name
// is already bound (libzmq same-Context behavior).
func (it *InprocTransport) Listen(_ context.Context, endpoint string) (iface.Listener, error) {
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
func (it *InprocTransport) LookupDirectAcceptor(endpoint string) iface.DirectPipeAcceptor {
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
func (it *InprocTransport) removeListener(name string, l *inprocListener) {
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
func (it *InprocTransport) cancelWaiter(name string, w *inprocWaiter) *inprocListener {
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
// inprocListener — iface.Listener over an in-memory accept channel.
// --------------------------------------------------------------------------

// inprocListener accepts in-process Dial pairings. Dial pushes the
// server-side end of a fresh pipe pair onto acceptCh; Accept dequeues. We
// keep a small buffer so a few concurrent Dials don't immediately block on
// the Accept goroutine.
type inprocListener struct {
	t        *InprocTransport
	name     string
	endpoint string

	acceptCh     chan *inprocConn
	directPipeCh chan *iface.DirectPipeOffer

	mu        sync.Mutex
	closed    bool
	closeCh   chan struct{}
	closeOnce sync.Once
}

func newInprocListener(t *InprocTransport, name string) *inprocListener {
	return &inprocListener{
		t:        t,
		name:     name,
		endpoint: "inproc://" + name,
		// Buffer of 16 is plausible for short connect bursts; if the bind
		// side is slow to Accept, additional Dials block (correct behavior
		// for backpressure).
		acceptCh:     make(chan *inprocConn, 16),
		directPipeCh: make(chan *iface.DirectPipeOffer, 16),
		closeCh:      make(chan struct{}),
	}
}

func (l *inprocListener) Endpoint() string { return l.endpoint }

func (l *inprocListener) Accept(ctx context.Context) (iface.Conn, error) {
	select {
	case c := <-l.acceptCh:
		if c == nil {
			return nil, listenerClosedErr()
		}
		return c, nil
	case <-l.closeCh:
		return nil, listenerClosedErr()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// AcceptDirectPipe blocks until a direct-pipe offer arrives or ctx cancels.
func (l *inprocListener) AcceptDirectPipe(ctx context.Context) (*iface.DirectPipeOffer, error) {
	select {
	case offer := <-l.directPipeCh:
		if offer == nil {
			return nil, listenerClosedErr()
		}
		return offer, nil
	case <-l.closeCh:
		return nil, listenerClosedErr()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// OfferDirectPipe pushes a direct-pipe offer to the bind side.
func (l *inprocListener) OfferDirectPipe(offer *iface.DirectPipeOffer) error {
	select {
	case l.directPipeCh <- offer:
		return nil
	case <-l.closeCh:
		return listenerClosedErr()
	}
}

// CloseCh returns a channel that closes when the listener is closed.
func (l *inprocListener) CloseCh() <-chan struct{} {
	return l.closeCh
}

func (l *inprocListener) Close() error {
	l.closeOnce.Do(func() {
		l.mu.Lock()
		l.closed = true
		close(l.closeCh)
		l.mu.Unlock()
		l.t.removeListener(l.name, l)
		// Drain & close any pending accepts so their dialing peer fails
		// promptly.
		for {
			select {
			case c := <-l.acceptCh:
				if c != nil {
					_ = c.Close()
				}
			default:
				goto drainDirect
			}
		}
	drainDirect:
		// Drain & close any pending direct-pipe offers.
		for {
			select {
			case offer := <-l.directPipeCh:
				if offer != nil {
					_ = offer.BindPipe.Close()
				}
			default:
				return
			}
		}
	})
	return nil
}

// dialIn is the Dial-side hand-off into an established Listener. We forge
// a fresh pipe pair — one end becomes the dialer's Conn, the other is
// queued onto the listener's acceptCh.
func (l *inprocListener) dialIn(name string) (iface.Conn, error) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, listenerClosedErr()
	}
	l.mu.Unlock()

	endpoint := "inproc://" + name
	// Pipe HWM here only governs the inproc transport-level buffer. Real
	// HWM lives one layer up in the engine pipes; we just need a small
	// buffer that does not deadlock on a single-frame round-trip.
	const transportPipeHWM = 64
	clientPipe, serverPipe := pipe.NewPair(transportPipeHWM, transportPipeHWM,
		pipe.PeerInfo{Endpoint: endpoint}, // client sees the server endpoint
		pipe.PeerInfo{Endpoint: endpoint}, // server sees the client endpoint (no separate value for inproc)
	)

	clientConn := newInprocConn(clientPipe, endpoint, endpoint)
	serverConn := newInprocConn(serverPipe, endpoint, endpoint)

	// Hand the server-side conn to the listener. If the listener is
	// closing we drop it.
	select {
	case l.acceptCh <- serverConn:
		return clientConn, nil
	case <-l.closeCh:
		_ = serverConn.Close()
		_ = clientConn.Close()
		return nil, listenerClosedErr()
	}
}

// --------------------------------------------------------------------------
// inprocConn — iface.Conn over a Pipe pair.
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

// inprocConn satisfies iface.Conn by delegating to a pipe.Pipe. Because
// inproc bypasses ZMTP framing entirely, this adapter exists so the
// iface.Conn contract remains uniform across all transports.
type inprocConn struct {
	p             iface.Pipe
	local, remote string
	mu            sync.Mutex
	leftover      []byte // unread bytes from the last frame
	readDeadline  time.Time
	writeDeadline time.Time
	closed        bool
}

func newInprocConn(p iface.Pipe, local, remote string) *inprocConn {
	return &inprocConn{p: p, local: local, remote: remote}
}

func (c *inprocConn) LocalEndpoint() string  { return c.local }
func (c *inprocConn) RemoteEndpoint() string { return c.remote }

func (c *inprocConn) Read(buf []byte) (int, error) {
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
	c.mu.Unlock()

	ctx := context.Background()
	var cancel context.CancelFunc
	if !deadline.IsZero() {
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	m, err := c.p.Recv(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return 0, &timeoutError{}
		}
		return 0, err
	}
	// Concatenate the frames (typically a single frame; multipart can be
	// represented by callers writing multiple times — but we chose the
	// "1 Write = 1 frame, 1 Read = up to 1 frame" contract above).
	var data []byte
	if m.NumFrames() == 1 {
		data = m.Frames()[0]
	} else {
		data = bytes.Join(m.Frames(), nil)
	}
	n := copy(buf, data)
	if n < len(data) {
		c.mu.Lock()
		c.leftover = append(c.leftover, data[n:]...)
		c.mu.Unlock()
	}
	return n, nil
}

func (c *inprocConn) Write(buf []byte) (int, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	deadline := c.writeDeadline
	c.mu.Unlock()

	ctx := context.Background()
	var cancel context.CancelFunc
	if !deadline.IsZero() {
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	// Copy so the caller may safely reuse buf after Write returns.
	body := bytes.Clone(buf)
	if err := c.p.Send(ctx, msg.FromBytes(body)); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return 0, &timeoutError{}
		}
		return 0, err
	}
	return len(buf), nil
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
	c.mu.Unlock()
	return nil
}

func (c *inprocConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.mu.Unlock()
	return nil
}

func (c *inprocConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.writeDeadline = t
	c.mu.Unlock()
	return nil
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
	_ iface.Transport          = (*InprocTransport)(nil)
	_ iface.DirectPipeDialer   = (*InprocTransport)(nil)
	_ iface.Listener           = (*inprocListener)(nil)
	_ iface.DirectPipeAcceptor = (*inprocListener)(nil)
	_ iface.Conn               = (*inprocConn)(nil)
	_ net.Error                = (*timeoutError)(nil)
)
