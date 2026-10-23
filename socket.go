package gozmq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/session"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
	"github.com/exergy-dev/gozmq/security"
	"github.com/exergy-dev/gozmq/transport"
)

// NewSocket constructs a socket of the given type on this Context.
func (c *Context) NewSocket(t SocketType, opts ...Option) (*Socket, error) {
	if c.closed.Load() {
		return nil, ErrContextClosed
	}
	cfg := newSocketConfig()
	for _, o := range opts {
		if err := o.apply(cfg); err != nil {
			return nil, err
		}
	}
	if err := validateSocketConfig(t, cfg); err != nil {
		return nil, err
	}
	engine, subHandle, err := newEngineFor(t, cfg)
	if err != nil {
		return nil, err
	}
	// Apply any WithSubscribe options that landed before the engine
	// existed — the Sub/XSub engines accept subscriptions via the
	// dedicated handle rather than the config.
	if subHandle != nil {
		for _, prefix := range cfg.subscriptions {
			if err := subHandle.Subscribe(prefix); err != nil {
				return nil, err
			}
		}
	}

	ctx, cancel := context.WithCancel(c.ctx)
	s := &Socket{
		ctx:        c,
		runCtx:     ctx,
		runCancel:  cancel,
		socketType: t,
		cfg:        cfg,
		engine:     engine,
		subHandle:  subHandle,
		links:      make(map[*link]struct{}),
	}
	// Transfer any monitor config set via options to the atomic pointer so
	// emit() can read it lock-free.
	if cfg.monitor != nil {
		s.monitorCfg.Store(&monitorState{ch: cfg.monitor, mask: cfg.monitorMask})
	}
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		cancel()
		_ = engine.Close()
		return nil, ErrContextClosed
	}
	c.sockets = append(c.sockets, s)
	c.mu.Unlock()
	return s, nil
}

// newEngineFor returns the pattern-specific socket.Engine for the
// supported socket types. For SUB/XSUB the returned subHandle is non-nil;
// the public Socket uses it to implement Subscribe/Unsubscribe. ROUTER
// honors the configured WithRouterMandatory flag.
func newEngineFor(t SocketType, cfg *socketConfig) (socket.Engine, *socket.SubEngine, error) {
	switch t {
	case Pair:
		return socket.NewPairEngine(), nil, nil
	case Push:
		return socket.NewPushEngine(), nil, nil
	case Pull:
		return socket.NewPullEngine(), nil, nil
	case Dealer:
		return socket.NewDealerEngine(), nil, nil
	case Router:
		return socket.NewRouterEngine(cfg.routerMandatory), nil, nil
	case Req:
		return socket.NewReqEngine(), nil, nil
	case Rep:
		return socket.NewRepEngine(), nil, nil
	case Pub:
		return socket.NewPubEngine(), nil, nil
	case Sub:
		eng, sub := socket.NewSubEngine()
		return eng, sub, nil
	case XPub:
		return socket.NewXPubEngine(cfg.xpubVerbose), nil, nil
	case XSub:
		eng, sub := socket.NewXSubEngine()
		return eng, sub, nil
	case Stream:
		// STREAM bypasses ZMTP on the wire: its sessions run in raw
		// mode (see sessionConfig).
		return socket.NewStreamEngine(), nil, nil
	}
	return nil, nil, fmt.Errorf("gozmq: unknown socket type %d", t)
}

// Socket is the user-facing message endpoint.
//
// Concurrency: SendMsg and RecvMsg are thread-safe (serialized by the engine).
// The idiomatic pattern is still one goroutine per socket.
//
// The zero value is NOT usable; construct via Context.NewSocket.
type Socket struct {
	ctx *Context

	runCtx    context.Context
	runCancel context.CancelFunc

	socketType SocketType
	cfg        *socketConfig
	engine     socket.Engine
	subHandle  *socket.SubEngine // non-nil for Sub/XSub

	closed atomic.Bool

	monitorCfg atomic.Pointer[monitorState]

	mu           sync.Mutex
	lastEndpoint string
	listeners    []transport.Listener
	reconnectors []reconnectEntry
	links        map[*link]struct{}
	acceptWG     sync.WaitGroup
}

type reconnectEntry struct {
	endpoint string
	rc       *session.Reconnector
}

// Subscribe adds prefix to a SUB or XSUB socket's filter. Returns an error
// wrapping ErrFSMViolation on other socket types.
func (s *Socket) Subscribe(prefix []byte) error {
	if s.closed.Load() {
		return ErrSocketClosed
	}
	if s.subHandle == nil {
		return fmt.Errorf("gozmq: Subscribe only valid on SUB/XSUB: %w", zmqerr.ErrFSMViolation)
	}
	return s.subHandle.Subscribe(prefix)
}

// Unsubscribe decrements the refcount for prefix on a SUB or XSUB socket.
func (s *Socket) Unsubscribe(prefix []byte) error {
	if s.closed.Load() {
		return ErrSocketClosed
	}
	if s.subHandle == nil {
		return fmt.Errorf("gozmq: Unsubscribe only valid on SUB/XSUB: %w", zmqerr.ErrFSMViolation)
	}
	return s.subHandle.Unsubscribe(prefix)
}

// Type reports the socket's SocketType.
func (s *Socket) Type() SocketType { return s.socketType }

// LastEndpoint returns the endpoint of the most recent successful Bind,
// with wildcards resolved. Useful for recovering an ephemeral port.
func (s *Socket) LastEndpoint() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastEndpoint
}

// Bind listens on the given endpoint (e.g. "tcp://*:5555",
// "inproc://bar"). Returns ErrSocketClosed after Close.
func (s *Socket) Bind(endpoint string) error {
	if s.closed.Load() {
		return ErrSocketClosed
	}
	if isInproc(endpoint) {
		return s.bindInproc(endpoint)
	}
	tr, err := s.ctx.lookupTransport(endpoint)
	if err != nil {
		s.emit(EventBindFailed, endpoint, err.Error())
		return err
	}
	l, err := tr.Listen(s.runCtx, endpoint)
	if err != nil {
		s.emit(EventBindFailed, endpoint, err.Error())
		return err
	}

	// Close may have run while Listen was in flight. Re-check under s.mu,
	// which CloseCtx takes to snapshot s.listeners: either Close sees this
	// listener and closes it, or we see closed and close it ourselves. The
	// acceptWG.Add sits in the same critical section so it cannot race
	// CloseCtx's acceptWG.Wait.
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		_ = l.Close()
		return ErrSocketClosed
	}
	s.listeners = append(s.listeners, l)
	resolved := l.Endpoint()
	s.lastEndpoint = resolved
	s.acceptWG.Add(1)
	s.mu.Unlock()

	s.emit(EventListening, resolved, "")
	go s.acceptLoop(l)
	return nil
}

// Connect dials the given endpoint. Returns ErrSocketClosed after Close.
// Connect starts a reconnect loop that survives transient dial failures
// and post-handshake disconnects, with exponential backoff + jitter per
// the configured WithReconnectInterval / WithReconnectIntervalMax.
//
// An inproc Connect joins the bound socket directly. Before the Bind it
// waits, with no time limit, until the Bind, Disconnect or Close; an
// incompatible socket type is then reported as EventConnectFailed rather
// than returned.
//
// Exception: STREAM sockets dial exactly once and do NOT reconnect — a
// dial failure is returned synchronously (with EventConnectFailed), and a
// later peer disconnect is final (surfaced via EventDisconnected).
// Reconnect supervision for raw STREAM peers is the application's
// responsibility.
func (s *Socket) Connect(endpoint string) error {
	if s.closed.Load() {
		return ErrSocketClosed
	}
	if isInproc(endpoint) {
		return s.connectInproc(endpoint)
	}
	tr, err := s.ctx.lookupTransport(endpoint)
	if err != nil {
		s.emit(EventConnectFailed, endpoint, err.Error())
		return err
	}
	if s.socketType == Stream {
		conn, err := tr.Dial(s.runCtx, endpoint)
		if err != nil {
			s.emit(EventConnectFailed, endpoint, err.Error())
			return err
		}
		_, err = s.startSession(conn, security.RoleClient, endpoint)
		return err
	}
	cfg := session.ReconnectConfig{
		Dial: func(ctx context.Context) (transport.Conn, error) {
			return tr.Dial(ctx, endpoint)
		},
		Start: func(conn transport.Conn) (session.Runner, error) {
			sess, err := s.startSession(conn, security.RoleClient, endpoint)
			if err != nil {
				return nil, err
			}
			return sess, nil
		},
		InitialBackoff: s.cfg.reconnectInterval,
		MaxBackoff:     s.cfg.reconnectIntervalMax,
		JitterFraction: 0.2,
		OnRetry: func(attempt int, backoff time.Duration) {
			// Convention: the first backoff after a dial/handshake
			// failure is "delayed"; subsequent attempts are
			// "retried". libzmq's monitor events use the same
			// split to let consumers differentiate "first trouble"
			// from "ongoing trouble".
			if attempt <= 1 {
				s.emit(EventConnectDelayed, endpoint, backoff.String())
			} else {
				s.emit(EventConnectRetried, endpoint, backoff.String())
			}
		},
		Logger: s.cfg.logger,
	}

	// Start under s.mu so CloseCtx either sees the reconnector (and stops
	// it) or we see closed and never start it.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return ErrSocketClosed
	}
	s.reconnectors = append(s.reconnectors, reconnectEntry{
		endpoint: endpoint,
		rc:       session.StartReconnector(s.runCtx, cfg),
	})
	return nil
}

// Unbind stops listening on a previously-bound endpoint, matched against
// the resolved form returned by Bind (the same value LastEndpoint
// reports, with wildcards and ephemeral ports filled in). Established
// peer connections accepted from that listener are NOT torn down — only
// new accepts stop, matching libzmq's zmq_unbind. Returns an error
// wrapping ErrEndpointUnknown when no listener matches.
func (s *Socket) Unbind(endpoint string) error {
	if s.closed.Load() {
		return ErrSocketClosed
	}
	if isInproc(endpoint) {
		if !s.ctx.inproc.unbind(s, endpoint) {
			return fmt.Errorf("gozmq: endpoint %q not bound: %w", endpoint, ErrEndpointUnknown)
		}
		return nil
	}

	s.mu.Lock()
	var matched transport.Listener
	for i, l := range s.listeners {
		if l.Endpoint() == endpoint {
			matched = l
			s.listeners = slices.Delete(s.listeners, i, i+1)
			break
		}
	}
	s.mu.Unlock()

	if matched == nil {
		return fmt.Errorf("gozmq: endpoint %q not bound: %w", endpoint, ErrEndpointUnknown)
	}
	return matched.Close()
}

// Disconnect tears down every connection made by a Connect to exactly
// endpoint and stops reconnecting to it; an inproc Connect still waiting
// for its Bind is cancelled. Peers that connected to this socket are
// unaffected. Returns an error wrapping ErrEndpointUnknown when nothing
// matches.
func (s *Socket) Disconnect(endpoint string) error {
	if s.closed.Load() {
		return ErrSocketClosed
	}
	s.mu.Lock()
	var (
		rcs   []*session.Reconnector
		links []*link
	)
	s.reconnectors = slices.DeleteFunc(s.reconnectors, func(e reconnectEntry) bool {
		if e.endpoint == endpoint {
			rcs = append(rcs, e.rc)
			return true
		}
		return false
	})
	for l := range s.links {
		if l.ep != "" && l.ep == endpoint {
			links = append(links, l)
			delete(s.links, l)
		}
	}
	s.mu.Unlock()
	if len(rcs) == 0 && len(links) == 0 {
		return fmt.Errorf("gozmq: endpoint %q not connected: %w", endpoint, ErrEndpointUnknown)
	}

	// Reconnectors first, so none redials; stopping their sessions again
	// below is a no-op.
	var errs []error
	for _, rc := range rcs {
		errs = append(errs, rc.Stop())
	}
	for _, l := range links {
		errs = append(errs, l.peer.Stop())
	}
	return errors.Join(errs...)
}

func (s *Socket) acceptLoop(l transport.Listener) {
	defer s.acceptWG.Done()
	// Retry backoff after an Accept error: a persistent failure (EMFILE,
	// say) must not spin, but a transient one should recover quickly.
	const (
		minBackoff = 5 * time.Millisecond
		maxBackoff = time.Second
	)
	bound := l.Endpoint()
	var backoff time.Duration
	for {
		conn, err := l.Accept(s.runCtx)
		if err != nil {
			// Only a closed listener (Unbind/Close) or socket shutdown ends
			// the loop. Anything else — EMFILE, a socket-option failure, an
			// out-of-tree transport's per-connection error — concerns one
			// connection attempt while the listener stays open: report it
			// and retry.
			if errors.Is(err, net.ErrClosed) || s.runCtx.Err() != nil {
				return
			}
			s.emit(EventAcceptFailed, bound, err.Error())
			backoff = min(max(2*backoff, minBackoff), maxBackoff)
			t := time.NewTimer(backoff)
			select {
			case <-t.C:
			case <-s.runCtx.Done():
				t.Stop()
				return
			}
			continue
		}
		backoff = 0
		s.emit(EventAccepted, conn.RemoteEndpoint(), "")
		_, _ = s.startSession(conn, security.RoleServer, conn.RemoteEndpoint())
	}
}

// startSession runs a session over conn, for both the accept path and
// the Reconnector, registered as a link so Close drains it. Its callbacks
// attach and detach the pipe, emit the monitor events and drop the link,
// so a session costs no goroutine beyond its own two. On error conn is
// closed.
func (s *Socket) startSession(conn transport.Conn, role security.Role, endpoint string) (*session.Session, error) {
	cfg, err := s.sessionConfig(conn, role)
	if err != nil {
		s.logger().Debug("gozmq: session construction failed", "err", err, "endpoint", endpoint)
		s.emit(EventHandshakeFailedProtocol, endpoint, err.Error())
		_ = conn.Close()
		return nil, err
	}
	var p *pipe.End
	l := &link{}
	if role == security.RoleClient {
		l.ep = endpoint
	}
	raw := cfg.Raw // capturing cfg itself would move it to the heap
	cfg.OnReady = func(ep *pipe.End) error {
		if !raw {
			s.emit(EventHandshakeSucceeded, endpoint, "")
		}
		// A refused pipe (PAIR already has a peer, engine closed) ends
		// the session unattached: a client backs off and redials.
		if err := s.engine.AttachPipe(ep); err != nil {
			return err
		}
		p = ep
		// EventConnected marks the post-handshake attach on the client
		// side (libzmq parity: not at raw TCP-dial success).
		if role == security.RoleClient {
			s.emit(EventConnected, endpoint, "")
		}
		return nil
	}
	cfg.OnClosed = func(err error, attached bool) {
		switch {
		case attached:
			_ = s.engine.DetachPipe(p)
			s.emit(EventDisconnected, endpoint, "")
		case err != nil && s.runCtx.Err() == nil:
			// A handshake cut short by Close is not a failure to report.
			s.logger().Debug("gozmq: handshake failed", "err", err, "role", role,
				"socket-type", socketTypeName(s.socketType))
			s.emit(classifyHandshakeEvent(err), endpoint, err.Error())
		}
		s.removeLink(l)
	}
	sess := session.New(cfg)
	l.peer = sess
	if !s.addLink(l) {
		_ = conn.Close()
		return nil, ErrSocketClosed
	}
	sess.Start()
	return sess, nil
}

// SendMsg enqueues m for transmission per the socket's pattern policy.
//
// m stays the caller's: for a lent message (see [Msg.Lent]) SendMsg takes
// its own reference for the send, released once the message has been
// written (or received and released by an inproc peer), so the caller may
// Release m as soon as SendMsg returns — the usual forwarding idiom is
// RecvMsg, SendMsg, Release. A plain message is sent as is; the caller
// must not modify its frames while it may still be in flight.
//
// If WithMaxMessageSize is set (any positive cap), every frame in m is
// measured against the cap up front and the call returns an error
// wrapping ErrMalformedFrame on overflow — matching the per-frame
// body-length check enforced by the ZMTP codec (see zmtp/framing.go,
// WriteFrame). The synchronous check means oversize sends are visible
// to the caller without requiring a monitor channel and do not tear
// down the session.
func (s *Socket) SendMsg(ctx context.Context, m *Msg) error {
	if s.closed.Load() {
		return ErrSocketClosed
	}
	if m == nil {
		// Reject here rather than in the engines, whose handling of nil
		// differed (REQ/REP dereferenced it after the request was sent).
		return fmt.Errorf("gozmq: SendMsg requires a non-nil message: %w", ErrFSMViolation)
	}
	if maxSize := s.cfg.maxMessageSize; maxSize > 0 {
		var total int64
		for i, f := range m.Frames() {
			total += int64(len(f))
			if total > maxSize {
				return fmt.Errorf("gozmq: message size %d through frame %d exceeds MaxMessageSize %d: %w",
					total, i, maxSize, ErrMalformedFrame)
			}
		}
	}
	if !m.Lent() {
		return s.engine.Send(ctx, m)
	}
	// The engine takes over the derived reference on success; a failed
	// send leaves it with us.
	d := m.Derive(m.Frames()...)
	if err := s.engine.Send(ctx, d); err != nil {
		d.Release()
		return err
	}
	return nil
}

// SendBytes sends a single-frame message containing b. It is a
// convenience shortcut equivalent to SendMsg(ctx, NewMsgFromBytes(b)).
func (s *Socket) SendBytes(ctx context.Context, b []byte) error {
	return s.SendMsg(ctx, msg.FromBytes(b))
}

// RecvMsg blocks until a message arrives or ctx cancels. The returned
// frames are ordinary memory owned by the caller, except on a socket
// created with [WithRecvBufferPool]: there large frames are lent, valid
// until the message's final [Msg.Release] (see that option).
func (s *Socket) RecvMsg(ctx context.Context) (*Msg, error) {
	if s.closed.Load() {
		return nil, ErrSocketClosed
	}
	return s.engine.Recv(ctx)
}

// RecvBytes receives a single-frame message and returns an independent
// copy of its payload (inproc delivery can share frame bytes with the
// sender, e.g. PUB fan-out), releasing the message itself. Multipart
// messages return an error; use RecvMsg for those.
func (s *Socket) RecvBytes(ctx context.Context) ([]byte, error) {
	m, err := s.RecvMsg(ctx)
	if err != nil {
		return nil, err
	}
	defer m.Release()
	f, ok := m.SingleFrame()
	if !ok {
		// ErrFSMViolation: a valid message met the wrong receive call (as
		// with Subscribe on a non-SUB); ErrMalformedFrame would wrongly
		// suggest a wire error and a torn-down session.
		return nil, fmt.Errorf("gozmq: RecvBytes received a %d-frame message; use RecvMsg for multipart: %w",
			m.NumFrames(), zmqerr.ErrFSMViolation)
	}
	return bytes.Clone(f), nil
}

// SocketMetrics is a point-in-time snapshot of per-socket counters and
// gauges returned by (*Socket).Metrics.
//
// Sent, Recv, and Dropped are monotonic uint64 counters read atomically.
// QueueDepth is a gauge sampled at the moment Metrics is called and
// reflects the number of messages queued in the socket's inbound pipes at
// that instant (zero for send-only patterns).
//
// Drop semantics:
//   - PUB increments Dropped once per (message, peer) pair when the
//     peer's pipe is at HWM during fan-out (slow-subscriber drop).
//   - ROUTER (non-mandatory) increments Dropped on a Send whose leading
//     identity matches no attached peer.
//
// Send-side errors returned to the caller do NOT count as drops — the
// caller already observed the failure and can decide what to do.
type SocketMetrics struct {
	// Sent counts messages successfully handed to a peer pipe.
	Sent uint64
	// Recv counts messages successfully delivered to the application.
	Recv uint64
	// Dropped counts messages the engine refused to deliver after the
	// caller's Send already returned successfully (see the drop
	// semantics above).
	Dropped uint64
	// QueueDepth gauges the messages queued in the socket's inbound
	// pipes at the instant Metrics was called.
	QueueDepth uint64
}

// Metrics returns a point-in-time snapshot of this socket's counters and
// queue-depth gauge. Safe to call from any goroutine. Returns the zero
// SocketMetrics if the socket has been closed.
func (s *Socket) Metrics() SocketMetrics {
	if s.closed.Load() {
		return SocketMetrics{}
	}
	return SocketMetrics(s.engine.Metrics())
}

// Monitor registers ch to receive Events matching mask, replacing any
// previously registered monitor channel (a socket has at most one).
// Passing a nil channel deregisters monitoring. ch must be buffered:
// events are delivered non-blocking and dropped when the channel is
// full, so the socket's hot path is never stalled by a slow consumer
// (see [WithMonitor] for sizing guidance). Never close a monitor channel:
// an emit already in flight on a session goroutine may still send to it
// after Monitor(nil) or Close returns. Let it be garbage collected.
func (s *Socket) Monitor(ch chan<- Event, mask EventType) error {
	if s.closed.Load() {
		return ErrSocketClosed
	}
	s.monitorCfg.Store(&monitorState{ch: ch, mask: mask})
	return nil
}
