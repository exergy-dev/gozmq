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

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/session"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
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

// newEngineFor returns the pattern-specific SocketEngine for the
// supported socket types. For SUB/XSUB the returned subHandle is non-nil;
// the public Socket uses it to implement Subscribe/Unsubscribe. ROUTER
// honors the configured WithRouterMandatory flag.
func newEngineFor(t SocketType, cfg *socketConfig) (iface.SocketEngine, *socket.SubEngine, error) {
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
		return socket.NewXPubEngineV(cfg.xpubVerbose), nil, nil
	case XSub:
		eng, sub := socket.NewXSubEngine()
		return eng, sub, nil
	case Stream:
		// STREAM bypasses ZMTP on the wire: the engine receives raw
		// bytes from the transport via an internal/stream.Adapter
		// rather than internal/session.Session. See Socket.Bind /
		// Socket.Connect for the dispatch.
		return socket.NewStreamEngine(), nil, nil
	}
	return nil, nil, fmt.Errorf("gozmq: unknown socket type %d", t)
}

// Socket is the user-facing message endpoint.
//
// Concurrency: Send and Recv are thread-safe (serialized by the engine).
// The idiomatic pattern is still one goroutine per socket.
//
// The zero value is NOT usable; construct via Context.NewSocket.
type Socket struct {
	ctx *Context

	runCtx    context.Context
	runCancel context.CancelFunc

	socketType SocketType
	cfg        *socketConfig
	engine     iface.SocketEngine
	subHandle  *socket.SubEngine // non-nil for Sub/XSub

	closed atomic.Bool

	monitorCfg atomic.Pointer[monitorState]

	mu           sync.Mutex
	lastEndpoint string
	listeners    []iface.Listener
	reconnectors []reconnectEntry
	// links holds every attached peer (server-accepted sessions, STREAM
	// adapters, and direct inproc pipes) behind one teardown contract. Client
	// sessions are NOT here — they are owned by their reconnector and
	// collected via CurrentSession() at close time.
	links     []peerLink
	acceptWG  sync.WaitGroup
	connectWG sync.WaitGroup
}

type reconnectEntry struct {
	endpoint string
	rc       *session.Reconnector
}

// Subscribe adds prefix to a SUB socket's filter. Returns an error on
// non-Sub sockets.
func (s *Socket) Subscribe(prefix []byte) error {
	if s.closed.Load() {
		return ErrSocketClosed
	}
	if s.subHandle == nil {
		return fmt.Errorf("gozmq: Subscribe only valid on SUB/XSUB: %w", zmqerr.ErrFSMViolation)
	}
	return s.subHandle.Subscribe(prefix)
}

// Unsubscribe decrements the refcount for prefix on a SUB socket.
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
	tr, err := s.ctx.registry.Lookup(endpoint)
	if err != nil {
		s.emit(EventBindFailed, endpoint, err.Error())
		return err
	}
	l, err := tr.Listen(s.runCtx, endpoint)
	if err != nil {
		s.emit(EventBindFailed, endpoint, err.Error())
		return err
	}

	// If the listener supports direct-pipe bypass (inproc), a second
	// accept loop handles direct-pipe offers from connecting sockets. The
	// regular acceptLoop is still required even for inproc — two dial
	// paths verifiably reach it:
	//   - a dialer with PLAIN/CURVE configured skips the direct-pipe
	//     path in Connect and dials through transport.InprocTransport,
	//     whose dialIn hands a conn to Listener.Accept;
	//   - a Connect that races ahead of this Bind finds no
	//     LookupDirectAcceptor yet and falls back to the Reconnector,
	//     whose tr.Dial parks in the transport's pending-waiter queue
	//     and likewise resolves through Listener.Accept.
	// The loop runs even when this socket has security configured: a
	// no-security dialer cannot see our config and will still offer a
	// direct pipe, so the loop must actively REJECT (close) those offers —
	// leaving them undrained would hang the dialer silently.
	acceptor, direct := l.(iface.DirectPipeAcceptor)

	// Close may have run while Listen was in flight. Re-check under s.mu,
	// which CloseCtx takes to snapshot s.listeners: either Close sees this
	// listener and closes it, or we see closed and close it ourselves. The
	// acceptWG.Adds sit in the same critical section so they cannot race
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
	if direct {
		s.acceptWG.Add(1)
	}
	s.mu.Unlock()

	s.emit(EventListening, resolved, "")
	go s.acceptLoop(l)
	if direct {
		go s.acceptDirectPipeLoop(acceptor)
	}
	return nil
}

// Connect dials the given endpoint. Returns ErrSocketClosed after Close.
// Connect starts a reconnect loop that survives transient dial failures
// and post-handshake disconnects, with exponential backoff + jitter per
// the configured WithReconnectInterval / WithReconnectIntervalMax.
//
// Exception: STREAM sockets dial exactly once and do NOT reconnect — a
// dial failure is returned synchronously, and a later peer disconnect is
// final (surfaced via EventDisconnected). Reconnect supervision for raw
// STREAM peers is the application's responsibility.
func (s *Socket) Connect(endpoint string) error {
	if s.closed.Load() {
		return ErrSocketClosed
	}
	tr, err := s.ctx.registry.Lookup(endpoint)
	if err != nil {
		s.emit(EventConnectFailed, endpoint, err.Error())
		return err
	}
	if s.socketType == Stream {
		// STREAM does not ride on ZMTP / session, so the Reconnector
		// (which builds iface.Sessions) is not reusable here. Dial once.
		return s.connectStream(tr, endpoint)
	}
	// Direct-pipe fast path: if the transport supports it (inproc within
	// the same Context) AND no security mechanism is configured (CURVE /
	// PLAIN require the session layer), skip sessions entirely and wire
	// engine pipes directly. This eliminates ZMTP greeting, handshake,
	// codec, and reader/writer goroutines.
	if !s.hasSecurityMech() {
		if dpd, ok := tr.(iface.DirectPipeDialer); ok {
			if acceptor := dpd.LookupDirectAcceptor(endpoint); acceptor != nil {
				return s.connectDirectPipe(acceptor, endpoint)
			}
		}
	}
	rc := session.NewReconnector(session.ReconnectConfig{
		Dial: func(ctx context.Context) (iface.Conn, error) {
			return tr.Dial(ctx, endpoint)
		},
		NewSession: func(conn iface.Conn) (iface.Session, error) {
			sess, err := s.buildSession(conn, iface.RoleClient)
			if err != nil {
				s.emit(EventHandshakeFailedProtocol, endpoint, err.Error())
				return nil, err
			}
			s.watchClientHandshake(sess, endpoint)
			return sess, nil
		},
		InitialBackoff: s.cfg.reconnectInterval,
		MaxBackoff:     s.cfg.reconnectIntervalMax,
		JitterFraction: 0.2,
		OnAttach: func(p iface.Pipe) {
			// EventConnected marks the post-handshake attach on
			// the client side (libzmq parity: Connected fires once
			// the pipe is wired up, not at raw TCP-dial success).
			s.emit(EventConnected, endpoint, "")
			_ = s.engine.AttachPipe(p)
		},
		OnDetach: func(p iface.Pipe) {
			_ = s.engine.DetachPipe(p)
			s.emit(EventDisconnected, endpoint, "")
			s.closeDetachedPipe(p)
		},
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
	})

	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return ErrSocketClosed
	}
	s.reconnectors = append(s.reconnectors, reconnectEntry{
		endpoint: endpoint,
		rc:       rc,
	})
	s.mu.Unlock()

	rc.Start(s.runCtx)
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

	s.mu.Lock()
	var matched iface.Listener
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

// Disconnect tears down every previously-established outbound connection
// whose Connect target exactly matches endpoint and prevents future
// reconnect attempts to that endpoint. It applies only to non-STREAM
// sockets; Bind-created server-side sessions are unaffected.
func (s *Socket) Disconnect(endpoint string) error {
	if s.closed.Load() {
		return ErrSocketClosed
	}
	if s.socketType == Stream {
		return fmt.Errorf("gozmq: Disconnect is not implemented for STREAM sockets: %w", ErrFSMViolation)
	}

	s.mu.Lock()
	var (
		matched []*session.Reconnector
		kept    = s.reconnectors[:0]
	)
	for _, entry := range s.reconnectors {
		if entry.endpoint == endpoint {
			matched = append(matched, entry.rc)
			continue
		}
		kept = append(kept, entry)
	}
	if len(matched) == 0 {
		s.mu.Unlock()
		return fmt.Errorf("gozmq: endpoint %q not connected: %w", endpoint, ErrEndpointUnknown)
	}
	s.reconnectors = kept
	s.mu.Unlock()

	var errs []error
	for _, rc := range matched {
		if err := rc.Stop(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// watchClientHandshake spawns a goroutine that waits for the given
// client-side session's handshake to complete and emits the
// appropriate EventHandshake* event, mirroring startSession's
// server-side behavior.
//
// Safe to spawn freely: each goroutine exits once Handshaked closes
// or the Socket's run context cancels.
func (s *Socket) watchClientHandshake(sess iface.Session, endpoint string) {
	s.connectWG.Add(1)
	go func() {
		defer s.connectWG.Done()
		select {
		case <-sess.Handshaked():
		case <-s.runCtx.Done():
			return
		}
		if err := sess.HandshakeErr(); err != nil {
			s.emit(classifyHandshakeEvent(err), endpoint, err.Error())
			return
		}
		s.emit(EventHandshakeSucceeded, endpoint, "")
	}()
}

func (s *Socket) acceptLoop(l iface.Listener) {
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
		if s.socketType == Stream {
			s.startStreamAdapter(conn)
			continue
		}
		s.startSession(conn, iface.RoleServer)
	}
}

// addLink registers an attached peer for teardown. Returns false if the
// socket is already closing, in which case the caller must tear the link down
// itself.
func (s *Socket) addLink(l peerLink) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return false
	}
	s.links = append(s.links, l)
	return true
}

// removeLink drops a previously-added link. Server sessions self-remove on
// disconnect so long-lived bind sockets don't accumulate them; direct pipes
// and stream adapters stay until Close (matching the original behavior).
func (s *Socket) removeLink(l peerLink) {
	s.mu.Lock()
	if i := slices.Index(s.links, l); i >= 0 {
		s.links = slices.Delete(s.links, i, i+1)
	}
	s.mu.Unlock()
}

// startSession is the server-accept path: wraps conn in a session, spawns
// a handshake goroutine, attaches the pipe on handshake success, detaches
// on pipe close. The client-side (Connect) path uses Reconnector, not
// this helper.
func (s *Socket) startSession(conn iface.Conn, role iface.Role) {
	remote := conn.RemoteEndpoint()
	sess, err := s.buildSession(conn, role)
	if err != nil {
		s.logger().Debug("gozmq: session construction failed", "err", err, "remote", remote)
		s.emit(EventHandshakeFailedProtocol, remote, err.Error())
		_ = conn.Close()
		return
	}
	link := sessionLink{sess}
	if !s.addLink(link) {
		_ = conn.Close()
		return
	}

	sess.Start()

	s.connectWG.Add(1)
	go func() {
		defer s.connectWG.Done()
		defer s.removeLink(link)
		select {
		case <-sess.Handshaked():
		case <-s.runCtx.Done():
			_ = sess.Stop()
			return
		}
		if err := sess.HandshakeErr(); err != nil {
			s.logger().Debug("gozmq: server handshake failed",
				"err", err, "role", role, "socket-type", socketTypeName(s.socketType))
			s.emit(classifyHandshakeEvent(err), remote, err.Error())
			_ = sess.Stop()
			return
		}
		s.emit(EventHandshakeSucceeded, remote, "")
		if err := s.engine.AttachPipe(sess.Pipe()); err != nil {
			_ = sess.Stop()
			return
		}
		<-sess.Pipe().Closed()
		_ = s.engine.DetachPipe(sess.Pipe())
		s.emit(EventDisconnected, remote, "")
		_ = sess.Stop()
		s.closeDetachedPipe(sess.Pipe())
	}()
}

// closeDetachedPipe closes the engine-side end of a session pipe after the
// engine has detached it. Nothing else ever closes that end, and with
// WithRecvHWM(0) the inbound relay goroutine (and its queue) would
// otherwise wait forever for a reader that is gone.
//
// It must not close too early: an engine may still be draining the
// detached pipe to EOF (FairQueue's forwarder does), and closing the
// receiving end of an HWM-0 direction discards the relay's unflushed
// queue. So it closes once the pipe is Drained — every message is then
// in the channel, which Recv still returns before EOF on a closed end,
// and the relay has exited — or once the socket shuts down and no one
// will read it. Channel-backed pipes are always Drained and close at
// once; otherwise a goroutine waits, tracked by connectWG (added under
// s.mu while not closed, so it happens before CloseCtx's Wait).
func (s *Socket) closeDetachedPipe(p iface.Pipe) {
	dp, ok := p.(interface{ Drained() <-chan struct{} })
	if !ok {
		_ = p.Close()
		return
	}
	drained := dp.Drained()
	select {
	case <-drained:
		_ = p.Close()
		return
	default:
	}
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		_ = p.Close()
		return
	}
	s.connectWG.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.connectWG.Done()
		select {
		case <-drained:
		case <-s.runCtx.Done():
		}
		_ = p.Close()
	}()
}

// SendMsg enqueues m for transmission per the socket's pattern policy.
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
	if maxSize := s.cfg.maxMessageSize; maxSize > 0 && m != nil {
		for i, f := range m.Frames() {
			if int64(len(f)) > maxSize {
				return fmt.Errorf("gozmq: frame %d length %d exceeds MaxMessageSize %d: %w",
					i, len(f), maxSize, ErrMalformedFrame)
			}
		}
	}
	return s.engine.Send(ctx, m)
}

// SendBytes sends a single-frame message containing b. It is a
// convenience shortcut equivalent to SendMsg(ctx, NewMsgFromBytes(b)).
func (s *Socket) SendBytes(ctx context.Context, b []byte) error {
	return s.SendMsg(ctx, msg.FromBytes(b))
}

// RecvMsg blocks until a message arrives or ctx cancels. The returned
// *Msg owns its frame bytes. Call (*Msg).Release when done; this is
// always safe and recycles any internal pool buffers backing large
// inbound frames (see the Msg doc).
func (s *Socket) RecvMsg(ctx context.Context) (*Msg, error) {
	if s.closed.Load() {
		return nil, ErrSocketClosed
	}
	return s.engine.Recv(ctx)
}

// RecvBytes receives a single-frame message and returns an independent
// copy of its payload. It releases the message's internal pool buffers
// before returning, so no (*Msg).Release bookkeeping is needed — it is
// the misuse-proof counterpart to SendBytes. Multipart messages return
// an error; use RecvMsg for those.
func (s *Socket) RecvBytes(ctx context.Context) ([]byte, error) {
	m, err := s.RecvMsg(ctx)
	if err != nil {
		return nil, err
	}
	defer m.Release()
	f, ok := m.SingleFrame()
	if !ok {
		return nil, fmt.Errorf("gozmq: RecvBytes received a %d-frame message; use RecvMsg for multipart", m.NumFrames())
	}
	return bytes.Clone(f), nil
}

// SocketMetrics is a point-in-time snapshot of per-socket counters and
// gauges returned by (*Socket).Metrics.
//
// Sent, Recv, and Dropped are monotonic uint64 counters read atomically.
// QueueDepth is a gauge sampled at the moment Metrics is called and
// reflects the number of messages buffered in the engine's inbound
// handoff at that instant (zero for send-only patterns and for engines
// that read directly from a single pipe).
//
// Drop semantics:
//   - PUB increments Dropped once per (message, peer) pair when the
//     peer's pipe is at HWM during fan-out (slow-subscriber drop).
//   - ROUTER (non-mandatory) increments Dropped on a Send whose leading
//     identity matches no attached peer.
//   - Every pattern that uses the shared FairQueue inbound multiplexer
//     also surfaces FairQueue ctx-cancel/close drops here.
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
	// QueueDepth gauges the messages buffered in the engine's inbound
	// handoff at the instant Metrics was called.
	QueueDepth uint64
}

// Metrics returns a point-in-time snapshot of this socket's counters and
// queue-depth gauge. Safe to call from any goroutine. Returns the zero
// SocketMetrics if the socket has been closed.
func (s *Socket) Metrics() SocketMetrics {
	if s.closed.Load() {
		return SocketMetrics{}
	}
	m := s.engine.Metrics()
	return SocketMetrics{
		Sent:       m.Sent,
		Recv:       m.Recv,
		Dropped:    m.Dropped,
		QueueDepth: m.QueueDepth,
	}
}

// Monitor registers ch to receive Events matching mask, replacing any
// previously registered monitor channel (a socket has at most one).
// Passing a nil channel deregisters monitoring. ch must be buffered:
// events are delivered non-blocking and dropped when the channel is
// full, so the socket's hot path is never stalled by a slow consumer
// (see [WithMonitor] for sizing guidance).
func (s *Socket) Monitor(ch chan<- Event, mask EventType) error {
	if s.closed.Load() {
		return ErrSocketClosed
	}
	s.monitorCfg.Store(&monitorState{ch: ch, mask: mask})
	return nil
}
