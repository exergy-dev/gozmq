package gozmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/metrics"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/session"
	"github.com/rkm/gozmq/internal/stream"
	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/rkm/gozmq/security"
	"github.com/rkm/gozmq/security/curve"
	"github.com/rkm/gozmq/socket"
	"github.com/rkm/gozmq/transport"
	"github.com/rkm/gozmq/zmtp"
)

// SocketType enumerates the ZMTP socket patterns.
type SocketType uint8

const (
	Req SocketType = iota + 1
	Rep
	Dealer
	Router
	Pub
	Sub
	XPub
	XSub
	Push
	Pull
	Pair
	Stream
)

// socketTypeName maps a SocketType to its ZMTP "Socket-Type" metadata value.
func socketTypeName(t SocketType) string {
	switch t {
	case Req:
		return "REQ"
	case Rep:
		return "REP"
	case Dealer:
		return "DEALER"
	case Router:
		return "ROUTER"
	case Pub:
		return "PUB"
	case Sub:
		return "SUB"
	case XPub:
		return "XPUB"
	case XSub:
		return "XSUB"
	case Push:
		return "PUSH"
	case Pull:
		return "PULL"
	case Pair:
		return "PAIR"
	case Stream:
		return "STREAM"
	}
	return ""
}

// Msg is a ZMTP multipart message.
type Msg = msg.Msg

// NewMsg returns an empty message.
func NewMsg() *Msg { return msg.New() }

// NewMsgFromBytes wraps b in a single-frame message (no copy).
func NewMsgFromBytes(b []byte) *Msg { return msg.FromBytes(b) }

// NewMsgFromFrames wraps the provided frames in a multipart message.
func NewMsgFromFrames(frames ...[]byte) *Msg { return msg.FromFrames(frames...) }

// Context is the top-level resource owner. A Context holds the transport
// registry (including the inproc name table shared by all inproc sockets
// on this Context) and the defaults sockets inherit.
//
// The zero value is NOT usable; construct via NewContext.
type Context struct {
	closed atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc

	registry iface.Registry
	zapAuth  iface.ZAPAuthorizer

	mu      sync.Mutex
	sockets []*Socket
}

// ContextOption configures a Context at construction.
type ContextOption interface{ applyContext(*Context) }

type contextOptionFunc func(*Context)

func (f contextOptionFunc) applyContext(c *Context) { f(c) }

// WithZapHandler installs a context-level RFC 27 ZAP handler used by
// server-side PLAIN and CURVE handshakes.
func WithZapHandler(handler *security.ZapHandler) ContextOption {
	return contextOptionFunc(func(c *Context) {
		if handler == nil {
			c.zapAuth = nil
			return
		}
		c.zapAuth = func(domain, address, mech string, credentials [][]byte) (string, map[string]string, error) {
			req := &security.ZapRequest{
				Domain:      domain,
				Address:     address,
				Mechanism:   mech,
				Credentials: credentials,
			}
			resp, err := security.DecodeZapResponse(handler.ServeRequest(security.EncodeZapRequest(req)))
			if err != nil {
				return "", nil, fmt.Errorf("gozmq: decode ZAP response: %w", err)
			}
			switch resp.StatusCode {
			case security.ZapStatusOK:
				return resp.UserID, resp.Metadata, nil
			case security.ZapStatusTempError:
				return "", nil, fmt.Errorf("gozmq: ZAP temporary failure: %s: %w", resp.StatusText, security.ErrZapTemporary)
			case security.ZapStatusClientError:
				return "", nil, fmt.Errorf("gozmq: ZAP rejected %s peer: %s: %w", mech, resp.StatusText, zmqerr.ErrAuthFailed)
			case security.ZapStatusServerError:
				return "", nil, fmt.Errorf("gozmq: ZAP server error: %s", resp.StatusText)
			default:
				return "", nil, fmt.Errorf("gozmq: unknown ZAP status code %q", resp.StatusCode)
			}
		}
	})
}

// NewContext creates a new Context. Call Close to release resources.
func NewContext(opts ...ContextOption) *Context {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Context{
		ctx:      ctx,
		cancel:   cancel,
		registry: transport.New(),
	}
	for _, o := range opts {
		o.applyContext(c)
	}
	return c
}

// Ctx returns the Context's cancellation context. Blocking API calls that
// accept a context.Context honor this root cancellation.
func (c *Context) Ctx() context.Context { return c.ctx }

// RegisterTransport installs t in the Context's transport registry. The
// transport's Scheme() determines which endpoint prefix it handles;
// registering a transport for an already-registered scheme overwrites the
// prior entry.
//
// This is the supported extension point for transports whose defaults
// cannot be baked into transport.New() — most notably TLS, which needs a
// caller-supplied *tls.Config. Typical usage:
//
//	ctx := gozmq.NewContext()
//	ctx.RegisterTransport(transport.NewTLSTransport(tlsCfg))
//	sock, _ := ctx.NewSocket(gozmq.Router)
//	_ = sock.Bind("tls+tcp://0.0.0.0:5555")
//
// Safe to call concurrently with NewSocket; existing sockets consult the
// registry at Bind / Connect time, so a socket constructed before
// RegisterTransport sees the updated scheme set as long as its Bind /
// Connect call races after Register.
func (c *Context) RegisterTransport(t iface.Transport) {
	c.registry.Register(t)
}

// Close cancels the Context and tears down every socket created on it.
// Subsequent NewSocket calls return ErrContextClosed.
func (c *Context) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return ErrContextClosed
	}
	c.mu.Lock()
	socks := append([]*Socket(nil), c.sockets...)
	c.sockets = nil
	c.mu.Unlock()
	for _, s := range socks {
		_ = s.Close()
	}
	c.cancel()
	return nil
}

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
		metrics:    metrics.NewSocketMetrics(socketTypeName(t)),
	}
	c.mu.Lock()
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

	// metrics collects per-socket expvar counters (messages, bytes,
	// handshake outcomes, reconnects, HWM hits, monitor drops). Non-
	// nil for every socket constructed via Context.NewSocket.
	metrics *metrics.SocketMetrics

	closed atomic.Bool

	mu             sync.Mutex
	lastEndpoint   string
	listeners      []iface.Listener
	sessions       []iface.Session
	reconnectors   []*session.Reconnector
	streamAdapters []*stream.Adapter // STREAM-only: one per raw-bytes peer
	acceptWG       sync.WaitGroup
	connectWG      sync.WaitGroup
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

	s.mu.Lock()
	s.listeners = append(s.listeners, l)
	resolved := l.Endpoint()
	s.lastEndpoint = resolved
	s.mu.Unlock()

	s.emit(EventListening, resolved, "")

	s.acceptWG.Add(1)
	go s.acceptLoop(l)
	return nil
}

// Connect dials the given endpoint. Returns ErrSocketClosed after Close.
// Connect starts a reconnect loop that survives transient dial failures
// and post-handshake disconnects, with exponential backoff + jitter per
// the configured WithReconnectInterval / WithReconnectIntervalMax.
func (s *Socket) Connect(endpoint string) error {
	if s.closed.Load() {
		return ErrSocketClosed
	}
	tr, err := s.ctx.registry.Lookup(endpoint)
	if err != nil {
		return err
	}
	if s.socketType == Stream {
		// STREAM does not ride on ZMTP / session, so the Reconnector
		// (which builds iface.Sessions) is not reusable here. Dial
		// once; a future track can layer reconnect semantics on top.
		return s.connectStream(tr, endpoint)
	}
	rc := session.NewReconnector(session.ReconnectConfig{
		Dial: func(ctx context.Context) (iface.Conn, error) {
			conn, err := tr.Dial(ctx, endpoint)
			if err != nil {
				return nil, err
			}
			if err := s.applyConnSocketOptions(conn); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return conn, nil
		},
		NewSession: func(conn iface.Conn) iface.Session {
			sess := s.buildSession(conn, iface.RoleClient)
			s.watchClientHandshake(sess, endpoint)
			return sess
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
		},
		OnRetry: func(attempt int, backoff time.Duration) {
			if s.metrics != nil {
				s.metrics.Reconnects.Add(1)
			}
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
	s.reconnectors = append(s.reconnectors, rc)
	s.mu.Unlock()

	rc.Start(s.runCtx)
	return nil
}

// watchClientHandshake spawns a goroutine that waits for the given
// client-side session's handshake to complete and emits the
// appropriate EventHandshake* event, mirroring startSession's
// server-side behavior. It also bumps the handshake success/failure
// metrics.
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
			if s.metrics != nil {
				s.metrics.HandshakesFailed.Add(1)
			}
			s.emit(classifyHandshakeEvent(err), endpoint, err.Error())
			return
		}
		if s.metrics != nil {
			s.metrics.HandshakesSucceeded.Add(1)
		}
		s.emit(EventHandshakeSucceeded, endpoint, "")
	}()
}

// buildSession is the factory the Reconnector (and the acceptLoop) call
// to wrap a Conn in a Session with the mechanism and codec configured for
// this Socket.
func (s *Socket) buildSession(conn iface.Conn, role iface.Role) iface.Session {
	mech := s.newMechanism(role)
	if role == iface.RoleServer {
		if cfg, ok := mech.(iface.ZapConfigurable); ok && s.ctx.zapAuth != nil {
			cfg.ConfigureZAP(s.ctx.zapAuth, s.cfg.zapDomain, conn.RemoteEndpoint())
		}
	}
	codec := zmtp.NewFrameCodec(s.cfg.maxMessageSize)
	return session.New(session.Config{
		Conn:              conn,
		Mech:              mech,
		Codec:             codec,
		Role:              role,
		HeartbeatInterval: s.cfg.heartbeatInterval,
		HeartbeatTimeout:  s.cfg.heartbeatTimeout,
		HandshakeTimeout:  s.cfg.handshakeTimeout,
		SendHWM:           s.cfg.sendHWM,
		RecvHWM:           s.cfg.recvHWM,
		Logger:            s.cfg.logger,
	})
}

// newMechanism picks the security mechanism for a new session based on
// the socket's configured options. Precedence: CURVE > PLAIN > NULL.
func (s *Socket) newMechanism(role iface.Role) iface.SecurityMech {
	meta := map[string]string{
		"Socket-Type": socketTypeName(s.socketType),
	}
	if len(s.cfg.identity) > 0 {
		meta["Identity"] = string(s.cfg.identity)
	}
	switch {
	case len(s.cfg.curveSecretKey) == 32 && s.cfg.curveServer:
		var sk curve.Key
		copy(sk[:], s.cfg.curveSecretKey)
		m, err := curve.NewCurveServer(sk, meta)
		if err != nil {
			return security.NewNull(role, meta)
		}
		return m
	case len(s.cfg.curveSecretKey) == 32 && !s.cfg.curveServer && len(s.cfg.curveServerKey) == 32 && len(s.cfg.curvePublicKey) == 32:
		var sk, pk, spk curve.Key
		copy(sk[:], s.cfg.curveSecretKey)
		copy(pk[:], s.cfg.curvePublicKey)
		copy(spk[:], s.cfg.curveServerKey)
		m, err := curve.NewCurveClient(spk, pk, sk, meta)
		if err != nil {
			return security.NewNull(role, meta)
		}
		return m
	case s.cfg.plainAuth != nil && role == iface.RoleServer:
		return security.NewPlainServer(s.cfg.plainAuth, meta)
	case s.cfg.plainClient && role == iface.RoleClient:
		return security.NewPlainClient(s.cfg.plainClientUser, s.cfg.plainClientPass, meta)
	}
	return security.NewNull(role, meta)
}

func (s *Socket) acceptLoop(l iface.Listener) {
	defer s.acceptWG.Done()
	bound := l.Endpoint()
	for {
		conn, err := l.Accept(s.runCtx)
		if err != nil {
			// Normal teardown on listener Close / ctx cancel:
			// emit EventAcceptFailed so external observers see the
			// terminal transition (with ctx.Err reason on shutdown,
			// or the underlying net error on a real failure).
			if !s.closed.Load() {
				s.emit(EventAcceptFailed, bound, err.Error())
			}
			return
		}
		if err := s.applyConnSocketOptions(conn); err != nil {
			s.emit(EventAcceptFailed, bound, err.Error())
			_ = conn.Close()
			continue
		}
		s.emit(EventAccepted, conn.RemoteEndpoint(), "")
		if s.socketType == Stream {
			s.startStreamAdapter(conn)
			continue
		}
		s.startSession(conn, iface.RoleServer)
	}
}

// startSession is the server-accept path: wraps conn in a session, spawns
// a handshake goroutine, attaches the pipe on handshake success, detaches
// on pipe close. The client-side (Connect) path uses Reconnector, not
// this helper.
func (s *Socket) startSession(conn iface.Conn, role iface.Role) {
	remote := conn.RemoteEndpoint()
	sess := s.buildSession(conn, role)

	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		_ = conn.Close()
		return
	}
	s.sessions = append(s.sessions, sess)
	s.mu.Unlock()

	sess.Start()

	s.connectWG.Add(1)
	go func() {
		defer s.connectWG.Done()
		select {
		case <-sess.Handshaked():
		case <-s.runCtx.Done():
			_ = sess.Stop()
			return
		}
		if err := sess.HandshakeErr(); err != nil {
			s.logger().Debug("gozmq: server handshake failed",
				"err", err, "role", role, "socket-type", socketTypeName(s.socketType))
			if s.metrics != nil {
				s.metrics.HandshakesFailed.Add(1)
			}
			s.emit(classifyHandshakeEvent(err), remote, err.Error())
			_ = sess.Stop()
			return
		}
		if s.metrics != nil {
			s.metrics.HandshakesSucceeded.Add(1)
		}
		s.emit(EventHandshakeSucceeded, remote, "")
		if err := s.engine.AttachPipe(sess.Pipe()); err != nil {
			_ = sess.Stop()
			return
		}
		<-sess.Pipe().Closed()
		_ = s.engine.DetachPipe(sess.Pipe())
		s.emit(EventDisconnected, remote, "")
	}()
}

// startStreamAdapter is the STREAM-specific counterpart to startSession:
// wraps conn in a raw-bytes Adapter, attaches its engine-facing pipe to
// the streamEngine, and spawns a watcher that DetachPipes + Closes the
// adapter when the peer disconnects. No ZMTP handshake, no security
// mechanism — STREAM peers are by definition non-ZMQ.
func (s *Socket) startStreamAdapter(conn iface.Conn) {
	remote := conn.RemoteEndpoint()
	ad := stream.NewAdapter(s.runCtx, conn, stream.Config{
		Endpoint: remote,
		SendHWM:  s.cfg.sendHWM,
		RecvHWM:  s.cfg.recvHWM,
	})

	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		_ = ad.Close()
		return
	}
	s.streamAdapters = append(s.streamAdapters, ad)
	s.mu.Unlock()

	p := ad.EnginePipe()
	if err := s.engine.AttachPipe(p); err != nil {
		_ = ad.Close()
		return
	}

	s.connectWG.Add(1)
	go func() {
		defer s.connectWG.Done()
		select {
		case <-p.Closed():
		case <-s.runCtx.Done():
		}
		_ = s.engine.DetachPipe(p)
		_ = ad.Close()
		s.emit(EventDisconnected, remote, "")
	}()
}

// connectStream is the STREAM-specific Connect path. It dials once via
// the transport, constructs a stream.Adapter, attaches it, and wires up
// teardown. STREAM does not integrate with session.Reconnector — a
// reconnect loop for raw-bytes transports is a future enhancement.
func (s *Socket) connectStream(tr iface.Transport, endpoint string) error {
	conn, err := tr.Dial(s.runCtx, endpoint)
	if err != nil {
		s.emit(EventConnectDelayed, endpoint, err.Error())
		return err
	}
	if err := s.applyConnSocketOptions(conn); err != nil {
		_ = conn.Close()
		return err
	}
	s.emit(EventConnected, endpoint, "")
	s.startStreamAdapter(conn)
	return nil
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

// RecvMsg blocks until a message arrives or ctx cancels.
func (s *Socket) RecvMsg(ctx context.Context) (*Msg, error) {
	if s.closed.Load() {
		return nil, ErrSocketClosed
	}
	return s.engine.Recv(ctx)
}

// SetOption is intentionally strict on live sockets: options that affect
// engine/session/transport construction are rejected after NewSocket so the
// API never silently accepts a no-op mutation. Use dedicated live methods
// such as Monitor, Subscribe, and Unsubscribe instead.
func (s *Socket) SetOption(o Option) error {
	if s.closed.Load() {
		return ErrSocketClosed
	}
	name := "option"
	if meta, ok := o.(socketOptionMeta); ok {
		name = meta.optionName()
	}
	return fmt.Errorf("gozmq: %s is construction-only on live sockets; use dedicated methods such as Monitor, Subscribe, and Unsubscribe instead", name)
}

// Monitor registers ch to receive Events matching mask. Wired fully in
// Wave 3 C1; for now the config fields are stored on the socket.
func (s *Socket) Monitor(ch chan<- Event, mask EventType) error {
	if s.closed.Load() {
		return ErrSocketClosed
	}
	s.cfg.monitor = ch
	s.cfg.monitorMask = mask
	return nil
}

// Close tears the socket down. Returns ErrSocketClosed if already closed.
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
// a second call returns ErrSocketClosed.
func (s *Socket) CloseCtx(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return ErrSocketClosed
	}

	s.mu.Lock()
	reconnectors := s.reconnectors
	s.reconnectors = nil
	listeners := s.listeners
	s.listeners = nil
	acceptSessions := s.sessions
	s.sessions = nil
	streamAdapters := s.streamAdapters
	s.streamAdapters = nil
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
	for _, rc := range reconnectors {
		rc.Quiesce()
	}
	// 2. Close listeners so acceptLoop exits.
	for _, l := range listeners {
		addErr(l.Close())
	}

	// 3. Collect every live session (server-accept + client-reconnect).
	sessions := make([]iface.Session, 0, len(acceptSessions)+len(reconnectors))
	sessions = append(sessions, acceptSessions...)
	for _, rc := range reconnectors {
		if sess := rc.CurrentSession(); sess != nil {
			sessions = append(sessions, sess)
		}
	}

	// 4–5. LINGER drain (design §6.4):
	//
	//   engine.Close()       → no new engine.Sends enqueue messages
	//   CancelWriter()       → writer's blocking Recv wakes, drains remaining
	//                          queued messages to conn, exits (conn stays open)
	//   waitWritersFlushed() → wait up to linger for all writers to exit
	//   runCancel()          → cascades into reconnector ctx → loop exits
	//   sess.Stop()          → conn.Close → reader exits → session Done fires
	//
	// We call engine.Close BEFORE CancelWriter so any messages the engine
	// had queued before Close are fully in the pipe channel — none can
	// arrive after engine.Close returns.
	//
	// CRUCIAL: runCancel MUST NOT fire before the drain finishes. The
	// Reconnector watches s.runCtx, and when it cancels, the loop's
	// `<-r.ctx.Done()` branch calls sess.Stop() — which closes the conn
	// and aborts the writer mid-drain. Holding runCancel until after
	// waitWritersFlushed keeps the reconnector loop parked in its
	// pipeClosed/ctx select until the writer finishes flushing.
	_ = s.engine.Close()

	var ctxErr error
	if s.cfg.linger != 0 {
		for _, sess := range sessions {
			sess.CancelWriter() // signal: drain, then exit (no conn.Close yet)
		}
		ctxErr = waitWritersFlushed(ctx, sessions, s.cfg.linger)
	}

	// 6. Now hard-stop: cancel the reconnector/accept context, then
	// explicitly stop each reconnector and session. conn.Close → readers
	// exit → full cleanup. Teardown continues even if the linger drain
	// was cancelled — callers observe a fully-closed Socket on return.
	s.runCancel()
	for _, rc := range reconnectors {
		addErr(rc.Stop())
	}
	for _, sess := range sessions {
		addErr(sess.Stop())
	}
	// STREAM adapters were created with runCtx as parent — runCancel
	// already signalled their goroutines to exit. Close waits on the
	// reader/writer wg and surfaces any conn.Close error.
	for _, ad := range streamAdapters {
		addErr(ad.Close())
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

// waitWritersFlushed blocks until every session's WriterFlushed() channel
// has fired, bounded by linger and cancellable via ctx. linger < 0 waits
// indefinitely (ctx-cancellable); linger > 0 caps the wait. Returns
// ctx.Err() if ctx fires before the drain completes, nil otherwise.
// Should be called AFTER closing each session's engine-facing pipe (which
// signals the writer to drain).
func waitWritersFlushed(ctx context.Context, sessions []iface.Session, linger time.Duration) error {
	if len(sessions) == 0 || linger == 0 {
		return nil
	}

	waitCtx := ctx
	cancel := func() {}
	if linger > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, linger)
	}
	defer cancel()

	for _, sess := range sessions {
		if sess == nil {
			continue
		}
		select {
		case <-sess.WriterFlushed():
		case <-waitCtx.Done():
			// Positive linger is a bounded "best effort" wait, not an error.
			// Only surface ctx cancellation from the caller.
			if linger > 0 && errors.Is(waitCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
				return nil
			}
			return waitCtx.Err()
		}
	}
	return nil
}

// emit publishes a lifecycle Event on the socket's configured monitor
// channel, non-blocking. Delivery is best-effort: if the channel is
// full we drop the event and increment the MonitorChanDropped metric,
// because backpressuring the socket's hot path on a slow consumer is
// never the right call — monitor channels are user-controlled and a
// misconfigured consumer must not stall Send/Recv.
//
// The monitor mask filters by event type bitmask. Events whose Type
// bit is not set in cfg.monitorMask are discarded silently (and not
// counted as drops — they were never intended for delivery).
func (s *Socket) emit(eventType EventType, endpoint, reason string) {
	ch := s.cfg.monitor
	if ch == nil {
		return
	}
	if s.cfg.monitorMask&eventType == 0 {
		return
	}
	ev := Event{
		Type:     eventType,
		Endpoint: endpoint,
		Time:     time.Now(),
		Reason:   reason,
	}
	select {
	case ch <- ev:
	default:
		if s.metrics != nil {
			s.metrics.MonitorChanDropped.Add(1)
		}
	}
}

// logger returns the socket's configured logger, falling back to
// slog.Default when none was installed via WithLogger. Centralising
// the fallback keeps the call sites terse.
func (s *Socket) logger() *slog.Logger {
	if s.cfg.logger != nil {
		return s.cfg.logger
	}
	return slog.Default()
}

type socketTuner interface {
	SetTCPKeepAlive(enabled bool, period time.Duration) error
	SetTCPNoDelay(enabled bool) error
}

func (s *Socket) applyConnSocketOptions(conn iface.Conn) error {
	tuner, ok := conn.(socketTuner)
	if !ok {
		return nil
	}
	if err := tuner.SetTCPKeepAlive(s.cfg.tcpKeepalive, 30*time.Second); err != nil {
		return err
	}
	if err := tuner.SetTCPNoDelay(s.cfg.tcpNoDelay); err != nil {
		return err
	}
	return nil
}

// classifyHandshakeEvent maps a handshake error to the appropriate
// Event type. Errors wrapping ErrAuthFailed surface as
// EventHandshakeFailedAuth; errors wrapping ErrMalformedFrame or
// ErrProtocolVersion surface as EventHandshakeFailedProtocol. Any
// other error (dial drop mid-handshake, timeout) also falls under
// protocol failure by convention — the user's monitor consumer can
// inspect Reason for finer classification.
func classifyHandshakeEvent(err error) EventType {
	switch {
	case errors.Is(err, zmqerr.ErrAuthFailed):
		return EventHandshakeFailedAuth
	case errors.Is(err, zmqerr.ErrMalformedFrame), errors.Is(err, zmqerr.ErrProtocolVersion):
		return EventHandshakeFailedProtocol
	}
	return EventHandshakeFailedProtocol
}

// Compile-time sanity check: the public error aliases are the underlying
// zmqerr values, so errors.Is works identically.
var _ = zmqerr.ErrSocketClosed
