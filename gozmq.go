package gozmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/pipe"
	secmech "github.com/rkm/gozmq/internal/security"
	"github.com/rkm/gozmq/internal/session"
	"github.com/rkm/gozmq/internal/socket"
	"github.com/rkm/gozmq/internal/stream"
	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/rkm/gozmq/internal/zmtp"
	"github.com/rkm/gozmq/security"
	"github.com/rkm/gozmq/security/curve"
	"github.com/rkm/gozmq/transport"
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

// String implements fmt.Stringer using the ZMTP "Socket-Type" metadata names.
func (t SocketType) String() string { return socketTypeName(t) }

// Msg is a ZMTP multipart message. Frames delivered by RecvMsg are
// caller-owned and safe to retain, stash, or pass between goroutines.
//
// Some large inbound frames are backed by buffers drawn from an internal
// size-tiered pool; call (*Msg).Release when finished to return those
// buffers to the pool. Release is always safe to call (it is a no-op for
// *Msg values that carry no pooled frames, including any *Msg you
// constructed via NewMsg / NewMsgFromBytes / NewMsgFromFrames), so the
// safest pattern is to call Release on every received *Msg once done.
// A safety-net finalizer reclaims pooled buffers for callers that drop
// a *Msg without calling Release, but it is not the primary path.
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
// It is idempotent; subsequent Close calls return nil. Subsequent
// NewSocket calls return ErrContextClosed.
func (c *Context) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
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

// monitorState bundles the monitor channel and event mask so they can be
// swapped atomically without holding a mutex on the hot emit() path.
type monitorState struct {
	ch   chan<- Event
	mask EventType
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

	mu             sync.Mutex
	lastEndpoint   string
	listeners      []iface.Listener
	sessions       []iface.Session
	reconnectors   []reconnectEntry
	streamAdapters []*stream.Adapter // STREAM-only: one per raw-bytes peer
	directPipes    []iface.Pipe      // inproc direct-pipe bypass: pipes to drain on close
	acceptWG       sync.WaitGroup
	connectWG      sync.WaitGroup
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

	s.mu.Lock()
	s.listeners = append(s.listeners, l)
	resolved := l.Endpoint()
	s.lastEndpoint = resolved
	s.mu.Unlock()

	s.emit(EventListening, resolved, "")

	s.acceptWG.Add(1)
	go s.acceptLoop(l)
	// If the listener supports direct-pipe bypass (inproc), start a
	// second accept loop that handles direct-pipe offers from connecting
	// sockets. This runs in parallel with the regular acceptLoop so
	// that legacy-path Dial (security configured, or other edge cases)
	// still works.
	if acceptor, ok := l.(iface.DirectPipeAcceptor); ok && !s.hasSecurityMech() {
		s.acceptWG.Add(1)
		go s.acceptDirectPipeLoop(acceptor)
	}
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
			conn, err := tr.Dial(ctx, endpoint)
			if err != nil {
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
		return fmt.Errorf("gozmq: endpoint %q not connected: %w", endpoint, ErrEndpointInvalid)
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

func toKey(b []byte) curve.Key {
	var k curve.Key
	copy(k[:], b)
	return k
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
		if m, err := curve.NewCurveServer(toKey(s.cfg.curveSecretKey), meta); err == nil {
			return m
		}
	case len(s.cfg.curveSecretKey) == 32 && !s.cfg.curveServer && len(s.cfg.curveServerKey) == 32 && len(s.cfg.curvePublicKey) == 32:
		if m, err := curve.NewCurveClient(toKey(s.cfg.curveServerKey), toKey(s.cfg.curvePublicKey), toKey(s.cfg.curveSecretKey), meta); err == nil {
			return m
		}
	case s.cfg.plainAuth != nil && role == iface.RoleServer:
		return secmech.NewPlainServer(s.cfg.plainAuth, meta)
	case s.cfg.plainClient && role == iface.RoleClient:
		return secmech.NewPlainClient(s.cfg.plainClientUser, s.cfg.plainClientPass, meta)
	}
	return secmech.NewNull(role, meta)
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
		s.emit(EventAccepted, conn.RemoteEndpoint(), "")
		if s.socketType == Stream {
			s.startStreamAdapter(conn)
			continue
		}
		s.startSession(conn, iface.RoleServer)
	}
}

// removeSession removes sess from the socket's session list so it can be
// garbage collected after its goroutines exit. Without this, long-lived
// bind sockets accumulate sessions for every peer that ever connected.
func (s *Socket) removeSession(sess iface.Session) {
	s.mu.Lock()
	for i, existing := range s.sessions {
		if existing == sess {
			s.sessions = slices.Delete(s.sessions, i, i+1)
			break
		}
	}
	s.mu.Unlock()
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
		defer s.removeSession(sess)
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

	s.watchPipeClose(p, remote, func() { _ = ad.Close() })
}

// connectStream is the STREAM-specific Connect path. It dials once via
// the transport, constructs a stream.Adapter, attaches it, and wires up
// teardown.
func (s *Socket) connectStream(tr iface.Transport, endpoint string) error {
	conn, err := tr.Dial(s.runCtx, endpoint)
	if err != nil {
		s.emit(EventConnectDelayed, endpoint, err.Error())
		return err
	}
	s.emit(EventConnected, endpoint, "")
	s.startStreamAdapter(conn)
	return nil
}

// --------------------------------------------------------------------------
// Direct-pipe (inproc session bypass)
// --------------------------------------------------------------------------

// hasSecurityMech reports whether the socket is configured with PLAIN or
// CURVE security. Direct-pipe bypass is only safe when the transport is
// in-process AND no security handshake is required.
func (s *Socket) hasSecurityMech() bool {
	return s.cfg.plainClient || s.cfg.plainAuth != nil ||
		(len(s.cfg.curveSecretKey) == 32)
}

// connectDirectPipe is the inproc fast path that replaces the full
// Reconnector+Session stack with a single pipe.NewPair wired directly
// into both engines. The dialer creates the pair, attaches one end to
// its own engine, and offers the other end to the bind-side listener via
// the DirectPipeAcceptor interface.
func (s *Socket) connectDirectPipe(acceptor iface.DirectPipeAcceptor, endpoint string) error {
	dialerPipe, binderPipe := pipe.NewPair(
		s.cfg.sendHWM, s.cfg.recvHWM,
		pipe.PeerInfo{Endpoint: endpoint},
		pipe.PeerInfo{Endpoint: endpoint, Identity: s.cfg.identity},
	)
	offer := &iface.DirectPipeOffer{
		BindPipe:         binderPipe,
		DialerSocketType: socketTypeName(s.socketType),
		Endpoint:         endpoint,
	}

	// Push the offer to the bind side. If the listener is closed, clean up.
	if err := acceptor.OfferDirectPipe(offer); err != nil {
		_ = dialerPipe.Close()
		_ = binderPipe.Close()
		return err
	}

	// Track the pipe for linger drain in CloseCtx.
	s.mu.Lock()
	s.directPipes = append(s.directPipes, dialerPipe)
	s.mu.Unlock()

	// Attach our end to our engine.
	s.emit(EventConnected, endpoint, "")
	if err := s.engine.AttachPipe(dialerPipe); err != nil {
		_ = dialerPipe.Close()
		return err
	}

	s.watchPipeClose(dialerPipe, endpoint, func() { _ = dialerPipe.Close() })
	return nil
}

// acceptDirectPipeLoop runs alongside acceptLoop for inproc listeners
// that support direct-pipe bypass. It accepts DirectPipeOffers from
// connecting sockets and wires them directly into the engine without
// going through the session layer.
func (s *Socket) acceptDirectPipeLoop(acceptor iface.DirectPipeAcceptor) {
	defer s.acceptWG.Done()
	localType := socketTypeName(s.socketType)
	for {
		offer, err := acceptor.AcceptDirectPipe(s.runCtx)
		if err != nil {
			return
		}

		// Socket-type compatibility check.
		if !session.SocketTypesCompatible(localType, offer.DialerSocketType) {
			_ = offer.BindPipe.Close()
			continue
		}

		// Track the pipe for linger drain in CloseCtx.
		s.mu.Lock()
		s.directPipes = append(s.directPipes, offer.BindPipe)
		s.mu.Unlock()

		// Attach the bind-side pipe end to our engine.
		if err := s.engine.AttachPipe(offer.BindPipe); err != nil {
			_ = offer.BindPipe.Close()
			continue
		}

		bp := offer.BindPipe
		s.watchPipeClose(bp, offer.Endpoint, func() { _ = bp.Close() })
	}
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
	if s.closed.Load() {
		return ErrSocketClosed
	}
	if maxSize := s.cfg.maxMessageSize; maxSize > 0 && int64(len(b)) > maxSize {
		return fmt.Errorf("gozmq: frame length %d exceeds MaxMessageSize %d: %w",
			len(b), maxSize, ErrMalformedFrame)
	}
	return s.engine.Send(ctx, msg.FromBytes(b))
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
type SocketMetrics = iface.SocketMetrics

// Metrics returns a point-in-time snapshot of this socket's counters and
// queue-depth gauge. Safe to call from any goroutine. Returns the zero
// SocketMetrics if the socket has been closed.
func (s *Socket) Metrics() SocketMetrics {
	if s.closed.Load() || s.engine == nil {
		return SocketMetrics{}
	}
	return s.engine.Metrics()
}

// Monitor registers ch to receive Events matching mask.
func (s *Socket) Monitor(ch chan<- Event, mask EventType) error {
	if s.closed.Load() {
		return ErrSocketClosed
	}
	s.monitorCfg.Store(&monitorState{ch: ch, mask: mask})
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
	reconnectors := append([]reconnectEntry(nil), s.reconnectors...)
	s.reconnectors = nil
	listeners := s.listeners
	s.listeners = nil
	acceptSessions := s.sessions
	s.sessions = nil
	streamAdapters := s.streamAdapters
	s.streamAdapters = nil
	directPipes := s.directPipes
	s.directPipes = nil
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

	// 3. Collect every live session (server-accept + client-reconnect).
	sessions := make([]iface.Session, 0, len(acceptSessions)+len(reconnectors))
	sessions = append(sessions, acceptSessions...)
	for _, entry := range reconnectors {
		if sess := entry.rc.CurrentSession(); sess != nil {
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
		// Direct pipes (inproc bypass): wait for the peer to close its
		// end so buffered messages have a chance to be consumed. This
		// mirrors the session-based linger drain for TCP/IPC sessions.
		if ctxErr == nil && len(directPipes) > 0 {
			ctxErr = waitDirectPipesDrained(ctx, directPipes, s.cfg.linger)
		}
	}

	// 6. Now hard-stop: cancel the reconnector/accept context, then
	// explicitly stop each reconnector and session. conn.Close → readers
	// exit → full cleanup. Teardown continues even if the linger drain
	// was cancelled — callers observe a fully-closed Socket on return.
	s.runCancel()
	for _, entry := range reconnectors {
		addErr(entry.rc.Stop())
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
	// Direct pipes: close our end so the peer's watcher goroutine
	// detects the close and tears down.
	for _, dp := range directPipes {
		_ = dp.Close()
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

func waitWritersFlushed(ctx context.Context, sessions []iface.Session, linger time.Duration) error {
	return waitChannels(ctx, sessions, func(s iface.Session) <-chan struct{} {
		if s == nil {
			return nil
		}
		return s.WriterFlushed()
	}, linger)
}

func waitDirectPipesDrained(ctx context.Context, pipes []iface.Pipe, linger time.Duration) error {
	return waitChannels(ctx, pipes, func(p iface.Pipe) <-chan struct{} {
		if p == nil {
			return nil
		}
		return p.Closed()
	}, linger)
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

// emit publishes a lifecycle Event on the socket's configured monitor
// channel, non-blocking. Delivery is best-effort: if the channel is
// full we drop the event because backpressuring the socket's hot path on
// a slow consumer is never the right call — monitor channels are
// user-controlled and a misconfigured consumer must not stall Send/Recv.
//
// The monitor mask filters by event type bitmask. Events whose Type
// bit is not set in cfg.monitorMask are discarded silently (and not
// counted as drops — they were never intended for delivery).
func (s *Socket) emit(eventType EventType, endpoint, reason string) {
	ms := s.monitorCfg.Load()
	if ms == nil || ms.ch == nil {
		return
	}
	if ms.mask&eventType == 0 {
		return
	}
	ev := Event{
		Type:     eventType,
		Endpoint: endpoint,
		Time:     time.Now(),
		Reason:   reason,
	}
	select {
	case ms.ch <- ev:
	default:
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
