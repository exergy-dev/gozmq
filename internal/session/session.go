// Package session implements the ZMTP per-peer state machine. Each Session
// wraps a single iface.Conn, drives greeting + security handshake, then
// runs two traffic-phase goroutines (reader: wire->pipe, writer: pipe->wire)
// plus an optional heartbeat scheduler.
//
// The session layer never imports the concrete framing/security/transport
// packages — it consumes the iface contracts (FrameCodec, SecurityMech,
// Conn) so it can be developed and tested in isolation per the Wave-1
// parallel plan.
package session

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/pipe"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// Config bundles the inputs to New. All fields are required except the
// timeout/heartbeat values, which default to sensible production numbers
// when zero.
type Config struct {
	Conn              iface.Conn
	Mech              iface.SecurityMech
	Codec             iface.FrameCodec
	Role              iface.Role
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	HandshakeTimeout  time.Duration
	SendHWM           int
	RecvHWM           int

	// Logger is the destination for non-fatal diagnostics (heartbeat
	// timeout reasons, malformed-frame events). When nil, slog.Default()
	// is used.
	Logger *slog.Logger
}

// New constructs a Session. The session is inert until Start is called.
//
// The returned Session owns cfg.Conn: Stop closes it. The caller retains
// ownership of cfg.Mech and cfg.Codec, which are assumed to be safe for
// the (single) session's use until Stop returns.
func New(cfg Config) iface.Session {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.SendHWM < 0 {
		cfg.SendHWM = 0
	}
	if cfg.RecvHWM < 0 {
		cfg.RecvHWM = 0
	}
	ctx, cancel := context.WithCancel(context.Background())
	wctx, wcancel := context.WithCancel(ctx) // child of session ctx
	encode, decode, direct := sessionFramePaths(cfg.Mech, cfg.Codec)
	s := &sessionImpl{
		cfg:          cfg,
		ctx:          ctx,
		cancel:       cancel,
		reader:       sessionReader(cfg.Conn),
		encode:       encode,
		decode:       decode,
		direct:       direct,
		writerCtx:    wctx,
		writerCancel: wcancel,
		handshaked:   make(chan struct{}),
		stopped:      make(chan struct{}),
		writerExited: make(chan struct{}),
	}
	return s
}

// sessionImpl is the iface.Session implementation. The two traffic-phase
// goroutines (reader, writer) plus the optional heartbeat goroutine
// communicate via channels; mutable state shared between them
// (lastInboundUnix, handshake error) uses sync/atomic to keep -race
// happy without coarse-grained mutexes.
type sessionImpl struct {
	cfg Config

	ctx    context.Context
	cancel context.CancelFunc
	reader io.Reader
	encode frameEncoder
	decode frameDecoder
	direct iface.FrameBufferAppender

	// startOnce guards Start; stopOnce guards Stop.
	startOnce sync.Once
	stopOnce  sync.Once

	// handshaked closes when the handshake completes (success or fail).
	// HandshakeErr returns hsErr after handshaked closes. handshakedOnce
	// guards against concurrent close attempts from run() and Stop().
	handshaked     chan struct{}
	handshakedOnce sync.Once
	hsErr          atomic.Pointer[error]

	// stopped closes when both traffic-phase goroutines have exited.
	stopped     chan struct{}
	stoppedOnce sync.Once

	// writerExited closes when the writer goroutine has finished flushing.
	writerExited chan struct{}

	// writerCtx / writerCancel are a separate context for the writer's
	// blocking Recv call. Cancelling writerCtx causes the writer to drain
	// remaining messages non-blocking and exit — WITHOUT closing the conn.
	// Socket.Close calls CancelWriter after engine.Close so the writer
	// flushes the outbound queue while the conn stays alive for the reader.
	writerCtx    context.Context
	writerCancel context.CancelFunc

	wg sync.WaitGroup

	// writeMu serialises wire writes between the writer goroutine
	// (user messages) and the reader/heartbeat goroutines (PONG, PING).
	// The underlying iface.Conn does not promise concurrent-safe Write.
	writeMu sync.Mutex

	// enginePipe is the engine-facing end of the pair. sessionPipe is the
	// session-facing end (we hold it; reader pushes to it, writer pulls
	// from it). Created lazily during the handshake phase once we know
	// the post-handshake HWMs are honored.
	enginePipe  iface.Pipe
	sessionPipe iface.Pipe

	// peerHeartbeats reports whether the peer's greeting indicated ZMTP
	// 3.1 (true) — heartbeats are only emitted when both ends agreed.
	peerHeartbeats atomic.Bool

	// lastInboundUnix records the wall-clock nanos of the last frame
	// received during traffic phase. The heartbeat goroutine reads this
	// to decide whether to fire a PING; the reader writes to it on every
	// inbound frame.
	lastInboundUnix atomic.Int64

	// stopErr is the cumulative error from Stop (e.g. Conn.Close failure).
	stopErr atomic.Pointer[error]
}

const sessionReadBufferSize = 32 * 1024

type frameEncoder func(flags uint8, body []byte) (uint8, []byte, error)
type frameDecoder func(flags uint8, body []byte) (uint8, []byte, error)

func sessionFramePaths(mech iface.SecurityMech, codec iface.FrameCodec) (frameEncoder, frameDecoder, iface.FrameBufferAppender) {
	if mech == nil {
		return nil, nil, nil
	}
	switch mech.Name() {
	case "NULL", "PLAIN":
		var direct iface.FrameBufferAppender
		if appender, ok := codec.(iface.FrameBufferAppender); ok {
			direct = appender
		}
		return identityEncodeFrame, identityDecodeFrame, direct
	default:
		return mech.EncodeFrame, mech.DecodeFrame, nil
	}
}

func identityEncodeFrame(flags uint8, body []byte) (uint8, []byte, error) {
	return flags, body, nil
}

func identityDecodeFrame(flags uint8, body []byte) (uint8, []byte, error) {
	return flags, body, nil
}

func sessionReader(conn iface.Conn) io.Reader {
	if conn == nil {
		return nil
	}
	ep := conn.RemoteEndpoint()
	if ep == "" {
		ep = conn.LocalEndpoint()
	}
	switch {
	case strings.HasPrefix(ep, "tcp://"),
		strings.HasPrefix(ep, "tcp6://"),
		strings.HasPrefix(ep, "tls+tcp://"),
		strings.HasPrefix(ep, "ipc://"):
		return bufio.NewReaderSize(conn, sessionReadBufferSize)
	default:
		return conn
	}
}

// Start launches the handshake + traffic goroutines. Subsequent calls are
// no-ops.
func (s *sessionImpl) Start() {
	s.startOnce.Do(func() {
		s.wg.Add(1)
		go s.run()
		// Monitor goroutine: closes `stopped` once ALL session goroutines
		// (run itself plus reader/writer/heartbeat spawned inside run)
		// have exited. Without this the run-level defer would close
		// `stopped` as soon as run returns — which is right after it
		// spawns the workers, not after they finish. Done() consumers
		// (like Socket.Close's LINGER drain) rely on this being the true
		// end of life.
		go func() {
			s.wg.Wait()
			s.stoppedOnce.Do(func() { close(s.stopped) })
		}()
	})
}

// Stop tears the session down. It closes the conn (waking any blocked
// read/write), cancels the internal context, waits for goroutines, and
// returns the first non-nil shutdown error (if any). Stop is idempotent
// and safe to call before Start (in which case it just signals
// completion).
func (s *sessionImpl) Stop() error {
	s.stopOnce.Do(func() {
		s.cancel()
		// Closing the conn unblocks any in-flight Read/Write inside the
		// reader/writer goroutines. We do this even if Start was never
		// called so a half-constructed session releases its conn.
		// Match shutdown()'s graceful-close path so Stop, too, waits for
		// the TCP kernel send buffer to drain before the conn is torn
		// down (otherwise rapid Close-then-process-exit loses in-flight
		// data; see closeConnGraceful).
		if ls, ok := s.cfg.Conn.(interface{ SetLinger(int) error }); ok {
			_ = ls.SetLinger(2)
		}
		if err := s.cfg.Conn.Close(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			s.stopErr.CompareAndSwap(nil, &err)
		}
		// If the handshake hasn't completed yet, signal so observers
		// (and any in-flight handshake goroutine that hasn't reached
		// the close call) see a non-nil HandshakeErr. handshakedOnce
		// guarantees we only close the channel once even if run() races
		// us to it.
		s.handshakedOnce.Do(func() {
			err := errors.New("gozmq/session: stopped before handshake")
			s.hsErr.CompareAndSwap(nil, &err)
			close(s.handshaked)
		})
	})
	s.wg.Wait()
	// run() closes stopped on exit; if Start was never called we close
	// it here for symmetry. stoppedOnce keeps the close exactly-once.
	s.stoppedOnce.Do(func() { close(s.stopped) })
	if e := s.stopErr.Load(); e != nil {
		return *e
	}
	return nil
}

// Handshaked returns a channel that closes once the handshake completes
// (success or failure). HandshakeErr returns the failure reason, or nil
// on success.
func (s *sessionImpl) Handshaked() <-chan struct{} { return s.handshaked }

// HandshakeErr returns nil on a successful handshake or the wrapped
// failure error. Read after Handshaked has closed.
func (s *sessionImpl) HandshakeErr() error {
	if e := s.hsErr.Load(); e != nil {
		return *e
	}
	return nil
}

// Pipe returns the engine-facing end of the session's pipe. Valid only
// after Handshaked closes without error.
func (s *sessionImpl) Pipe() iface.Pipe { return s.enginePipe }

// Endpoint reports the peer's ZMQ endpoint string.
func (s *sessionImpl) Endpoint() string { return s.cfg.Conn.RemoteEndpoint() }

// Role reports whether this session was initiated by Connect or Accept.
func (s *sessionImpl) Role() iface.Role { return s.cfg.Role }

// Done returns a channel closed when every goroutine spawned by this
// session has exited.
func (s *sessionImpl) Done() <-chan struct{} { return s.stopped }

// WriterFlushed returns a channel that closes when the writer goroutine
// exits — the signal that all outbound messages have been flushed to conn.
func (s *sessionImpl) WriterFlushed() <-chan struct{} { return s.writerExited }

// CancelWriter cancels the writer goroutine's blocking Recv, causing it
// to drain remaining messages non-blocking and exit. The conn stays alive
// so in-flight conn.Write calls complete. Call before sess.Stop() to
// implement LINGER drain without data loss.
func (s *sessionImpl) CancelWriter() { s.writerCancel() }

// run is the top-level goroutine for a session's lifecycle. It performs
// the handshake, signals readiness, then forks the reader/writer (and
// optional heartbeat) goroutines and waits for all to exit.
func (s *sessionImpl) run() {
	defer s.wg.Done()
	// stopped is closed by the monitor goroutine in Start (or by Stop as
	// a fallback) AFTER every spawned goroutine has wg.Done'd — NOT when
	// run itself returns. See Start for why.

	if err := s.doHandshake(); err != nil {
		s.hsErr.CompareAndSwap(nil, &err)
		s.handshakedOnce.Do(func() { close(s.handshaked) })
		// On handshake failure we still close the conn to release the
		// transport. Stop(), if not yet called, will be a no-op.
		_ = s.cfg.Conn.Close()
		return
	}

	// Construct the pipe pair. The session-side end is consumed by the
	// reader (Send) and writer (Recv); the engine-side end is published
	// via Pipe().
	peerInfo := pipe.PeerInfo{
		Endpoint: s.cfg.Conn.RemoteEndpoint(),
		Identity: identityFromMetadata(s.cfg.Mech),
	}
	// HWM of 0 makes the channel synchronous, which is acceptable but
	// unusual; we honor whatever the caller passed.
	s.sessionPipe, s.enginePipe = pipe.NewPair(s.cfg.RecvHWM, s.cfg.SendHWM, pipe.PeerInfo{}, peerInfo)
	s.handshakedOnce.Do(func() { close(s.handshaked) })

	// Seed the last-inbound clock so a heartbeat fired immediately
	// after handshake doesn't trip the timeout.
	s.lastInboundUnix.Store(time.Now().UnixNano())

	// Reader and writer share the conn but on disjoint sides (reader
	// only reads, writer only writes), so they don't need to
	// coordinate beyond the shared session/pipe primitives.
	s.wg.Add(2)
	go s.runReader()
	go s.runWriter()

	// Heartbeat is optional: skip when interval is 0 or the peer
	// negotiated ZMTP 3.0.
	if s.cfg.HeartbeatInterval > 0 && s.peerHeartbeats.Load() {
		s.wg.Add(1)
		go s.runHeartbeat()
	}
}

// identityFromMetadata pulls the ZMTP "Identity" property out of the
// PEER mechanism's stashed READY metadata — that's what ROUTER expects
// on AttachPipe for routing. iface.SecurityMech doesn't expose peer
// metadata (it's write-only via SetPeerMetadata), but every concrete
// mechanism (NULL, PLAIN, CURVE) provides a PeerMetadata() accessor;
// we probe for it via type assertion.
//
// We fall back to the local mechanism's Metadata() for the test doubles
// in session_test.go whose fakeMech doesn't implement PeerMetadata, and
// for any future mechanism that might legitimately mirror local identity
// as peer identity.
func identityFromMetadata(m iface.SecurityMech) []byte {
	type peerMetaReader interface {
		PeerMetadata() map[string]string
	}
	if pm, ok := m.(peerMetaReader); ok {
		if props := pm.PeerMetadata(); props != nil {
			if v, ok := props["Identity"]; ok {
				return []byte(v)
			}
		}
	}
	if v, ok := m.Metadata()["Identity"]; ok {
		return []byte(v)
	}
	return nil
}

// shutdown is the one-stop teardown path used by reader/writer/heartbeat
// when they detect a fatal condition. It cancels the context (waking
// peer goroutines), closes the conn (waking blocked read/write), and
// closes the session-side pipe (waking the engine).
func (s *sessionImpl) shutdown() {
	s.cancel()
	closeConnGraceful(s.cfg.Conn)
	if s.sessionPipe != nil {
		_ = s.sessionPipe.Close()
	}
}

// closeConnGraceful closes c after ensuring buffered TCP data is flushed.
// For *net.TCPConn (via the SetLinger method on the iface.Conn wrapper)
// it sets SO_LINGER with a positive timeout so close(2) blocks until
// the kernel acknowledges the data or the timeout elapses — otherwise
// a rapid Close-then-process-exit races the kernel's async FIN
// transmission and queued data gets dropped before the peer TCP stack
// can deliver it. The timeout is intentionally short (a few seconds):
// user-space drain has already completed, so only the kernel send
// buffer + in-flight ACKs remain.
func closeConnGraceful(c io.Closer) {
	if ls, ok := c.(interface{ SetLinger(sec int) error }); ok {
		_ = ls.SetLinger(2)
	}
	_ = c.Close()
}

// wrapClosed returns ErrSocketClosed when err looks like a normal
// shutdown (the conn was closed by Stop or the peer EOF'd cleanly), and
// the original error otherwise. Used to keep error reporting consistent
// across the three goroutines.
func wrapClosed(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
		return zmqerr.ErrSocketClosed
	}
	return err
}

// Compile-time check.
var _ iface.Session = (*sessionImpl)(nil)
