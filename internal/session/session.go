// Package session implements the ZMTP per-peer state machine. Each Session
// wraps a single iface.Conn, drives greeting + security handshake, then
// runs two traffic-phase goroutines (reader: wire->pipe, writer: pipe->wire)
// plus an optional heartbeat scheduler.
//
// The session layer never imports the concrete framing/security/transport
// packages — it consumes the iface contracts (FrameCodec, SecurityMech,
// Conn) so it can be developed and tested in isolation.
package session

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/msg"
)

// Config bundles the inputs to New. Conn, Mech, and Codec are
// required. The timeout/heartbeat values are used exactly as given:
// zero disables the corresponding behavior (no handshake deadline, no
// heartbeats, no inbound size cap). New applies no defaults to them —
// the socket layer is responsible for supplying production values.
type Config struct {
	Conn              iface.Conn
	Mech              iface.SecurityMech
	Codec             iface.FrameCodec
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	HandshakeTimeout  time.Duration
	SendHWM           int
	RecvHWM           int

	// MaxMessageSize caps the total decoded payload, in bytes, of one
	// logical inbound message — the sum of all frame lengths accumulated
	// while FlagMore is set. The frame codec enforces only a per-frame
	// cap, so without this a peer could stream an endless MORE chain and
	// OOM the receiver. When the cap is exceeded the session tears down
	// with an error wrapping zmqerr.ErrMalformedFrame. 0 means unlimited.
	// The root socket layer wires its option through this field.
	MaxMessageSize int64

	// Logger is the destination for non-fatal diagnostics (heartbeat
	// timeout reasons, malformed-frame events). When nil, slog.Default()
	// is used.
	Logger *slog.Logger

	// Now returns a monotonic nanosecond timestamp used to stamp inbound
	// frames and measure heartbeat idle time. When nil it defaults to the
	// package monoNow (runtime monotonic clock). Tests inject a fake clock
	// here to drive heartbeat-timeout behavior deterministically without
	// real elapsed time.
	Now func() int64
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
	if cfg.Now == nil {
		cfg.Now = monoNow
	}
	ctx, cancel := context.WithCancel(context.Background())
	wctx, wcancel := context.WithCancel(ctx) // child of session ctx
	encode, decode, identityMech := sessionFramePaths(cfg.Mech)
	// directHeader is the writer hot-path fast lane: when the mechanism
	// is an identity transform (NULL/PLAIN — encode does not rewrite
	// body bytes) and the codec implements FrameHeaderAppender, the
	// writer appends wire headers into a caller-owned scratch buffer
	// with zero per-frame allocations.
	var directHeader iface.FrameHeaderAppender
	if identityMech {
		if h, ok := cfg.Codec.(iface.FrameHeaderAppender); ok {
			directHeader = h
		}
	}
	s := &sessionImpl{
		cfg:          cfg,
		ctx:          ctx,
		cancel:       cancel,
		reader:       sessionReader(cfg.Conn),
		encode:       encode,
		decode:       decode,
		identityMech: identityMech,
		directHeader: directHeader,
		writerCtx:    wctx,
		writerCancel: wcancel,
		handshaked:   make(chan struct{}),
		stopped:      make(chan struct{}),
		writerExited: make(chan struct{}),
		cmdCh:        make(chan []byte, sessionCmdBacklog),
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
	// identityMech reports a NULL/PLAIN security mechanism: encode and
	// decode are identity transforms, so frame bodies cross the wire
	// unmodified. Gates the reader's zero-copy pooled-frame path.
	identityMech bool
	directHeader iface.FrameHeaderAppender

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
	// writerExitedOnce guards the close: besides the writer's own defer,
	// the handshake-failure path in run() and Stop() (covering
	// Stop-before-Start) must also release WriterFlushed waiters or
	// Socket.CloseCtx's linger drain would hang forever.
	writerExited     chan struct{}
	writerExitedOnce sync.Once

	// cmdCh carries serialized ZMTP command frames (PONG from the reader,
	// PING from the heartbeat) to the writer goroutine, which is the ONLY
	// goroutine that performs traffic-phase wire writes. Writing inline
	// from the reader would take writeMu on the read goroutine: if the
	// writer is parked in conn.Write (peer not draining) the reader would
	// stop reading and two saturated peers deadlock permanently. The
	// channel is small and lossy by design — see enqueueCommand.
	cmdCh chan []byte

	// writerCtx / writerCancel are a separate context for the writer's
	// blocking Recv call. Cancelling writerCtx causes the writer to drain
	// remaining messages non-blocking and exit — WITHOUT closing the conn.
	// Socket.Close calls CancelWriter after engine.Close so the writer
	// flushes the outbound queue while the conn stays alive for the reader.
	writerCtx    context.Context
	writerCancel context.CancelFunc

	wg sync.WaitGroup

	// writeMu serialises wire writes. Since PONG/PING are routed through
	// cmdCh, only the writer goroutine performs traffic-phase writes, so
	// the mutex is uncontended in practice; it is kept as a guard against
	// future callers writing off the writer goroutine. (The underlying
	// iface.Conn does not promise concurrent-safe Write.)
	writeMu sync.Mutex

	// enginePipe is the engine-facing end of the pair. sessionPipe is the
	// session-facing end (we hold it; reader pushes to it, writer pulls
	// from it). Created lazily during the handshake phase once we know
	// the post-handshake HWMs are honored.
	enginePipe  iface.Pipe
	sessionPipe sessionChanPipe

	// peerHeartbeats reports whether the peer's greeting indicated ZMTP
	// 3.1+ (true) — heartbeats are only emitted when both ends agreed.
	// Written in exchangeGreeting and read later in run(), both on the
	// run goroutine's call chain, so a plain bool suffices.
	peerHeartbeats bool

	// lastInboundUnix records the monotonic nanos (relative to
	// monoEpoch) of the last frame received during traffic phase. The
	// heartbeat goroutine reads this to decide whether to fire a PING;
	// the reader writes to it on every first inbound message frame and
	// on command frames.
	lastInboundUnix atomic.Int64

	// readerParked is set while the reader goroutine is blocked
	// publishing an inbound message into the session pipe (local engine
	// backpressure). No inbound stamps can occur while the reader is
	// parked, but the link itself is healthy — the stall is local — so
	// the heartbeat timeout check skips teardown while it is set.
	readerParked atomic.Bool

	// stopErr is the cumulative error from Stop (e.g. Conn.Close failure).
	stopErr atomic.Pointer[error]
}

const sessionReadBufferSize = 16 * 1024

// sessionCmdBacklog sizes cmdCh. PING/PONG are advisory keep-alives, so
// a small buffer suffices; enqueueCommand drops on overflow.
const sessionCmdBacklog = 4

// monoEpoch is a fixed reference point captured once at package init.
// monoNow returns time.Since(monoEpoch) as int64 nanoseconds, which
// uses Go's monotonic clock (runtime.nanotime) -- cheaper than
// time.Now().UnixNano() because it skips the wall-clock vDSO read and
// subsequent conversion. The heartbeat timeout is 90 s, so monotonic
// precision is more than adequate.
var monoEpoch = time.Now()

// monoNow returns a monotonic nanosecond timestamp relative to
// monoEpoch. Strictly cheaper than time.Now().UnixNano() on the
// hot path because time.Since uses the monotonic clock directly.
func monoNow() int64 {
	return int64(time.Since(monoEpoch))
}

type frameEncoder func(flags uint8, body []byte) (uint8, []byte, error)
type frameDecoder func(flags uint8, body []byte) (uint8, []byte, error)

// sessionFramePaths picks the per-frame encode/decode functions and
// reports whether the mechanism is an identity transform (NULL/PLAIN).
func sessionFramePaths(mech iface.SecurityMech) (frameEncoder, frameDecoder, bool) {
	switch mech.Name() {
	case "NULL", "PLAIN":
		return identityEncodeFrame, identityDecodeFrame, true
	default:
		return mech.EncodeFrame, mech.DecodeFrame, false
	}
}

// sessionChanPipe is the channel-level surface the session requires of
// its own pipe end: the iface.Pipe contract plus the non-blocking and
// select-able accessors the reader/writer hot paths use. *pipe.end (the
// sole iface.Pipe implementation, produced by pipe.NewPair in run())
// implements it; asSessionChanPipe panics on anything else, mirroring
// internal/socket's asChanPipe contract.
type sessionChanPipe interface {
	iface.Pipe
	// RecvCh exposes the inbound channel so the writer can multiplex
	// user messages with cmdCh and shutdown in a single select.
	RecvCh() <-chan *msg.Msg
	// TryRecv is a non-blocking Recv; false when empty or closed.
	TryRecv() (*msg.Msg, bool)
	// TrySend is a non-blocking Send; false when full or closed.
	TrySend(*msg.Msg) bool
}

// asSessionChanPipe asserts the channel-level surface once, where the
// pipe pair is created. Failure means a new pipe type was introduced
// without the channel accessors — a programmer error worth the panic.
func asSessionChanPipe(p iface.Pipe) sessionChanPipe {
	cp, ok := p.(sessionChanPipe)
	if !ok {
		panic(fmt.Sprintf("gozmq/session: pipe %T lacks the channel-level accessors (RecvCh/TryRecv/TrySend); *pipe.end is the only supported pipe implementation", p))
	}
	return cp
}

func identityEncodeFrame(flags uint8, body []byte) (uint8, []byte, error) {
	return flags, body, nil
}

func identityDecodeFrame(flags uint8, body []byte) (uint8, []byte, error) {
	return flags, body, nil
}

// sessionReader wraps stream transports in a bufio layer.
//
// TODO(transport): invert this — the transport layer knows whether its
// conns benefit from userspace read buffering; sniffing endpoint URL
// prefixes here couples the session to endpoint syntax. Behavior is
// kept as-is for now.
func sessionReader(conn iface.Conn) io.Reader {
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
		// isShutdownErr covers both the test double's io.ErrClosedPipe and
		// the net.ErrClosed a real conn returns when shutdown() already
		// closed it — the common teardown path must not surface a spurious
		// "use of closed network connection".
		if err := closeConnGraceful(s.cfg.Conn); err != nil && !isShutdownErr(err) {
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
	// Every session goroutine has exited (or none ever started). If the
	// writer never ran — Stop before Start, or a handshake that never
	// completed — WriterFlushed waiters must still be released.
	s.closeWriterExited()
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
		// No writer goroutine will ever run; release WriterFlushed
		// waiters so a linger drain doesn't hang on a session that died
		// during its handshake.
		s.closeWriterExited()
		// On handshake failure we still close the conn to release the
		// transport. Stop(), if not yet called, will be a no-op.
		_ = s.cfg.Conn.Close()
		return
	}

	// Construct the pipe pair. The session-side end is consumed by the
	// reader (Send) and writer (Recv); the engine-side end is published
	// via Pipe().
	peerInfo := pipe.PeerInfo{
		Identity: identityFromMetadata(s.cfg.Mech),
	}
	sp, ep := pipe.NewPair(s.cfg.RecvHWM, s.cfg.SendHWM, pipe.PeerInfo{}, peerInfo)
	s.sessionPipe, s.enginePipe = asSessionChanPipe(sp), ep
	s.handshakedOnce.Do(func() { close(s.handshaked) })

	// Seed the last-inbound clock so a heartbeat fired immediately
	// after handshake doesn't trip the timeout.
	s.lastInboundUnix.Store(s.cfg.Now())

	// Reader and writer share the conn but on disjoint sides (reader
	// only reads, writer only writes), so they don't need to
	// coordinate beyond the shared session/pipe primitives.
	s.wg.Add(2)
	go s.runReader()
	go s.runWriter()

	// Heartbeat is optional: skip when interval is 0 or the peer
	// negotiated ZMTP 3.0.
	if s.cfg.HeartbeatInterval > 0 && s.peerHeartbeats {
		s.wg.Add(1)
		go s.runHeartbeat()
	}
}

// identityFromMetadata pulls the ZMTP "Identity" property out of the
// PEER mechanism's stashed READY metadata — that's what ROUTER expects
// on AttachPipe for routing. It is only called after finalizePeerMetadata
// has succeeded, so peerMetadataFromMechanism cannot fail here in
// practice; a probe failure just means no identity.
func identityFromMetadata(m iface.SecurityMech) []byte {
	props, err := peerMetadataFromMechanism(m)
	if err != nil {
		return nil
	}
	if v, ok := props["Identity"]; ok {
		return []byte(v)
	}
	return nil
}

// closeWriterExited releases WriterFlushed waiters exactly once. Called
// from the writer goroutine's defer, the handshake-failure path in run(),
// and Stop() (covering Stop-before-Start).
func (s *sessionImpl) closeWriterExited() {
	s.writerExitedOnce.Do(func() { close(s.writerExited) })
}

// enqueueCommand serialises a ZMTP command (1-byte name length + name +
// body) and hands it to the writer goroutine via cmdCh — the reader and
// heartbeat goroutines must never write to the wire themselves (see the
// cmdCh field doc). The body bytes are copied during serialization, so
// callers may pass slices aliasing pooled buffers.
//
// If the channel is full the command is dropped: PING/PONG are advisory
// keep-alives — the peer (or our next tick) simply retries — and
// dropping is the price of never blocking the reader on the writer.
func (s *sessionImpl) enqueueCommand(name string, body []byte) {
	buf := make([]byte, 0, 1+len(name)+len(body))
	buf = append(buf, byte(len(name)))
	buf = append(buf, name...)
	buf = append(buf, body...)
	select {
	case s.cmdCh <- buf:
	default:
		s.cfg.Logger.Debug("gozmq/session: command channel full; dropping", "name", name)
	}
}

// shutdown is the one-stop teardown path used by reader/writer/heartbeat
// when they detect a fatal condition. It cancels the context (waking
// peer goroutines), closes the conn (waking blocked read/write), and
// closes the session-side pipe (waking the engine).
func (s *sessionImpl) shutdown() {
	s.cancel()
	_ = closeConnGraceful(s.cfg.Conn)
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
func closeConnGraceful(c io.Closer) error {
	if ls, ok := c.(interface{ SetLinger(sec int) error }); ok {
		_ = ls.SetLinger(2)
	}
	return c.Close()
}

// Compile-time check.
var _ iface.Session = (*sessionImpl)(nil)
