// Package session implements the ZMTP per-peer state machine. Each Session
// wraps a single transport.Conn with two goroutines: the reader drives the
// greeting and security handshake, then moves wire frames to the pipe; the
// writer puts the handshake frames, then the pipe's messages, on the wire.
// The optional heartbeat is a self-rearming timer, not a goroutine. Raw
// mode (Config.Raw, for STREAM) keeps the two goroutines but skips ZMTP.
//
// The session consumes its collaborators through their contracts
// (zmtp.FrameCodec, security.Mech, transport.Conn), so it can be tested
// with fakes for each.
package session

import (
	"bufio"
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/exergy-dev/gozmq/internal/bufpool"
	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
	"github.com/exergy-dev/gozmq/security"
	"github.com/exergy-dev/gozmq/transport"
)

// Config bundles the inputs to New. Conn is required, and Mech and Codec
// unless Raw is set. The timeout/heartbeat values are used exactly as given:
// zero disables the corresponding behavior (no handshake deadline, no
// heartbeats, no inbound size cap). New applies no defaults to them —
// the socket layer is responsible for supplying production values.
type Config struct {
	Conn              transport.Conn
	Mech              security.Mech
	Codec             zmtp.FrameCodec
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

	// RecvBufferPool reads inbound frames larger than the reader's scratch
	// into pooled buffers and delivers them as lent messages that return
	// the buffers on their final Release (gozmq.WithRecvBufferPool). It
	// applies only to identity-framed mechanisms (NULL, PLAIN), whose
	// decoded frame is the buffer itself.
	RecvBufferPool bool

	// SubscriptionCommands turns inbound ZMTP SUBSCRIBE/CANCEL commands
	// into 0x01/0x00-prefixed messages on the pipe. Set it only for
	// PUB/XPUB; every other socket type drops the commands.
	SubscriptionCommands bool

	// Raw carries bytes with no ZMTP at all (STREAM): no greeting,
	// handshake or heartbeat, each conn.Read is one single-frame inbound
	// message, and each outbound message is one body frame written
	// verbatim, an empty one closing the conn. Mech, Codec and the
	// ZMTP-only fields are ignored. The mode is fixed at New, so the ZMTP
	// loops carry no per-message branch for it.
	Raw bool

	// OnReady runs on the reader goroutine once the handshake succeeds,
	// with the engine-facing pipe end, before any inbound frame is read.
	// An error (the engine refused the pipe) tears the session down
	// unattached. nil accepts every pipe.
	OnReady func(*pipe.End) error

	// OnClosed runs exactly once, on the last session goroutine to exit,
	// after the conn is closed and before Done closes. err is the
	// handshake failure, nil once the handshake succeeded; attached
	// reports whether OnReady accepted the pipe.
	//
	// Callbacks must never call Stop: it waits for the goroutine running
	// them.
	OnClosed func(err error, attached bool)

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

// Session owns the ZMTP state machine for a single peer connection: it
// drives greeting, handshake, heartbeats, and the traffic phase. It sits
// between a transport.Conn (bytes) and a pipe (messages).
//
// Exactly two goroutines run per session. The reader performs the
// handshake, calls OnReady, then moves wire frames to the pipe. The writer
// writes the handshake frames, then moves pipe messages to the wire. The
// last of the two to exit closes the conn, calls OnClosed and closes done.
type Session struct {
	cfg Config

	ctx    context.Context
	cancel context.CancelFunc
	reader *bufio.Reader

	// identityMech reports that the mechanism leaves frames unchanged
	// (NULL, PLAIN): the writer hands user frames to writev by reference
	// and the reader copies small frames out of its scratch. Then encode
	// and decode are nil and never called. Otherwise they are the
	// mechanism's frame transforms, and appendEncode its
	// security.FrameAppender when it has one, which the writer prefers so
	// the wire body lands in the batch's own buffer.
	identityMech bool
	encode       frameFunc
	decode       frameFunc
	appendEncode func(dst []byte, flags uint8, plain []byte) ([]byte, uint8, error)

	// recvPool supplies the reader's large frame buffers: the shared
	// bufpool (bodies lent to the application) when pooledRecv is set, else
	// a per-session plainBodies. See Config.RecvBufferPool.
	recvPool   zmtp.BodyPool
	pooledRecv bool

	// hsOut carries the greeting and encoded handshake frames from the
	// reader to the writer, which writes them while the reader reads, so a
	// peer on a synchronous transport that writes before it reads cannot
	// deadlock us. The reader closes it when the handshake ends, having set
	// the pipes on success, so the last handshake frame precedes any
	// traffic. hsWriteErr records the writer's failure.
	hsOut      chan []byte
	hsWriteErr atomic.Pointer[error]

	// hsErr is the handshake failure (nil on success). The reader sets it
	// before it exits; the last goroutine out reads it for OnClosed.
	hsErr    error
	attached atomic.Bool

	// live counts running session goroutines; the one that drops it to
	// zero runs exit's teardown. done closes after OnClosed returns;
	// writerDone when the writer has exited.
	live       atomic.Int32
	done       chan struct{}
	writerDone chan struct{}

	// cmdCh carries serialized ZMTP command frames (PONG from the reader,
	// PING from the heartbeat) to the writer goroutine, the ONLY goroutine
	// that writes to the wire. Writing inline from the reader would stall
	// reads while the writer is parked in conn.Write (peer not draining),
	// and two saturated peers would deadlock permanently. The channel is
	// small and lossy by design — see enqueueCommand.
	cmdCh chan []byte
	// cmdWake (cap 1) is armed with every queued command. It is the
	// writer's abort channel for its blocking pipe receive; the command
	// itself is read from cmdCh.
	cmdWake chan struct{}

	// writerCtx / writerCancel are a separate context for the writer's
	// blocking Recv call. Cancelling writerCtx (Drain) makes the writer
	// flush the remaining queue and exit — WITHOUT closing the conn.
	writerCtx    context.Context
	writerCancel context.CancelFunc

	// enginePipe is the engine-facing end of the pair, handed to OnReady.
	// sessionPipe is the session-facing end (reader pushes to it, writer
	// pulls from it). The reader sets both before it closes hsOut; in
	// raw mode New does.
	enginePipe  *pipe.End
	sessionPipe *pipe.End

	// peerHeartbeats reports whether the peer's greeting indicated ZMTP
	// 3.1+ (true) — heartbeats are only emitted when both ends agreed.
	// Written and read on the reader goroutine only.
	peerHeartbeats bool

	// lastInboundUnix records the monotonic nanos (relative to
	// monoEpoch) of the last frame received during traffic phase. The
	// heartbeat tick reads this to decide whether the peer is alive; the
	// reader writes it on every command frame and once per read batch of
	// user messages (flushInbound), so it can lag by one batch.
	lastInboundUnix atomic.Int64

	// inboundOwed counts messages the reader published with
	// TrySendDeferred since its last flushInbound. Reader goroutine only.
	inboundOwed int

	// readerParked is set while the reader goroutine is blocked
	// publishing an inbound message into the session pipe (local engine
	// backpressure). No inbound stamps can occur while the reader is
	// parked, but the link itself is healthy — the stall is local — so
	// the heartbeat timeout check skips teardown while it is set.
	readerParked atomic.Bool

	// hbMu guards the heartbeat timer. A tick holds it throughout, so once
	// stopHeartbeat returns no tick is running or will act.
	hbMu      sync.Mutex
	hbTimer   *time.Timer
	hbStopped bool
}

const sessionReadBufferSize = 16 * 1024

// sessionCmdBacklog sizes cmdCh. PING/PONG are advisory keep-alives, so
// a small buffer suffices; enqueueCommand drops on overflow.
const sessionCmdBacklog = 4

// hsOutBacklog sizes hsOut. The handshake is lock-step, so only a frame or
// two is ever in flight; the slack means the reader never waits on a writer
// stalled behind a peer that is itself still writing.
const hsOutBacklog = 4

// monoEpoch anchors monoNow, a monotonic-clock timestamp that is cheaper
// than time.Now().UnixNano() on the per-frame path.
var monoEpoch = time.Now()

func monoNow() int64 { return int64(time.Since(monoEpoch)) }

// plainBodies is the reader's zmtp.BodyPool when bodies are not pooled:
// Get is a plain allocation the *Msg adopts (Put is only reached on a failed
// read, where the buffer is garbage anyway). The intermediate a large frame
// is collected in before its body is allocated is kept on the session, one
// at a time and only up to maxRetainedRecvScratch, so a stream of large frames
// costs exactly one allocation each, the body. The reader goroutine is the
// only user, so there is no locking.
//
// With reuse set (a mechanism that rewrites bodies, so the reader keeps
// nothing of the wire body once DecodeFrame has run) Get hands out the same
// buffer every time, grown as needed up to maxRetainedRecvScratch.
type plainBodies struct {
	scratch []byte
	body    []byte
	reuse   bool
}

// maxRetainedRecvScratch caps the intermediate a session keeps between frames;
// it is the codec's incremental threshold, so every frame up to 2 MiB reuses
// it and a rare larger one leaves its bigger intermediate to the GC.
const maxRetainedRecvScratch = 128 << 10

func (p *plainBodies) Get(n int) []byte {
	if !p.reuse || n > maxRetainedRecvScratch {
		return make([]byte, n)
	}
	if cap(p.body) < n {
		p.body = make([]byte, n)
	}
	return p.body[:n]
}

func (*plainBodies) Put([]byte) {}

func (p *plainBodies) GetScratch(n int) []byte {
	if cap(p.scratch) >= n {
		b := p.scratch[:n]
		p.scratch = nil
		return b
	}
	return make([]byte, n)
}

func (p *plainBodies) PutScratch(b []byte) {
	if cap(b) <= maxRetainedRecvScratch && cap(b) > cap(p.scratch) {
		p.scratch = b[:0]
	}
}

// frameFunc is the shape of Mech.EncodeFrame and Mech.DecodeFrame.
type frameFunc func(flags uint8, body []byte) (uint8, []byte, error)

// identityFramer is implemented by mechanisms whose EncodeFrame and
// DecodeFrame are the identity transform (NULL, PLAIN), opting them into
// the zero-copy framing path. The mechanism must declare that itself:
// keying on Name() would silently bypass the frame transform of any custom
// mechanism that happens to be called "NULL" or "PLAIN".
type identityFramer interface {
	IdentityFraming() bool
}

// New constructs a Session around cfg.Conn; Start runs it. New lets the
// caller register the session (for Close) before any callback can fire.
//
// The Session owns cfg.Conn once started: it closes it on teardown. The
// caller retains ownership of cfg.Mech and cfg.Codec, which are assumed
// to be safe for the session's use until Done closes.
func New(cfg Config) *Session {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = monoNow
	}
	ctx, cancel := context.WithCancel(context.Background())
	wctx, wcancel := context.WithCancel(ctx) // child of session ctx
	s := &Session{
		cfg:          cfg,
		ctx:          ctx,
		cancel:       cancel,
		writerCtx:    wctx,
		writerCancel: wcancel,
		done:         make(chan struct{}),
		writerDone:   make(chan struct{}),
	}
	if cfg.Raw {
		// Raw mode has no handshake: the pipes exist from the start.
		s.sessionPipe, s.enginePipe = pipe.NewPair(cfg.RecvHWM, cfg.SendHWM, nil, nil)
		return s
	}
	if f, ok := cfg.Mech.(identityFramer); ok && f.IdentityFraming() {
		s.identityMech = true
	} else {
		s.encode, s.decode = cfg.Mech.EncodeFrame, cfg.Mech.DecodeFrame
		if a, ok := cfg.Mech.(security.FrameAppender); ok {
			s.appendEncode = a.AppendEncodeFrame
		}
	}
	// recvPool hands the reader pooled frame buffers (see Config.RecvBufferPool).
	switch {
	case cfg.RecvBufferPool && s.identityMech:
		s.recvPool, s.pooledRecv = bufpool.Shared{}, true
	default:
		s.recvPool = &plainBodies{reuse: !s.identityMech}
	}
	s.reader = bufio.NewReaderSize(flushReader{s: s, r: cfg.Conn}, sessionReadBufferSize)
	s.hsOut = make(chan []byte, hsOutBacklog)
	s.cmdCh = make(chan []byte, sessionCmdBacklog)
	s.cmdWake = make(chan struct{}, 1)
	return s
}

// Start launches the reader and writer goroutines. Call it exactly once.
func (s *Session) Start() {
	s.live.Store(2)
	if s.cfg.Raw {
		go s.runRawReader()
		go s.runRawWriter()
		return
	}
	go s.runReader()
	go s.runWriter()
}

// Stop tears the session down: it closes the conn (waking any blocked
// read or write), cancels the session context, and blocks until Done.
// It returns the conn's close error, if any. Idempotent; never call it
// from OnReady or OnClosed.
func (s *Session) Stop() error {
	s.cancel()
	// A plain close: the kernel keeps delivering already-written data
	// after close(2), whereas a timed SO_LINGER would block here (per
	// session, sequentially during Socket.Close) and RST on expiry.
	// isShutdownErr covers both the test double's io.ErrClosedPipe and the
	// net.ErrClosed of a conn the session already closed itself.
	err := s.cfg.Conn.Close()
	<-s.done
	if err != nil && !zmqerr.IsShutdown(err) {
		return err
	}
	return nil
}

// Drain makes the writer flush the messages still queued and exit without
// closing the conn, so in-flight writes complete; the returned channel
// closes once it has. Call engine.Close first so nothing new is queued,
// wait on the channel, then call Stop. Idempotent.
func (s *Session) Drain() <-chan struct{} {
	s.writerCancel()
	return s.writerDone
}

// Done returns a channel closed once both session goroutines have exited
// and OnClosed has returned.
func (s *Session) Done() <-chan struct{} { return s.done }

// Attached reports whether OnReady accepted the session's pipe.
func (s *Session) Attached() bool { return s.attached.Load() }

// exit runs as each session goroutine returns. The last one out closes
// the conn (a failed handshake leaves that to it, so the writer can still
// flush an ERROR command), reports OnClosed and closes done.
func (s *Session) exit() {
	if s.live.Add(-1) != 0 {
		return
	}
	s.shutdown()
	if s.cfg.OnClosed != nil {
		s.cfg.OnClosed(s.hsErr, s.attached.Load())
	}
	close(s.done)
}

// enqueueCommand serialises a ZMTP command and hands it to the writer
// goroutine via cmdCh: the reader and heartbeat never write to the wire
// themselves (see the cmdCh field doc). The body is copied, so callers may
// pass slices aliasing the reader's scratch buffer.
//
// If the channel is full the command is dropped: PING/PONG are advisory
// keep-alives — the peer (or our next tick) simply retries — and
// dropping is the price of never blocking the reader on the writer.
func (s *Session) enqueueCommand(name string, body []byte) {
	buf := zmtp.EncodeCommand(zmtp.Command{Name: name, Body: body})
	select {
	case s.cmdCh <- buf:
		select {
		case s.cmdWake <- struct{}{}:
		default:
		}
	default:
		s.cfg.Logger.Debug("gozmq/session: command channel full; dropping", "name", name)
	}
}

// attach hands the engine-facing pipe end to OnReady (nil accepts) and
// records the session as attached. It reports false when the engine
// refused the pipe, and the reader must exit.
func (s *Session) attach() bool {
	if s.cfg.OnReady != nil {
		if err := s.cfg.OnReady(s.enginePipe); err != nil {
			s.cfg.Logger.Debug("gozmq/session: pipe refused", "err", err)
			return false
		}
	}
	s.attached.Store(true)
	return true
}

// logUnlessShutdown logs err at debug level unless it is the ordinary
// result of the session or its conn being closed.
func logUnlessShutdown(l *slog.Logger, msg string, err error) {
	if !zmqerr.IsShutdown(err) {
		l.Debug(msg, "err", err)
	}
}

// shutdown is the one-stop teardown path used by the reader and writer
// when they detect a fatal condition, and by exit. It stops the
// heartbeat, cancels the context (waking the other goroutine), closes the
// conn (waking blocked read/write), and closes the session-side pipe
// (waking the engine). Only the session goroutines call it, after the
// reader has published sessionPipe.
func (s *Session) shutdown() {
	s.stopHeartbeat()
	s.cancel()
	_ = s.cfg.Conn.Close()
	if s.sessionPipe != nil {
		_ = s.sessionPipe.Close()
	}
}
