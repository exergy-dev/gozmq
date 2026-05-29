package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/pipe"
)

// defaultReadBufSize is the max chunk pulled from conn.Read per iteration.
// 32 KiB is large enough to absorb a typical HTTP request/response head in
// one syscall and small enough that per-peer steady-state memory stays
// under a handful of pages. Each call is still just one conn.Read — the
// adapter never concatenates reads, honoring the STREAM "bytes-as-they-
// arrive" contract.
const defaultReadBufSize = 32 * 1024

// defaultPipeHWM is the per-direction high-water mark for the engine<->
// adapter pipe. Matches session.Config defaults in spirit: bounded enough
// to apply backpressure, large enough that a bursty HTTP client does not
// stall mid-request.
const defaultPipeHWM = 1024

// Config parameterizes an Adapter. All fields are optional; sane defaults
// apply when zero.
type Config struct {
	// Identity is the ZMTP-style peer identity advertised to the engine
	// via PeerInfo.Identity. Typically empty for STREAM (raw TCP peers
	// do not announce one); the engine synthesizes a 5-byte identity in
	// that case, matching libzmq's anonymous-peer convention.
	Identity []byte

	// Endpoint is the peer's ZMQ endpoint string (e.g. "tcp://host:port").
	// Surfaces through pipe.PeerEndpoint() for monitoring/logging.
	Endpoint string

	// ReadBufSize caps each conn.Read. Defaults to 32 KiB when <= 0.
	ReadBufSize int

	// SendHWM / RecvHWM bound the adapter<->engine pipe in each
	// direction. Default defaultPipeHWM when <= 0. A value of 0 means
	// "default", not "unbuffered" — raw-stream traffic without a buffer
	// is almost never what the caller wants.
	SendHWM int
	RecvHWM int
}

// Adapter wraps an iface.Conn and exposes an iface.Pipe to the
// streamEngine. It owns two goroutines for the lifetime of the conn:
//
//	reader: conn.Read → pipe.Send(Msg{chunk})
//	writer: pipe.Recv → conn.Write(body)  (empty body closes the conn)
//
// Adapter.EnginePipe is the pipe end the caller passes to
// engine.AttachPipe. The caller is responsible for also calling
// engine.DetachPipe on shutdown — the Adapter signals that via
// EnginePipe's Closed() channel, which fires when either goroutine exits.
type Adapter struct {
	conn    iface.Conn
	engEnd  iface.Pipe // handed to streamEngine via AttachPipe
	admEnd  iface.Pipe // used internally by reader/writer goroutines
	bufSize int

	ctx    context.Context
	cancel context.CancelFunc

	closeOnce sync.Once
	closeErr  error

	doneWG sync.WaitGroup
}

// NewAdapter constructs an Adapter around conn, wires up the engine<->
// adapter pipe, and launches reader/writer goroutines. The returned
// Adapter's EnginePipe() is ready to pass to streamEngine.AttachPipe.
//
// parent is the Socket's run context; cancelling it tears the adapter
// down (reader/writer exit, conn.Close, pipes close). The returned
// cancel func on Adapter.Close is an additional escape hatch.
//
// conn is owned by the Adapter from this point on; the adapter closes it
// on shutdown. Callers MUST NOT Read/Write conn directly afterwards.
func NewAdapter(parent context.Context, conn iface.Conn, cfg Config) *Adapter {
	if cfg.ReadBufSize <= 0 {
		cfg.ReadBufSize = defaultReadBufSize
	}
	if cfg.SendHWM <= 0 {
		cfg.SendHWM = defaultPipeHWM
	}
	if cfg.RecvHWM <= 0 {
		cfg.RecvHWM = defaultPipeHWM
	}

	engInfo := pipe.PeerInfo{
		Endpoint: cfg.Endpoint,
		Identity: bytes.Clone(cfg.Identity),
	}
	// admInfo is what the adapter end sees as "the peer" — the engine
	// side. STREAM has no identity to advertise in that direction, so
	// leave it zero.
	admInfo := pipe.PeerInfo{}

	// NewPair(aToB, bToA, aInfo, bInfo): a is engEnd, b is admEnd.
	// aToB = engine→adapter traffic (outbound writes).
	// bToA = adapter→engine traffic (inbound reads).
	engEnd, admEnd := pipe.NewPair(cfg.SendHWM, cfg.RecvHWM, engInfo, admInfo)

	ctx, cancel := context.WithCancel(parent)
	a := &Adapter{
		conn:    conn,
		engEnd:  engEnd,
		admEnd:  admEnd,
		bufSize: cfg.ReadBufSize,
		ctx:     ctx,
		cancel:  cancel,
	}

	a.doneWG.Add(2)
	go a.readerLoop()
	go a.writerLoop()
	return a
}

// EnginePipe returns the pipe end the caller hands to
// streamEngine.AttachPipe. Valid immediately after NewAdapter.
func (a *Adapter) EnginePipe() iface.Pipe { return a.engEnd }

// Close tears the adapter down: cancels the internal ctx, closes the
// underlying conn, closes both pipe ends, and waits for the reader /
// writer goroutines to exit. Idempotent.
func (a *Adapter) Close() error {
	a.closeOnce.Do(func() {
		a.cancel()
		// Closing conn unblocks any pending conn.Read in the reader.
		// net.Conn.Close returning "use of closed network connection"
		// on a second call is normal and silently swallowed by
		// isExpectedTeardownErr up the stack.
		if err := a.conn.Close(); err != nil && !isBenignCloseErr(err) {
			a.closeErr = err
		}
		_ = a.admEnd.Close()
		_ = a.engEnd.Close()
	})
	a.doneWG.Wait()
	return a.closeErr
}

// readerLoop pulls bytes off the conn and forwards each chunk as a
// single-frame Msg onto the adapter-side pipe. Exits on ctx cancel,
// conn error, or pipe close.
func (a *Adapter) readerLoop() {
	defer a.doneWG.Done()
	defer func() {
		// Close our end so the engine sees io.EOF on Recv. Also
		// cancel so the writer exits promptly; the caller should
		// DetachPipe in response to engEnd.Closed() firing.
		_ = a.admEnd.Close()
		a.cancel()
	}()

	for {
		// Fast path: ctx already done.
		select {
		case <-a.ctx.Done():
			return
		default:
		}

		buf := make([]byte, a.bufSize)
		n, err := a.conn.Read(buf)
		if n > 0 {
			// Deliver the chunk as a single-frame Msg. The engine's
			// streamEngine tags it with the peer identity before
			// surfacing to the app.
			m := msg.FromBytes(buf[:n])
			if sendErr := a.admEnd.Send(a.ctx, m); sendErr != nil {
				// Pipe closed or ctx cancelled — shut down.
				return
			}
		}
		if err != nil {
			// EOF / closed-network / ctx-interrupted I/O all look
			// alike and all mean "this peer is gone".
			return
		}
	}
}

// writerLoop drains outbound Msgs from the adapter-side pipe and writes
// their body bytes to conn. A zero-length body is the STREAM half-close
// sentinel: close the conn and exit.
//
// The streamEngine upstream collapses multi-frame bodies into exactly one
// body frame before Send, so the normal case is NumFrames()==1. We still
// tolerate multi-frame Msgs (concatenate) defensively.
func (a *Adapter) writerLoop() {
	defer a.doneWG.Done()
	defer func() {
		// Cancel + close our pipe end so the reader exits and the
		// engine sees the detach signal (engEnd.Closed() fires).
		_ = a.admEnd.Close()
		a.cancel()
	}()

	for {
		m, err := a.admEnd.Recv(a.ctx)
		if err != nil {
			// EOF (pipe peer closed), ctx cancel, or other error.
			// The writer closing the conn here is important on ctx
			// cancel; on pipe-peer-close it is a no-op harmless
			// belt-and-braces.
			_ = a.conn.Close()
			return
		}

		body := flattenBody(m)
		if len(body) == 0 {
			// libzmq STREAM half-close: empty body = conn.Close.
			// Drop any further outbound traffic (pipe close also
			// signals the engine to DetachPipe).
			_ = a.conn.Close()
			return
		}

		// Write the full body. On partial write net.Conn guarantees
		// io.ErrShortWrite if n<len(body)&&err==nil; treat any write
		// error as a terminal condition.
		if _, werr := writeFull(a.conn, body); werr != nil {
			_ = a.conn.Close()
			return
		}
	}
}

// flattenBody returns the concatenation of every frame in m. For the
// expected case (streamEngine hands down a one-frame body) this is just
// m.Frames()[0]; the slow-path loop covers misuse and defensive paranoia.
func flattenBody(m *msg.Msg) []byte {
	if m == nil || m.NumFrames() == 0 {
		return nil
	}
	frames := m.Frames()
	if len(frames) == 1 {
		return frames[0]
	}
	total := 0
	for _, f := range frames {
		total += len(f)
	}
	out := make([]byte, 0, total)
	for _, f := range frames {
		out = append(out, f...)
	}
	return out
}

// writeFull loops until every byte has been written or an error surfaces.
// net.Conn.Write typically returns everything in one call, but honoring
// io.Writer semantics for non-net implementations (e.g. TLS on slow-peer
// congestion) costs nothing.
func writeFull(w io.Writer, b []byte) (int, error) {
	total := 0
	for total < len(b) {
		n, err := w.Write(b[total:])
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

// isBenignCloseErr matches "use of closed network connection" and friends
// — the expected outcomes when we close a conn the peer has already
// closed, or the reader/writer raced into double-close. The caller maps
// these to "no error worth reporting".
func isBenignCloseErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, io.EOF) {
		return true
	}
	return false
}
