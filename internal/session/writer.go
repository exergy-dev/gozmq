package session

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
	"github.com/exergy-dev/gozmq/msg"
)

// Batch caps for write coalescing. The writer pulls additional messages
// from the session pipe non-blocking after the first one and emits all
// accumulated frames in one writev-backed syscall via net.Buffers. These
// caps prevent unbounded memory growth and keep latency predictable under
// sustained sender bursts.
const (
	// maxBatchMessages caps the number of messages coalesced into one
	// writev. Picked to balance syscall amortization against worst-case
	// latency if a single syscall stalls.
	maxBatchMessages = 32

	// maxBatchBytes caps the total encoded bytes coalesced into one
	// writev. Primary guard against pathological large-payload bursts
	// pinning megabytes in the writer's buffer list.
	maxBatchBytes = 2 << 20 // 2 MiB
)

// runWriter is the session's writing goroutine and the ONLY goroutine that
// writes to the wire. It first writes the reader's handshake frames (see
// writeHandshake), then runs the pipe->wire traffic phase: it pulls
// *msg.Msg values from the session-side pipe, splits them into ZMTP frames
// with MORE flags applied to all but the last, encodes each body via the
// security mechanism, and writes via the codec. PONG/PING commands from
// the reader and the heartbeat arrive via cmdCh and are written here (see
// writerNext).
//
// Throughput optimization (design §11): after receiving the first
// message, the writer tries a non-blocking Recv for additional queued
// messages. All accumulated frame blobs are written in a single writev
// syscall via net.Buffers — preserving the interactive single-message
// path (no coalescing delay) while amortizing syscall overhead across
// rapid bursts.
//
// The writer waits on s.writerCtx (a child of s.ctx) alongside the pipe.
// Socket.Close's LINGER drain calls Drain, which cancels only writerCtx.
// The writer then drains the rest of the queue (see drainSessionPipe) and
// exits WITHOUT closing the conn — so in-flight writes complete before
// Stop closes the conn.
func (s *Session) runWriter() {
	defer s.exit()
	defer close(s.writerDone)
	if !s.writeHandshake() {
		return
	}

	// Reusable batch buffers; recycled each loop iteration.
	var batch writerBatch

	for {
		m, err := s.writerNext()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// writerCtx was cancelled by the socket-level linger path.
				// Drain whatever is already queued, then exit WITHOUT
				// tearing down the conn. No settle delay is needed: the
				// deferred close of writerDone (Drain's channel) follows
				// drainSessionPipe's final flushBatch, and Socket.CloseCtx
				// waits on it before Stop closes the conn, so the final
				// burst cannot race teardown.
				s.drainSessionPipe()
			} else {
				if !zmqerr.IsShutdown(err) {
					s.cfg.Logger.Debug("gozmq/session: writer recv exit", "err", err)
				}
				s.shutdown()
			}
			return
		}
		if m == nil {
			// writerNext serviced a PING/PONG command; nothing to batch.
			continue
		}
		if err := s.writeBatchFrom(&batch, m); err != nil {
			return
		}
	}
}

// writeHandshake writes the greeting and handshake frames the reader
// queues on hsOut, in order, until the reader closes it. It reports
// whether the session reached its traffic phase: false if the handshake
// failed, or a write did, in which case the conn is closed so the reader
// unblocks and the reader picks the recorded error up.
func (s *Session) writeHandshake() bool {
	failed := false
	for b := range s.hsOut {
		if failed {
			continue // keep draining so the reader never blocks
		}
		if _, err := s.cfg.Conn.Write(b); err != nil {
			s.hsWriteErr.Store(&err)
			_ = s.cfg.Conn.Close()
			failed = true
		}
	}
	return !failed && s.sessionPipe != nil
}

// writerNext blocks until the writer has work: a queued user message
// (returned), a pending PING/PONG from cmdCh (written inline; returns
// nil, nil), writer cancellation, or pipe closure (queued messages first,
// then io.EOF). All traffic-phase wire writes funnel through the writer
// goroutine — see the cmdCh field doc for why the reader/heartbeat
// goroutines never write directly.
//
// cmdCh is polled before every receive: RecvOr prefers queued data over
// its abort channel (cmdWake), so under sustained traffic this check is
// what keeps PING/PONG flowing.
func (s *Session) writerNext() (*msg.Msg, error) {
	select {
	case buf := <-s.cmdCh:
		return nil, s.writeFrame(zmtp.FlagCommand, buf)
	default:
	}
	m, err := s.sessionPipe.RecvOr(s.writerCtx, s.cmdWake)
	if errors.Is(err, pipe.ErrAborted) {
		return nil, nil // a command was queued; the next call writes it
	}
	return m, err
}

// writeBatchFrom encodes first plus any immediately-queued messages
// (bounded by maxBatchMessages/maxBatchBytes) into batch and flushes
// them in one writev. Shared by the runWriter hot loop and the linger
// drain. On error it releases the batch, logs, and triggers a session
// shutdown unless the error is an expected teardown artifact; the
// caller just exits on a non-nil return.
func (s *Session) writeBatchFrom(batch *writerBatch, first *msg.Msg) error {
	batchBytes, err := batch.appendMessage(s, first, 0)
	if err == nil {
		for msgCount := 1; msgCount < maxBatchMessages && batchBytes < maxBatchBytes; msgCount++ {
			next, ok := s.sessionPipe.TryRecv()
			if !ok {
				break
			}
			batchBytes, err = batch.appendMessage(s, next, batchBytes)
			if err != nil {
				break
			}
		}
	}
	if err == nil {
		batch.materializeBuffers()
		// Flush a COPY of the slice header: net.Buffers.WriteTo (and the
		// tcpConn WriteBuffers writev fast path) CONSUME the header they
		// are handed (*v = (*v)[i:] as data drains). Passing &batch.buffers
		// directly would leave it pointing at the advanced, empty tail, so
		// release()'s b.buffers[:0] would discard the backing array and
		// every batch would re-grow it from zero capacity.
		bufs := batch.buffers
		err = s.flushBatch(&bufs)
	}
	batch.release()
	if err != nil {
		if !zmqerr.IsShutdown(err) {
			s.cfg.Logger.Debug("gozmq/session: writer batch exit", "err", err)
			s.shutdown()
		}
		return err
	}
	return nil
}

type bufferWriter interface {
	WriteBuffers(*net.Buffers) (int64, error)
}

// writerBatch coalesces multiple outbound *msg.Msg into one writev. Two
// encode paths append into the same per-batch scratch byte slice, each
// frame recording (off, len, body) into entries:
//
//   - Direct/header-only fast path (NULL/PLAIN security + a codec that
//     implements FrameHeaderAppender): only the wire header goes into
//     scratch; body is the caller's frame, referenced without a copy.
//
//   - Fallback path (CURVE / other mechanisms that rewrite body bytes):
//     the whole encoded [header||body] frame goes into scratch and body
//     is nil.
//
// At flush time materializeBuffers walks entries once, slicing scratch
// and pairing each slice with its body. scratch is reused across batches
// (zero allocations after warm-up); slicing it is safe because all
// appends complete before any subslice is taken.
//
// release is called after every flush (writeBatchFrom). It releases the
// batched lent messages and truncates all per-batch state to zero length while
// preserving capacity, leaving the batch ready for the next fill.
type writerBatch struct {
	buffers net.Buffers

	// scratch must NOT be referenced by net.Buffers until the batch is
	// fully encoded: a later append() may relocate the array.
	scratch []byte
	entries []writerEntry

	// lent are the batched lent messages. Each pipe item carries a
	// reference the writer owns; for a lent message it is released only
	// after the flush, because the direct path hands frame bodies to
	// writev by reference. (Release on a plain message does nothing, so
	// those are not tracked.)
	lent []*msg.Msg
}

// maxRetainedScratch bounds the scratch capacity a batch keeps between
// flushes, so one huge CURVE message does not pin its encoded size in
// the writer for the rest of the session.
const maxRetainedScratch = 2 * maxBatchBytes

// writerEntry records one frame's serialization plan: a [off, off+n)
// slice of scratch, followed by body. The final net.Buffers is
// materialized by walking entries after all encoding completes (scratch
// can no longer grow).
type writerEntry struct {
	off  int
	n    int
	body []byte
}

func (b *writerBatch) release() {
	for _, m := range b.lent {
		m.Release()
	}
	clear(b.lent)
	b.lent = b.lent[:0]
	clear(b.entries) // drop body references so sent frames can be collected
	b.entries = b.entries[:0]
	if cap(b.scratch) > maxRetainedScratch {
		b.scratch = nil
	} else {
		b.scratch = b.scratch[:0]
	}
	clear(b.buffers)
	b.buffers = b.buffers[:0]
}

func (b *writerBatch) appendMessage(s *Session, m *msg.Msg, sizeSoFar int) (int, error) {
	if m.Lent() {
		b.lent = append(b.lent, m)
	}
	frames := m.Frames()
	if len(frames) == 0 {
		return b.appendFrame(s, 0, nil, sizeSoFar)
	}
	last := len(frames) - 1
	for i, body := range frames {
		var flags uint8
		if i < last {
			flags |= zmtp.FlagMore
		}
		var err error
		sizeSoFar, err = b.appendFrame(s, flags, body, sizeSoFar)
		if err != nil {
			return sizeSoFar, err
		}
	}
	return sizeSoFar, nil
}

func (b *writerBatch) appendFrame(s *Session, flags uint8, body []byte, sizeSoFar int) (int, error) {
	off := len(b.scratch)
	if s.directHeader != nil {
		newScratch, hdrLen, err := s.directHeader.AppendFrameHeader(b.scratch, flags, body)
		if err != nil {
			return sizeSoFar, fmt.Errorf("gozmq/session: serialize frame: %w", err)
		}
		b.scratch = newScratch
		b.entries = append(b.entries, writerEntry{off: off, n: hdrLen, body: body})
		return sizeSoFar + hdrLen + len(body), nil
	}
	wireFlags, encoded, err := s.encode(flags, body)
	if err != nil {
		return sizeSoFar, fmt.Errorf("gozmq/session: encode frame: %w", err)
	}
	if err := s.cfg.Codec.WriteFrame(b, wireFlags, encoded); err != nil {
		return sizeSoFar, fmt.Errorf("gozmq/session: serialize frame: %w", err)
	}
	n := len(b.scratch) - off
	b.entries = append(b.entries, writerEntry{off: off, n: n})
	return sizeSoFar + n, nil
}

// Write appends p to scratch. It makes the batch the io.Writer the codec
// serializes fallback-path frames into.
func (b *writerBatch) Write(p []byte) (int, error) {
	b.scratch = append(b.scratch, p...)
	return len(p), nil
}

// materializeBuffers walks entries and appends scratch + body slices into
// b.buffers in order. Must be called after all appendFrame calls for this
// batch — at that point b.scratch's underlying array is stable, so the
// subslices we hand to net.Buffers stay valid through flushBatch.
//
// Empty bodies are omitted from b.buffers to keep writev's iovec count
// minimal (a frame with len(body)==0 is still wire-correct because the
// header encodes length=0 — no body bytes follow).
func (b *writerBatch) materializeBuffers() {
	for _, e := range b.entries {
		b.buffers = append(b.buffers, b.scratch[e.off:e.off+e.n])
		if len(e.body) > 0 {
			b.buffers = append(b.buffers, e.body)
		}
	}
}

// flushBatch writes the accumulated frame blobs to the conn in a
// single writev syscall (where supported by the underlying net.Conn).
// net.Buffers.WriteTo preserves order and detects io.Writer shortcuts
// for *net.TCPConn / *net.UnixConn to use writev(2); for other writers
// it falls back to a Write loop. Either way, ordering is preserved.
func (s *Session) flushBatch(buffers *net.Buffers) error {
	if len(*buffers) == 0 {
		return nil
	}
	var err error
	if bw, ok := s.cfg.Conn.(bufferWriter); ok {
		_, err = bw.WriteBuffers(buffers)
	} else {
		_, err = buffers.WriteTo(s.cfg.Conn)
	}
	if err != nil {
		return fmt.Errorf("gozmq/session: writev: %w", err)
	}
	return nil
}

// drainSessionPipe flushes every remaining queued message via the shared
// batching path. Returns when the queue is exhausted or a write error
// occurs (writeBatchFrom logs and shuts the session down on genuine
// errors; Stop closing the conn when the linger expires is one). Called
// after writerCtx cancellation, which follows engine.Close, so nothing
// more is queued and an empty TryRecv is the true end of the queue.
func (s *Session) drainSessionPipe() {
	var batch writerBatch
	for {
		m, ok := s.sessionPipe.TryRecv()
		if !ok {
			return
		}
		if err := s.writeBatchFrom(&batch, m); err != nil {
			return
		}
	}
}

// writeFrame encodes body via the mechanism, then writes via the codec.
// Used by the writer goroutine for single-frame
// COMMAND writes (heartbeat PING, PONG reply, dequeued from cmdCh) —
// heartbeat-rate traffic where batching/zero-copy is not worth a fast
// path.
func (s *Session) writeFrame(flags uint8, body []byte) error {
	wireFlags, encoded, err := s.encode(flags, body)
	if err != nil {
		return fmt.Errorf("gozmq/session: encode frame: %w", err)
	}
	if err := s.cfg.Codec.WriteFrame(s.cfg.Conn, wireFlags, encoded); err != nil {
		return fmt.Errorf("gozmq/session: write frame: %w", err)
	}
	return nil
}
