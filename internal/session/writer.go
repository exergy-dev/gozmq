package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
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

	// writerDrainSettleTime keeps the conn open briefly after a
	// CancelWriter-driven drain completes. Socket.Close currently waits on
	// WriterFlushed, then hard-stops the session; without a small settle
	// window the final burst can race directly into conn.Close on fast
	// teardown paths.
	writerDrainSettleTime = 10 * time.Millisecond
)

// runWriter is the pipe->wire traffic-phase goroutine. It pulls *msg.Msg
// values from the session-side pipe, splits them into ZMTP frames with
// MORE flags applied to all but the last, encodes each body via the
// security mechanism, and writes via the codec.
//
// Throughput optimization (design §11): after receiving the first
// message via a blocking Recv, the writer tries a non-blocking Recv for
// additional queued messages. All accumulated frame blobs are written
// in a single writev syscall via net.Buffers — preserving the
// interactive single-message path (no coalescing delay) while
// amortizing syscall overhead across rapid bursts.
//
// The writer uses s.writerCtx (a child of s.ctx) for its blocking Recv.
// Socket.Close's LINGER drain calls CancelWriter() which cancels only
// writerCtx. The writer then drains remaining messages non-blocking and
// exits WITHOUT closing the conn — so in-flight writes complete before
// sess.Stop() closes the conn.
func (s *sessionImpl) runWriter() {
	defer s.wg.Done()
	defer close(s.writerExited)

	// Reusable batch buffers; recycled each loop iteration.
	var batch writerBatch

	for {
		m, err := s.sessionPipe.Recv(s.writerCtx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// writerCtx was cancelled by the socket-level linger path.
				// Drain whatever is already queued, then exit WITHOUT
				// tearing down the conn. Socket.Close waits on
				// WriterFlushed, then calls sess.Stop() to close the conn
				// in a coordinated order.
				if drainErr := s.drainSessionPipe(); drainErr != nil && !isWriterShutdownErr(drainErr) {
					s.cfg.Logger.Debug("gozmq/session: writer drain exit", "err", drainErr)
					s.shutdown()
				} else if writerDrainSettleTime > 0 {
					timer := time.NewTimer(writerDrainSettleTime)
					select {
					case <-timer.C:
					case <-s.ctx.Done():
					}
					timer.Stop()
				}
			} else {
				if !isWriterShutdownErr(err) {
					s.cfg.Logger.Debug("gozmq/session: writer recv exit", "err", err)
				}
				s.shutdown()
			}
			return
		}

		// Encode the first message's frames into the batch.
		batch.reset()
		batchBytes := 0
		msgCount := 1

		var encErr error
		batchBytes, encErr = batch.appendMessage(s, m, batchBytes)
		msg.Recycle(m)
		if encErr != nil {
			batch.release()
			if !isWriterShutdownErr(encErr) {
				s.cfg.Logger.Debug("gozmq/session: writer encode exit", "err", encErr)
				s.shutdown()
			}
			return
		}

		for msgCount < maxBatchMessages && batchBytes < maxBatchBytes {
			next, ok := recvNonBlocking(s.sessionPipe)
			if !ok {
				break
			}
			batchBytes, encErr = batch.appendMessage(s, next, batchBytes)
			msg.Recycle(next)
			if encErr != nil {
				batch.release()
				if !isWriterShutdownErr(encErr) {
					s.cfg.Logger.Debug("gozmq/session: writer encode exit", "err", encErr)
					s.shutdown()
				}
				return
			}
			msgCount++
		}

		batch.materializeBuffers()
		if err := s.flushBatch(&batch.buffers); err != nil {
			batch.release()
			if !isWriterShutdownErr(err) {
				s.cfg.Logger.Debug("gozmq/session: writer send exit", "err", err)
				s.shutdown()
			}
			return
		}
		batch.release()
	}
}

// cancelledCtx is a single pre-cancelled context reused by
// recvNonBlocking. Pipe.Recv has a fast path that returns any buffered
// message without consulting ctx, so a shared cancelled ctx is safe
// for concurrent callers and saves an allocation per poll.
var cancelledCtx = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

type bufferWriter interface {
	WriteBuffers(*net.Buffers) (int64, error)
}

type nonBlockingPipe interface {
	TryRecv() (*msg.Msg, bool)
}

// recvNonBlocking attempts to pull one more message from the pipe
// without blocking. Returns (nil, false) when the pipe is empty or
// closed. The pre-cancelled context makes Pipe.Recv fall through to
// the fast path: it returns the buffered message if any, otherwise
// ctx.Err().
func recvNonBlocking(p iface.Pipe) (*msg.Msg, bool) {
	if nb, ok := p.(nonBlockingPipe); ok {
		return nb.TryRecv()
	}
	m, err := p.Recv(cancelledCtx)
	if err != nil {
		return nil, false
	}
	return m, true
}

// writerBatch coalesces multiple outbound *msg.Msg into one writev. Two
// encode paths share the same flush:
//
//   - Direct/header-only fast path (NULL/PLAIN security + a codec that
//     implements FrameHeaderAppender): wire headers are appended into a
//     single, growing per-batch headerScratch byte slice. Each frame
//     records (headerOffset, headerLen, body) into entries. At flush time,
//     materializeBuffers walks entries once, slicing headerScratch into
//     header sub-buffers and pairing each with its body. headerScratch is
//     reused across batches (zero allocations after warm-up); aliasing is
//     safe because all appends complete before any subslice is taken.
//
//   - Fallback path (CURVE / other mechanisms that rewrite body bytes):
//     encodeOneFrame produces a contiguous [header||body] blob per frame
//     drawn from msg.PoolGet. The blob is owned by the batch (returned to
//     the pool on release) and appended directly to b.buffers.
//
// Reset is called between iterations of runWriter. release recycles any
// pool-backed fallback blobs and clears headerScratch + entries to their
// zero length while preserving the underlying arrays' capacity.
type writerBatch struct {
	buffers      net.Buffers
	scratch      bytes.Buffer
	ownedBuffers [][]byte

	// headerScratch and entries back the header-only direct fast path.
	// headerScratch must NOT be referenced by net.Buffers until the
	// batch is fully encoded — slicing into it before that point would
	// be invalidated by a later append() relocating the array.
	headerScratch []byte
	entries       []writerEntry
}

// writerEntry records one frame's serialization plan: a [headerOff,
// headerOff+headerLen) slice of headerScratch, followed by body. The
// final net.Buffers is materialized by walking entries after all encoding
// completes (headerScratch can no longer grow).
type writerEntry struct {
	headerOff int
	headerLen int
	body      []byte
}

func (b *writerBatch) reset() {
	b.release()
	b.buffers = b.buffers[:0]
	b.headerScratch = b.headerScratch[:0]
	b.entries = b.entries[:0]
}

func (b *writerBatch) release() {
	for _, owned := range b.ownedBuffers {
		msg.PoolPut(owned)
	}
	b.ownedBuffers = b.ownedBuffers[:0]
	b.entries = b.entries[:0]
	b.headerScratch = b.headerScratch[:0]
}

func (b *writerBatch) appendMessage(s *sessionImpl, m *msg.Msg, sizeSoFar int) (int, error) {
	frames := m.Frames()
	if len(frames) == 0 {
		return b.appendFrame(s, 0, nil, sizeSoFar)
	}
	last := len(frames) - 1
	for i, body := range frames {
		var flags uint8
		if i < last {
			flags |= iface.FlagMore
		}
		var err error
		sizeSoFar, err = b.appendFrame(s, flags, body, sizeSoFar)
		if err != nil {
			return sizeSoFar, err
		}
	}
	return sizeSoFar, nil
}

func (b *writerBatch) appendFrame(s *sessionImpl, flags uint8, body []byte, sizeSoFar int) (int, error) {
	if s.directHeader != nil {
		off := len(b.headerScratch)
		newScratch, hdrLen, err := s.directHeader.AppendFrameHeader(b.headerScratch, flags, body)
		if err != nil {
			return sizeSoFar, fmt.Errorf("gozmq/session: serialize frame: %w", err)
		}
		b.headerScratch = newScratch
		b.entries = append(b.entries, writerEntry{
			headerOff: off,
			headerLen: hdrLen,
			body:      body,
		})
		return sizeSoFar + hdrLen + len(body), nil
	}
	blob, sz, err := encodeOneFrame(s.encode, s.cfg.Codec, &b.scratch, flags, body)
	if err != nil {
		return sizeSoFar, err
	}
	b.buffers = append(b.buffers, blob)
	b.ownedBuffers = append(b.ownedBuffers, blob)
	return sizeSoFar + sz, nil
}

// materializeBuffers walks entries and appends header + body slices into
// b.buffers in order. Must be called after all appendFrame calls for this
// batch — at that point b.headerScratch's underlying array is stable, so
// the subslices we hand to net.Buffers stay valid through flushBatch.
//
// Empty bodies are omitted from b.buffers to keep writev's iovec count
// minimal (a frame with len(body)==0 is still wire-correct because the
// header encodes length=0 — no body bytes follow).
func (b *writerBatch) materializeBuffers() {
	if len(b.entries) == 0 {
		return
	}
	scratch := b.headerScratch
	for _, e := range b.entries {
		b.buffers = append(b.buffers, scratch[e.headerOff:e.headerOff+e.headerLen])
		if len(e.body) > 0 {
			b.buffers = append(b.buffers, e.body)
		}
	}
}

// encodeOneFrame applies the mechanism's body/flag rewrite, then the codec
// WriteFrame into scratch, returning a freshly-allocated []byte of the full
// header+body serialization.
func encodeOneFrame(
	encode frameEncoder,
	codec iface.FrameCodec,
	scratch *bytes.Buffer,
	flags uint8,
	body []byte,
) ([]byte, int, error) {
	wireFlags, encoded, err := encode(flags, body)
	if err != nil {
		return nil, 0, fmt.Errorf("gozmq/session: encode frame: %w", err)
	}
	scratch.Reset()
	if err := codec.WriteFrame(scratch, wireFlags, encoded); err != nil {
		return nil, 0, fmt.Errorf("gozmq/session: serialize frame: %w", err)
	}
	// Copy out so scratch can be reused for the next frame.
	blob := msg.PoolGet(len(scratch.Bytes()))
	copy(blob, scratch.Bytes())
	return blob, len(blob), nil
}

// flushBatch writes the accumulated frame blobs to the conn in a
// single writev syscall (where supported by the underlying net.Conn).
// The writeMu is held for the duration so PONG/PING replies from the
// reader/heartbeat don't interleave with a multipart user message.
//
// net.Buffers.WriteTo preserves order and detects io.Writer shortcuts
// for *net.TCPConn / *net.UnixConn to use writev(2); for other writers
// it falls back to a Write loop. Either way, ordering is preserved.
func (s *sessionImpl) flushBatch(buffers *net.Buffers) error {
	if len(*buffers) == 0 {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var (
		n   int64
		err error
	)
	if bw, ok := s.cfg.Conn.(bufferWriter); ok {
		n, err = bw.WriteBuffers(buffers)
	} else {
		n, err = buffers.WriteTo(s.cfg.Conn)
	}
	if err != nil {
		return fmt.Errorf("gozmq/session: writev: %w", err)
	}
	_ = n
	return nil
}

// drainSessionPipe pulls any remaining messages from sessionPipe using a
// pre-cancelled context (non-blocking). Returns when the channel is empty
// or a write error occurs. Called after writerCtx cancellation to flush
// the outbound queue before the writer goroutine exits.
//
// Uses the batching path so large queued bursts drain in as few
// syscalls as possible without violating the bounded-batch invariants.
func (s *sessionImpl) drainSessionPipe() error {
	var batch writerBatch
	for {
		m, ok := recvNonBlocking(s.sessionPipe)
		if !ok {
			return nil
		}

		batch.reset()
		batchBytes := 0
		msgCount := 1

		var encErr error
		batchBytes, encErr = batch.appendMessage(s, m, batchBytes)
		if encErr != nil {
			batch.release()
			return encErr
		}
		for msgCount < maxBatchMessages && batchBytes < maxBatchBytes {
			next, more := recvNonBlocking(s.sessionPipe)
			if !more {
				break
			}
			batchBytes, encErr = batch.appendMessage(s, next, batchBytes)
			if encErr != nil {
				batch.release()
				return encErr
			}
			msgCount++
		}
		batch.materializeBuffers()
		if err := s.flushBatch(&batch.buffers); err != nil {
			batch.release()
			return err
		}
		batch.release()
	}
}

// writeFrame encodes body via the mechanism, then writes via the codec
// under the writeMu so PONGs from the reader don't interleave with a
// multipart user message. Used for single-frame COMMAND writes
// (heartbeat PING, PONG reply) where batching is not applicable.
func (s *sessionImpl) writeFrame(flags uint8, body []byte) error {
	if s.direct != nil {
		var buffers net.Buffers
		release, _, err := s.direct.AppendFrameBuffers(&buffers, flags, body)
		if err != nil {
			return fmt.Errorf("gozmq/session: serialize frame: %w", err)
		}
		defer func() {
			if release != nil {
				release()
			}
		}()
		if err := s.flushBatch(&buffers); err != nil {
			return fmt.Errorf("gozmq/session: write frame: %w", err)
		}
		return nil
	}
	wireFlags, encoded, err := s.encode(flags, body)
	if err != nil {
		return fmt.Errorf("gozmq/session: encode frame: %w", err)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.cfg.Codec.WriteFrame(s.cfg.Conn, wireFlags, encoded); err != nil {
		return fmt.Errorf("gozmq/session: write frame: %w", err)
	}
	return nil
}

// isWriterShutdownErr identifies writer exits we don't want to log as
// failures: context cancellation, pipe closed by either end, EOF.
func isWriterShutdownErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	if err.Error() == "use of closed network connection" {
		return true
	}
	return false
}
