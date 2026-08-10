package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/exergy-dev/gozmq/internal/bufpool"
	"github.com/exergy-dev/gozmq/internal/iface"
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

// runWriter is the pipe->wire traffic-phase goroutine. It pulls *msg.Msg
// values from the session-side pipe, splits them into ZMTP frames with
// MORE flags applied to all but the last, encodes each body via the
// security mechanism, and writes via the codec. It is the ONLY goroutine
// that writes to the wire during traffic phase: PONG/PING commands from
// the reader and heartbeat goroutines arrive via cmdCh and are written
// here (see writerNext).
//
// Throughput optimization (design §11): after receiving the first
// message, the writer tries a non-blocking Recv for additional queued
// messages. All accumulated frame blobs are written in a single writev
// syscall via net.Buffers — preserving the interactive single-message
// path (no coalescing delay) while amortizing syscall overhead across
// rapid bursts.
//
// The writer waits on s.writerCtx (a child of s.ctx) alongside the pipe.
// Socket.Close's LINGER drain calls CancelWriter() which cancels only
// writerCtx. The writer then drains remaining messages non-blocking and
// exits WITHOUT closing the conn — so in-flight writes complete before
// sess.Stop() closes the conn.
func (s *sessionImpl) runWriter() {
	defer s.wg.Done()
	defer s.closeWriterExited()

	// Reusable batch buffers; recycled each loop iteration.
	var batch writerBatch
	recvCh := s.sessionPipe.RecvCh()

	for {
		m, err := s.writerNext(recvCh)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// writerCtx was cancelled by the socket-level linger path.
				// Drain whatever is already queued, then exit WITHOUT
				// tearing down the conn. Socket.Close waits on
				// WriterFlushed, then calls sess.Stop() to close the conn
				// in a coordinated order.
				// No settle delay is needed here: the writer goroutine
				// closes writerExited (via the deferred close) only after
				// drainSessionPipe's final flushBatch has returned, and
				// Socket.CloseCtx blocks on WriterFlushed() == writerExited
				// before calling sess.Stop() (which closes the conn). That
				// wait already serializes "last writev complete" before
				// "conn.Close", so the final burst cannot race teardown.
				s.drainSessionPipe()
			} else {
				if !isShutdownErr(err) {
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

// writerNext blocks until the writer has work: a queued user message
// (returned), a pending PING/PONG from cmdCh (written inline; returns
// nil, nil), writer cancellation, or pipe closure. All traffic-phase
// wire writes funnel through the writer goroutine — see the cmdCh field
// doc for why the reader/heartbeat goroutines never write directly.
func (s *sessionImpl) writerNext(recvCh <-chan *msg.Msg) (*msg.Msg, error) {
	select {
	case buf := <-s.cmdCh:
		if err := s.writeFrame(iface.FlagCommand, buf); err != nil {
			return nil, err
		}
		return nil, nil
	case m := <-recvCh:
		return m, nil
	case <-s.sessionPipe.Closed():
		// Mirror pipe.end.Recv's closed-path semantics: hand out any
		// message already buffered before reporting EOF.
		if m, ok := s.sessionPipe.TryRecv(); ok {
			return m, nil
		}
		return nil, io.EOF
	case <-s.writerCtx.Done():
		return nil, s.writerCtx.Err()
	}
}

// writeBatchFrom encodes first plus any immediately-queued messages
// (bounded by maxBatchMessages/maxBatchBytes) into batch and flushes
// them in one writev. Shared by the runWriter hot loop and the linger
// drain. On error it releases the batch, logs, and triggers a session
// shutdown unless the error is an expected teardown artifact; the
// caller just exits on a non-nil return.
func (s *sessionImpl) writeBatchFrom(batch *writerBatch, first *msg.Msg) error {
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
		if !isShutdownErr(err) {
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
//     drawn from bufpool.Get. The blob is owned by the batch (returned to
//     the pool on release) and appended directly to b.buffers.
//
// release is called after every flush (writeBatchFrom). It recycles any
// pool-backed fallback blobs, recycles the batched source messages, and
// clears all per-batch state to zero length while preserving the
// underlying arrays' capacity, leaving the batch ready for the next fill.
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

	// batched holds the source *msg.Msg values whose frame bodies are
	// aliased into this batch by the direct/header fast path
	// (materializeBuffers records body slices into net.Buffers by
	// reference, no copy). They MUST NOT be recycled until after
	// flushBatch's writev has fully consumed the buffers — otherwise
	// Recycle would bufpool.Put a pooled inbound frame's backing buffer while
	// the pending write still aliases it (use-after-free / wire
	// corruption for a re-sent pooled message). release() recycles them
	// after the flush. Non-pooled messages are tracked too; Recycle is a
	// no-op for them.
	batched []*msg.Msg
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

func (b *writerBatch) release() {
	for _, owned := range b.ownedBuffers {
		bufpool.Put(owned)
	}
	b.ownedBuffers = b.ownedBuffers[:0]
	// Recycle source messages only now — after flushBatch has consumed
	// the (possibly aliased) bodies. See the batched field doc.
	for _, m := range b.batched {
		msg.Recycle(m)
	}
	clear(b.batched)
	b.batched = b.batched[:0]
	b.entries = b.entries[:0]
	b.headerScratch = b.headerScratch[:0]
	b.buffers = b.buffers[:0]
}

func (b *writerBatch) appendMessage(s *sessionImpl, m *msg.Msg, sizeSoFar int) (int, error) {
	// Defer Recycle(m) to release() (post-flush) for POOLED messages only:
	// the direct path aliases m's frame bodies into net.Buffers, so recycling
	// a pooled buffer now would free it while the pending writev still
	// references it. Non-pooled messages need no Recycle at all (it is a
	// no-op for them) and their heap frames stay alive via the batch's slice
	// references until flush, so we skip tracking them — keeping the common
	// case allocation- and bookkeeping-free.
	if m.IsPooled() {
		b.batched = append(b.batched, m)
	}
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
	blob := bufpool.Get(len(scratch.Bytes()))
	copy(blob, scratch.Bytes())
	return blob, len(blob), nil
}

// flushBatch writes the accumulated frame blobs to the conn in a
// single writev syscall (where supported by the underlying net.Conn).
// The writeMu is held for the duration (see the writeMu field doc; with
// PONG/PING routed through cmdCh it is uncontended in practice).
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

// drainSessionPipe pulls any remaining messages from sessionPipe
// non-blocking and flushes them via the shared batching path. Returns
// when the pipe is empty or a write error occurs (writeBatchFrom logs
// and shuts the session down on genuine errors). Called after writerCtx
// cancellation to flush the outbound queue before the writer goroutine
// exits.
func (s *sessionImpl) drainSessionPipe() {
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

// writeFrame encodes body via the mechanism, then writes via the codec
// under the writeMu. Used by the writer goroutine for single-frame
// COMMAND writes (heartbeat PING, PONG reply, dequeued from cmdCh) —
// heartbeat-rate traffic where batching/zero-copy is not worth a fast
// path.
func (s *sessionImpl) writeFrame(flags uint8, body []byte) error {
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
