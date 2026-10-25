package session

import (
	"context"
	"errors"
	"fmt"
	"net"
	"unsafe"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
	"github.com/exergy-dev/gozmq/msg"
)

// Batch caps for write coalescing. The writer pulls additional messages
// from the session pipe non-blocking after the first one and emits all
// accumulated frames in one flush. The caps are checked BEFORE each pull,
// so one message may overshoot a cap by its own size. They prevent
// unbounded memory growth and keep latency predictable under sustained
// sender bursts.
const (
	// coalesceBodyMax is the largest body (bytes) an identity-mechanism
	// frame copies into scratch right after its header, so that header,
	// body and the following small frames form one contiguous run (one
	// iovec, one write). Larger bodies go to the wire by reference: the
	// copy would cost more than the extra iovec saves.
	coalesceBodyMax = 1024

	// maxBatchCopied caps the bytes the batch copies into scratch (headers
	// and small bodies). Large sealed CURVE bodies are not counted: they
	// are one memmove each and bounded by maxBatchBytes.
	maxBatchCopied = 64 << 10

	// maxBatchIovecs stops pulling once the batch holds this many buffers,
	// below IOV_MAX (1024) so one writev takes the whole batch.
	maxBatchIovecs = 1000

	// maxBatchMessages is a sanity cap for pathological tiny or empty
	// messages; it also bounds the lent slice.
	maxBatchMessages = 4096

	// maxBatchBytes caps the total wire bytes in one batch, including
	// bodies sent by reference. Primary guard against pathological
	// large-payload bursts pinning megabytes in the writer.
	maxBatchBytes = 2 << 20 // 2 MiB
)

// runWriter is the session's writing goroutine and the ONLY goroutine that
// writes to the wire. It first writes the reader's handshake frames (see
// writeHandshake), then runs the pipe->wire traffic phase: it pulls
// *msg.Msg values from the session-side pipe, splits them into ZMTP frames
// with MORE flags applied to all but the last, encodes each body via the
// security mechanism, and writes them. PONG/PING commands from the reader
// and the heartbeat arrive via cmdCh and go out the same way (see
// writerNext).
//
// Throughput optimization: after receiving the first message, the writer
// tries a non-blocking Recv for additional queued messages. All accumulated
// frames are written in a single writev syscall via net.Buffers — preserving
// the interactive single-message path (no coalescing delay) while
// amortizing syscall overhead across rapid bursts.
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

// writerNext blocks until the writer has work: a queued user message, a
// PING/PONG queued on cmdCh (cmdWake aborts the receive; m is then nil and
// writeBatchFrom finds the command), writer cancellation, or pipe closure
// (queued messages first, then io.EOF). All traffic-phase wire writes
// funnel through the writer goroutine — see the cmdCh field doc for why
// the reader/heartbeat goroutines never write directly.
func (s *Session) writerNext() (*msg.Msg, error) {
	m, err := s.sessionPipe.RecvOr(s.writerCtx, s.cmdWake)
	if errors.Is(err, pipe.ErrAborted) {
		return nil, nil
	}
	return m, err
}

// writeBatchFrom encodes the commands queued on cmdCh, then first (when
// not nil) plus any immediately-queued messages (bounded by
// the maxBatch* caps) into batch and flushes them in one
// writev. Commands go first so a PONG never waits behind a data burst and
// a command queued before a message precedes it on the wire. Shared by the
// runWriter hot loop and the linger drain. On error it releases the batch,
// logs, and triggers a session shutdown unless the error is an expected
// teardown artifact; the caller just exits on a non-nil return.
func (s *Session) writeBatchFrom(batch *writerBatch, first *msg.Msg) error {
	var batchBytes int
	var err error
	for err == nil {
		select {
		case c := <-s.cmdCh:
			batchBytes, err = batch.appendFrame(s, zmtp.FlagCommand, c, batchBytes)
			continue
		default:
		}
		break
	}
	if err == nil && first != nil {
		batchBytes, err = batch.appendMessage(s, first, batchBytes)
	}
	if err == nil {
		for n := 1; n < maxBatchMessages && batchBytes < maxBatchBytes &&
			batch.copied < maxBatchCopied && len(batch.buffers) < maxBatchIovecs; n++ {
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
		// writev consumes the slice header it is handed (*v = (*v)[i:] as
		// data drains), so it gets a copy: buffers keeps the backing
		// array for the next batch.
		batch.flush = batch.buffers
		err = s.flushBatch(&batch.flush)
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

// writerBatch coalesces outbound frames into one writev. Every frame
// contributes its wire header, appended to scratch, and its body: the
// caller's frame itself for an identity mechanism (or one whose
// EncodeFrame returns fresh memory, as the Mech contract requires), or
// bytes a FrameAppender mechanism sealed into scratch. Subslices of scratch
// go into buffers as they are made; a later append that relocates scratch
// leaves them valid, since the old array is never written again.
//
// release is called after every flush. It releases the batched lent
// messages and truncates the per-batch state to zero length while keeping
// the capacity, so a warm batch allocates nothing.
type writerBatch struct {
	buffers net.Buffers
	flush   net.Buffers // the header writev consumes; see writeBatchFrom
	scratch []byte

	// lent are the batched lent messages. Each pipe item carries a
	// reference the writer owns; for a lent message it is released only
	// after the flush, because frame bodies go to writev by reference.
	// (Release on a plain message does nothing, so those are not tracked.)
	lent []*msg.Msg

	// copied counts the bytes this batch copied into scratch (headers and
	// small bodies); it bounds the batch together with maxBatchCopied.
	copied int

	// tailRun is set when buffers[len-1] is exactly
	// scratch[tailOff:len(scratch)] of the CURRENT backing array, so the
	// next frame's scratch bytes can be folded into that entry instead of
	// opening a new iovec. Invariants (see pushRun): wire order is buffers
	// order; only the LAST entry is ever extended, and only beyond its
	// current end; a scratch array that append relocated is never written
	// again, which keeps entries pointing into it valid.
	tailRun bool
	tailOff int
}

// maxRetainedScratch bounds the scratch capacity a batch keeps between
// flushes, so one huge CURVE message does not pin its encoded size in
// the writer for the rest of the session.
const maxRetainedScratch = 2 * maxBatchBytes

func (b *writerBatch) release() {
	for _, m := range b.lent {
		m.Release()
	}
	clear(b.lent)
	b.lent = b.lent[:0]
	if cap(b.scratch) > maxRetainedScratch {
		b.scratch = nil
	} else {
		b.scratch = b.scratch[:0]
	}
	b.copied = 0
	b.tailRun, b.tailOff = false, 0
	clear(b.buffers) // drop body references so sent frames can be collected
	b.buffers = b.buffers[:0]
	b.flush = nil
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

// pushRun publishes scratch[off:len(scratch)], the bytes this frame
// appended. base is unsafe.SliceData(b.scratch) captured by the caller
// BEFORE any append for this frame. The last buffers entry is extended
// only if it is the tail run (it ends at off) AND scratch was not
// relocated (base unchanged); otherwise a new entry starts here. Every
// path that appends a non-scratch entry (a body by reference) or a
// non-contiguous pair clears tailRun right after.
func (b *writerBatch) pushRun(off int, base *byte) {
	end := len(b.scratch)
	if end == off {
		return
	}
	if b.tailRun && len(b.buffers) > 0 && unsafe.SliceData(b.scratch) == base {
		b.buffers[len(b.buffers)-1] = b.scratch[b.tailOff:end:end]
		return
	}
	b.tailRun, b.tailOff = true, off
	b.buffers = append(b.buffers, b.scratch[off:end:end])
}

// appendFrame adds one frame to the batch and returns sizeSoFar plus the
// frame's wire size. Small identity bodies are copied behind their header
// so consecutive small frames collapse into one iovec (the wire bytes are
// the same, only the iovec layout differs); large bodies and bodies from a
// mechanism that returns fresh memory go to the wire by reference.
func (b *writerBatch) appendFrame(s *Session, flags uint8, body []byte, sizeSoFar int) (int, error) {
	off := len(b.scratch)
	base := unsafe.SliceData(b.scratch)
	var err error
	switch {
	case s.identityMech:
		if b.scratch, err = s.cfg.Codec.AppendFrameHeader(b.scratch, flags, len(body)); err != nil {
			return sizeSoFar, fmt.Errorf("gozmq/session: serialize frame: %w", err)
		}
		hdrLen := len(b.scratch) - off
		if len(body) <= coalesceBodyMax {
			b.scratch = append(b.scratch, body...)
			b.copied += len(b.scratch) - off
			b.pushRun(off, base)
		} else {
			b.copied += hdrLen
			b.pushRun(off, base)
			b.buffers = append(b.buffers, body)
			b.tailRun = false
		}
		return sizeSoFar + hdrLen + len(body), nil
	case s.appendEncode != nil:
		return b.appendSealed(s, flags, body, sizeSoFar)
	}
	wireFlags, wire, err := s.encode(flags, body)
	if err != nil {
		return sizeSoFar, fmt.Errorf("gozmq/session: encode frame: %w", err)
	}
	if b.scratch, err = s.cfg.Codec.AppendFrameHeader(b.scratch, wireFlags, len(wire)); err != nil {
		return sizeSoFar, fmt.Errorf("gozmq/session: serialize frame: %w", err)
	}
	hdrLen := len(b.scratch) - off
	b.copied += hdrLen
	b.pushRun(off, base)
	if len(wire) > 0 {
		// Empty bodies are left out to keep writev's iovec count minimal;
		// the header already says length 0.
		b.buffers = append(b.buffers, wire)
		b.tailRun = false
	}
	return sizeSoFar + hdrLen + len(wire), nil
}

// appendSealed adds a frame whose mechanism seals the body into scratch
// (security.FrameAppender): the body lands at scratch[off:bodyEnd), its
// header after it, and the two go to buffers in wire order (header first).
func (b *writerBatch) appendSealed(s *Session, flags uint8, body []byte, sizeSoFar int) (int, error) {
	off := len(b.scratch)
	var err error
	var wireFlags uint8
	if b.scratch, wireFlags, err = s.appendEncode(b.scratch, flags, body); err != nil {
		return sizeSoFar, fmt.Errorf("gozmq/session: encode frame: %w", err)
	}
	bodyEnd := len(b.scratch)
	n := bodyEnd - off
	if b.scratch, err = s.cfg.Codec.AppendFrameHeader(b.scratch, wireFlags, n); err != nil {
		return sizeSoFar, fmt.Errorf("gozmq/session: serialize frame: %w", err)
	}
	end := len(b.scratch)
	hdr := b.scratch[bodyEnd:end:end]
	b.copied += len(hdr)
	b.buffers = append(b.buffers, hdr)
	if n > 0 {
		b.buffers = append(b.buffers, b.scratch[off:bodyEnd:bodyEnd])
	}
	b.tailRun = false
	return sizeSoFar + len(hdr) + n, nil
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
