package session

import (
	"context"
	"errors"
	"unsafe"

	"github.com/exergy-dev/gozmq/msg"
)

// rawReadSize caps one conn.Read in raw mode. Each read becomes one
// message as it arrived: raw mode never coalesces reads (the STREAM
// contract). 64 KiB keeps a bulk transfer at one syscall per 64 KiB.
const rawReadSize = 64 * 1024

// rawAdoptMin is the read size from which the reader hands its buffer
// itself to the message, uncopied, and takes a fresh one: a read that
// (nearly) filled the buffer wastes at most a tenth of it. A shorter read
// is copied out at its exact size, so a queued small chunk never pins a
// whole read buffer (1-byte reads times the HWM would hold gigabytes).
const rawAdoptMin = rawReadSize - rawReadSize/10

// runRawReader is the reader in raw mode (Config.Raw): no greeting, no
// handshake, no heartbeat. It hands the engine end to OnReady at once,
// then publishes each conn.Read as a single-frame message. The message
// is plain memory either way (see rawAdoptMin), never lent.
func (s *Session) runRawReader() {
	defer s.exit()
	defer s.shutdown()
	if !s.attach() {
		return
	}
	buf := make([]byte, rawReadSize)
	for {
		n, err := s.cfg.Conn.Read(buf)
		if n > 0 {
			var m *msg.Msg
			if n >= rawAdoptMin {
				m = msg.FromBytes(buf[:n:n])
				buf = make([]byte, rawReadSize)
			} else {
				m = msg.FromFramesCopy(buf[:n])
			}
			if s.publishInbound(m) != nil {
				return
			}
			s.flushInbound() // no read loop to flush for us in raw mode
		}
		if err != nil {
			return
		}
	}
}

// runRawWriter is the writer in raw mode: each pipe message is a STREAM
// send, [identity, body...], whose body frames are written verbatim, back
// to back (see appendRawMessage). Like the ZMTP writer it coalesces:
// after the message it blocked for, it takes whatever is already queued
// (under the writer's batch caps) and writes the lot in one write or
// writev, so a lone message still goes out at once and only a backlog is
// batched.
// Drain (writerCtx cancelled) writes what is still queued and exits
// leaving the conn open, as in ZMTP mode.
func (s *Session) runRawWriter() {
	defer s.exit()
	defer close(s.writerDone)
	var batch writerBatch // reused by every flush, as in runWriter
	for {
		m, err := s.sessionPipe.RecvOr(s.writerCtx, nil)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				s.drainSessionPipe(func(m *msg.Msg) bool { return s.writeRawBatchFrom(&batch, m) })
			} else {
				s.shutdown()
			}
			return
		}
		if !s.writeRawBatchFrom(&batch, m) {
			return
		}
	}
}

// writeRawBatchFrom writes first and the messages already queued behind
// it, bounded by the maxBatch* caps, in one flush, and reports whether the
// writer should go on. An empty body is the STREAM half-close: it ends the
// batch, the bodies ahead of it are flushed, then the session shuts down
// (closing the conn) and anything queued after it is dropped. A lent
// message is released only after the flush, since its body may go to the
// wire by reference. A write error shuts the session down too.
func (s *Session) writeRawBatchFrom(batch *writerBatch, first *msg.Msg) bool {
	halfClose := batch.appendRawMessage(first)
	for n := 1; !halfClose && n < maxBatchMessages && batch.wireBytes < maxBatchBytes &&
		batch.copied < maxBatchCopied && len(batch.buffers) < maxBatchIovecs; n++ {
		next, ok := s.sessionPipe.TryRecv()
		if !ok {
			break
		}
		halfClose = batch.appendRawMessage(next)
	}
	// flushBatch consumes the header it is handed; see writeBatchFrom.
	batch.flush = batch.buffers
	err := s.flushBatch(&batch.flush)
	batch.release()
	if err == nil && !halfClose {
		return true
	}
	if err != nil {
		logUnlessShutdown(s.cfg.Logger, "gozmq/session: raw write", err)
	}
	s.shutdown()
	return false
}

// appendRawMessage adds m's body to the batch and reports whether m is
// the half-close, which adds nothing. m is a STREAM send as the engine
// routed it, [identity, body...]: the body is the concatenation of the
// frames after the identity, and a body of zero bytes (or a message with
// no frames at all) is the half-close. The batch owns m's reference from
// here on (see writerBatch.release).
func (b *writerBatch) appendRawMessage(m *msg.Msg) (halfClose bool) {
	if m.Lent() {
		b.lent = append(b.lent, m)
	}
	frames := m.Frames()
	if len(frames) == 0 || m.TotalSize() == len(frames[0]) {
		return true
	}
	for _, f := range frames[1:] {
		if len(f) > 0 {
			b.appendRaw(f)
		}
	}
	return false
}

// appendRaw adds body to the batch with no framing: the identity-mechanism
// branch of appendFrame without the header. A body of up to
// coalesceBodyMax bytes is copied into scratch, where consecutive small
// bodies form one iovec; a larger one goes to the wire by reference. (Raw
// sessions set neither identityMech nor a codec, so this stands alone.)
func (b *writerBatch) appendRaw(body []byte) {
	off := len(b.scratch)
	base := unsafe.SliceData(b.scratch)
	if len(body) <= coalesceBodyMax {
		b.scratch = append(b.scratch, body...)
		b.copied += len(body)
		b.pushRun(off, base)
	} else {
		b.buffers = append(b.buffers, body)
		b.tailRun = false
	}
	b.wireBytes += len(body)
}
