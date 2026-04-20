package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"

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
	maxBatchBytes = 1 << 20 // 1 MiB
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
	defer s.shutdown()
	defer close(s.writerExited)

	// Reusable batch buffers; recycled each loop iteration.
	var (
		buffers  net.Buffers
		scratch  bytes.Buffer
		frameBuf [][]byte // backing array reused across iterations
	)

	for {
		m, err := s.sessionPipe.Recv(s.writerCtx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// writerCtx was cancelled (LINGER drain signal). Drain
				// remaining buffered messages non-blocking before exit.
				s.drainSessionPipe()
			} else if !isWriterShutdownErr(err) {
				s.cfg.Logger.Debug("gozmq/session: writer recv exit", "err", err)
			}
			return
		}

		// Encode the first message's frames into the batch.
		buffers = buffers[:0]
		frameBuf = frameBuf[:0]
		batchBytes := 0
		msgCount := 1

		var encErr error
		frameBuf, batchBytes, encErr = encodeMessageFrames(s.cfg.Mech, s.cfg.Codec, &scratch, m, frameBuf, batchBytes)
		if encErr != nil {
			if !isWriterShutdownErr(encErr) {
				s.cfg.Logger.Debug("gozmq/session: writer encode exit", "err", encErr)
			}
			return
		}

		// Opportunistically coalesce additional queued messages so a
		// rapid burst becomes one writev. Stop at the cap or when the
		// pipe has nothing immediately available — preserves
		// backpressure (never drains ahead of writes beyond the cap).
		for msgCount < maxBatchMessages && batchBytes < maxBatchBytes {
			next, ok := recvNonBlocking(s.sessionPipe)
			if !ok {
				break
			}
			frameBuf, batchBytes, encErr = encodeMessageFrames(s.cfg.Mech, s.cfg.Codec, &scratch, next, frameBuf, batchBytes)
			if encErr != nil {
				if !isWriterShutdownErr(encErr) {
					s.cfg.Logger.Debug("gozmq/session: writer encode exit", "err", encErr)
				}
				return
			}
			msgCount++
		}

		// net.Buffers consumes its backing slice as it writes; borrow
		// frameBuf's contents but via a separate header so the reset
		// above keeps capacity for the next iteration.
		buffers = append(buffers[:0], frameBuf...)
		if err := s.flushBatch(&buffers); err != nil {
			if !isWriterShutdownErr(err) {
				s.cfg.Logger.Debug("gozmq/session: writer send exit", "err", err)
			}
			return
		}
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

// recvNonBlocking attempts to pull one more message from the pipe
// without blocking. Returns (nil, false) when the pipe is empty or
// closed. The pre-cancelled context makes Pipe.Recv fall through to
// the fast path: it returns the buffered message if any, otherwise
// ctx.Err().
func recvNonBlocking(p iface.Pipe) (*msg.Msg, bool) {
	m, err := p.Recv(cancelledCtx)
	if err != nil {
		return nil, false
	}
	return m, true
}

// encodeMessageFrames serializes every frame of m into individual
// []byte blobs appended to out. Each blob is header+body ready for
// the wire. Returns the updated slice, the new cumulative byte count,
// and any error from the security mechanism's EncodeFrame.
//
// The security mechanism encodes each frame independently (per-frame
// crypto wrapping in CURVE; identity for NULL/PLAIN), so batching is
// purely a syscall-level optimization and does not cross the crypto
// boundary.
//
// scratch is a shared bytes.Buffer reused across frames; its contents
// are copied out per-frame into a fresh []byte (so subsequent frames
// don't invalidate earlier slices).
func encodeMessageFrames(
	mech iface.SecurityMech,
	codec iface.FrameCodec,
	scratch *bytes.Buffer,
	m *msg.Msg,
	out [][]byte,
	sizeSoFar int,
) ([][]byte, int, error) {
	frames := m.Frames()
	if len(frames) == 0 {
		// Empty message: still emit one zero-length non-MORE frame so
		// the peer sees a logical message boundary.
		blob, sz, err := encodeOneFrame(mech, codec, scratch, 0, nil)
		if err != nil {
			return out, sizeSoFar, err
		}
		return append(out, blob), sizeSoFar + sz, nil
	}
	last := len(frames) - 1
	for i, body := range frames {
		var flags uint8
		if i < last {
			flags |= iface.FlagMore
		}
		blob, sz, err := encodeOneFrame(mech, codec, scratch, flags, body)
		if err != nil {
			return out, sizeSoFar, err
		}
		out = append(out, blob)
		sizeSoFar += sz
	}
	return out, sizeSoFar, nil
}

// encodeOneFrame applies the mechanism's body/flag rewrite, then the codec
// WriteFrame into scratch, returning a freshly-allocated []byte of the full
// header+body serialization.
func encodeOneFrame(
	mech iface.SecurityMech,
	codec iface.FrameCodec,
	scratch *bytes.Buffer,
	flags uint8,
	body []byte,
) ([]byte, int, error) {
	wireFlags, encoded, err := mech.EncodeFrame(flags, body)
	if err != nil {
		return nil, 0, fmt.Errorf("gozmq/session: encode frame: %w", err)
	}
	scratch.Reset()
	if err := codec.WriteFrame(scratch, wireFlags, encoded); err != nil {
		return nil, 0, fmt.Errorf("gozmq/session: serialize frame: %w", err)
	}
	// Copy out so scratch can be reused for the next frame.
	blob := append([]byte(nil), scratch.Bytes()...)
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
	_, err := buffers.WriteTo(s.cfg.Conn)
	if err != nil {
		return fmt.Errorf("gozmq/session: writev: %w", err)
	}
	return nil
}

// drainSessionPipe pulls any remaining messages from sessionPipe using a
// pre-cancelled context (non-blocking). Returns when the channel is empty
// or a write error occurs. Called after writerCtx cancellation to flush
// the outbound queue before the writer goroutine exits.
//
// Uses the batching path so large queued bursts drain in as few
// syscalls as possible without violating the bounded-batch invariants.
func (s *sessionImpl) drainSessionPipe() {
	var (
		buffers  net.Buffers
		scratch  bytes.Buffer
		frameBuf [][]byte
	)
	for {
		m, ok := recvNonBlocking(s.sessionPipe)
		if !ok {
			return
		}

		frameBuf = frameBuf[:0]
		batchBytes := 0
		msgCount := 1

		var encErr error
		frameBuf, batchBytes, encErr = encodeMessageFrames(s.cfg.Mech, s.cfg.Codec, &scratch, m, frameBuf, batchBytes)
		if encErr != nil {
			return
		}
		for msgCount < maxBatchMessages && batchBytes < maxBatchBytes {
			next, more := recvNonBlocking(s.sessionPipe)
			if !more {
				break
			}
			frameBuf, batchBytes, encErr = encodeMessageFrames(s.cfg.Mech, s.cfg.Codec, &scratch, next, frameBuf, batchBytes)
			if encErr != nil {
				return
			}
			msgCount++
		}
		buffers = append(buffers[:0], frameBuf...)
		if err := s.flushBatch(&buffers); err != nil {
			return
		}
	}
}

// writeMessage emits a *msg.Msg as a sequence of ZMTP frames in a single
// batched writev. Used by legacy single-message callers (if any remain)
// and as the per-message building block in the batched writer loop.
//
// Retained for API parity with prior revisions; the writer loop above
// inlines the encode+batch path directly for zero-allocation reuse of
// the frame-blob slice.
func (s *sessionImpl) writeMessage(m *msg.Msg) error {
	var (
		scratch  bytes.Buffer
		frameBuf [][]byte
	)
	var err error
	frameBuf, _, err = encodeMessageFrames(s.cfg.Mech, s.cfg.Codec, &scratch, m, frameBuf, 0)
	if err != nil {
		return err
	}
	buffers := net.Buffers(frameBuf)
	return s.flushBatch(&buffers)
}

// writeFrame encodes body via the mechanism, then writes via the codec
// under the writeMu so PONGs from the reader don't interleave with a
// multipart user message. Used for single-frame COMMAND writes
// (heartbeat PING, PONG reply) where batching is not applicable.
func (s *sessionImpl) writeFrame(flags uint8, body []byte) error {
	wireFlags, encoded, err := s.cfg.Mech.EncodeFrame(flags, body)
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
