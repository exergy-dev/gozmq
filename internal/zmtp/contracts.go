package zmtp

import "io"

// ZMTP frame flag bits (ZMTP 3.1 §2.1).
const (
	FlagMore    uint8 = 0x01
	FlagLong    uint8 = 0x02
	FlagCommand uint8 = 0x04
)

// FrameCodec encodes and decodes ZMTP frames. Implementations must be
// goroutine-safe only to the extent that concurrent Read/Write operate on
// disjoint io.Reader/io.Writer pairs; a single codec value is typically
// shared across sessions without synchronization.
type FrameCodec interface {
	// ReadFrame reads one frame from r. Returns the flags byte and a body
	// slice. If dst has sufficient capacity the body aliases dst[:n];
	// otherwise it is a freshly allocated buffer owned by the caller.
	ReadFrame(r io.Reader, dst []byte) (flags uint8, body []byte, err error)

	// WriteFrame writes flags + length + body as one ZMTP frame. Multi-byte
	// lengths are big-endian per the spec. Implementations may internally
	// batch via net.Buffers; callers should not assume one write = one
	// socket operation.
	WriteFrame(w io.Writer, flags uint8, body []byte) error
}

// FrameHeaderAppender is the allocation-free fast path used by the writer
// batching loop. It appends the wire-format frame header (flags + length)
// for a body of len(body) bytes onto scratch and returns the grown scratch.
// Callers materialize net.Buffers entries from scratch + body slices after
// the entire batch has been encoded — by that point scratch's underlying
// array is stable, so subslices referencing scratch[off:off+hdrLen] do not
// race with further appends.
//
// This method allocates no closures and uses no internal sync.Pool: header
// bytes live in caller-owned scratch storage that the caller recycles
// across batches.
type FrameHeaderAppender interface {
	AppendFrameHeader(scratch []byte, flags uint8, body []byte) (newScratch []byte, headerLen int, err error)
}
