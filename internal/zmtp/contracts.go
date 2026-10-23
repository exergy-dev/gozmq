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
	// otherwise it is a buffer of length n from pool.Get (a fresh
	// allocation when pool is nil) owned by the caller. Implementations
	// must not allocate the full body from the length header alone: memory
	// for a body must grow with the bytes actually received, so a peer that
	// announces a huge frame and stalls cannot pin the whole cap.
	ReadFrame(r io.Reader, dst []byte, pool BodyPool) (flags uint8, body []byte, err error)

	// WriteFrame writes flags + length + body as one ZMTP frame. Multi-byte
	// lengths are big-endian per the spec. Implementations may internally
	// batch via net.Buffers; callers should not assume one write = one
	// socket operation.
	WriteFrame(w io.Writer, flags uint8, body []byte) error
}

// BodyPool supplies frame body buffers to ReadFrame. Get returns a buffer of
// length n (its contents need not be zeroed); Put takes back a buffer the
// codec used only as an intermediate and will not return to the caller.
// The buffer ReadFrame returns is always one from Get, so the caller can Put
// it back once the frame is released.
type BodyPool interface {
	Get(n int) []byte
	Put(b []byte)
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
