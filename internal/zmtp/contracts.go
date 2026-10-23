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

	// AppendFrameHeader appends the wire header (flags + length) of a frame
	// whose body is bodyLen bytes to dst and returns the grown slice. The
	// body itself is the caller's to write after it, so the writer can hand
	// header and body to writev without copying the body.
	AppendFrameHeader(dst []byte, flags uint8, bodyLen int) ([]byte, error)
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

// ScratchPool is optionally implemented by a BodyPool to supply the
// intermediate buffers ReadFrame collects a large body's first bytes in,
// separately from the body buffers Get hands to the caller. A caller whose
// bodies are plain allocations (no Put ever reaches it) can still recycle
// intermediates this way; a BodyPool without it lends them through Get/Put.
type ScratchPool interface {
	GetScratch(n int) []byte
	PutScratch(b []byte)
}
