// Package msg holds the raw multipart-message type used across the internal
// layers. It lives in internal/ rather than the public root so the iface
// package can reference it without creating an import cycle with the public
// gozmq package (which re-exports Msg via a type alias).
package msg

import "runtime"

// Msg is a ZMTP message: an ordered sequence of binary frames. The zero value
// is an empty message.
//
// Frame ownership: the user does not need to manage frame buffers. A *Msg
// returned by Recv is safe to retain, stash, or pass between goroutines.
// Some frames are backed by size-tiered pool buffers (the session reader's
// wire-read path recycles these); when a *Msg with pool-backed frames is
// reclaimed by the garbage collector, a runtime finalizer returns those
// buffers to the pool. No user action is required.
type Msg struct {
	frames [][]byte
	// pooled holds the original pool-allocated buffers that back
	// entries in frames (possibly subsliced). Populated only by the
	// session reader via AppendPooledFrame; the finalizer walks this
	// slice on Msg reclamation and PoolPuts each entry.
	pooled [][]byte
}

// New returns an empty Msg.
func New() *Msg { return &Msg{} }

// FromBytes returns a single-frame Msg wrapping b without copying.
func FromBytes(b []byte) *Msg { return &Msg{frames: [][]byte{b}} }

// FromFrames returns a Msg whose frames are the provided slices, in order.
func FromFrames(frames ...[]byte) *Msg {
	out := &Msg{frames: make([][]byte, len(frames))}
	copy(out.frames, frames)
	return out
}

// Frames returns the backing frame slice.
func (m *Msg) Frames() [][]byte { return m.frames }

// NumFrames reports the number of frames in the message.
func (m *Msg) NumFrames() int { return len(m.frames) }

// Bytes returns the first frame for single-frame messages. Panics if the
// message has more than one frame; use Frames for multipart.
func (m *Msg) Bytes() []byte {
	if len(m.frames) != 1 {
		panic("gozmq: Msg.Bytes called on multipart message")
	}
	return m.frames[0]
}

// AppendFrame appends a frame to the message. The frame is retained by
// reference — callers must not mutate b after calling.
func (m *Msg) AppendFrame(b []byte) {
	m.frames = append(m.frames, b)
}

// AppendPooledFrame appends frame to the message and remembers pool as the
// pool-allocated backing buffer to return via PoolPut when the Msg is GCed.
// frame may be a subslice of pool. Intended for the session reader only.
//
// The first call installs a runtime finalizer on m; subsequent calls just
// append to the pooled-buffer list. The finalizer runs once, when m has no
// more referents, and returns every pool entry to its size-tiered pool.
func (m *Msg) AppendPooledFrame(frame, pool []byte) {
	if m.pooled == nil {
		runtime.SetFinalizer(m, finalizeMsg)
	}
	m.frames = append(m.frames, frame)
	m.pooled = append(m.pooled, pool)
}

// finalizeMsg returns every pool-backed buffer carried by m to the pool.
// Runs on the GC finalizer goroutine; PoolPut is concurrency-safe.
func finalizeMsg(m *Msg) {
	for _, b := range m.pooled {
		PoolPut(b)
	}
	m.pooled = nil
}

// TotalSize returns the sum of all frame lengths in bytes.
func (m *Msg) TotalSize() int {
	n := 0
	for _, f := range m.frames {
		n += len(f)
	}
	return n
}

// Reset truncates the frame slice to zero length without releasing capacity.
// Used internally by tests and helper code that reuses a message value
// directly.
func (m *Msg) Reset() {
	m.frames = m.frames[:0]
}
