// Package msg holds the raw multipart-message type used across the internal
// layers. It lives in internal/ rather than the public root so the iface
// package can reference it without creating an import cycle with the public
// gozmq package (which re-exports Msg via a type alias).
package msg

// Msg is a ZMTP message: an ordered sequence of binary frames. The zero value
// is an empty message. Frames obtained from a receiving session may reference
// pooled buffers; call Release when the message is no longer needed so those
// buffers can be reused. Clone retains the data independently.
type Msg struct {
	frames   [][]byte
	owned    [][]byte
	released bool
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

// Frames returns the backing frame slice. Callers MUST NOT retain references
// past Msg.Release unless they Clone first.
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
	if len(m.owned) != 0 {
		m.owned = append(m.owned, nil)
	}
}

// AppendOwnedFrame appends a frame whose lifetime is tied to owned. When the
// message is released, owned is returned to the size-tiered frame buffer pool.
// frame may be a subslice of owned.
func (m *Msg) AppendOwnedFrame(frame, owned []byte) {
	if len(m.owned) < len(m.frames) {
		m.owned = append(m.owned, make([][]byte, len(m.frames)-len(m.owned))...)
	}
	m.frames = append(m.frames, frame)
	m.owned = append(m.owned, owned)
}

// Clone returns an independently-owned deep copy of the message.
func (m *Msg) Clone() *Msg {
	out := &Msg{frames: make([][]byte, len(m.frames))}
	for i, f := range m.frames {
		out.frames[i] = append([]byte(nil), f...)
	}
	return out
}

// TotalSize returns the sum of all frame lengths in bytes.
func (m *Msg) TotalSize() int {
	n := 0
	for _, f := range m.frames {
		n += len(f)
	}
	return n
}

// Reset truncates the frame and ownership slices to zero length without
// releasing capacity. Used internally by tests and helper code that reuses a
// message value directly.
func (m *Msg) Reset() {
	m.frames = m.frames[:0]
	m.owned = m.owned[:0]
	m.released = false
}

// Release returns any pool-owned frame buffers carried by the message and
// invalidates the message. It is idempotent.
func (m *Msg) Release() {
	if m == nil || m.released {
		return
	}
	for _, b := range m.owned {
		PoolPut(b)
	}
	m.frames = m.frames[:0]
	m.owned = m.owned[:0]
	m.released = true
}
