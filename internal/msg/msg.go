// Package msg holds the raw multipart-message type used across the internal
// layers. It lives in internal/ rather than the public root so the iface
// package can reference it without creating an import cycle with the public
// gozmq package (which re-exports Msg via a type alias).
package msg

// Msg is a ZMTP message: an ordered sequence of binary frames. The zero value
// is an empty message. Frames obtained from a receiving session reference
// pooled buffers valid until the next call on the same socket; use Clone to
// retain a frame beyond that window.
type Msg struct {
	frames [][]byte
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
// past the next socket call unless they Clone first.
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
func (m *Msg) AppendFrame(b []byte) { m.frames = append(m.frames, b) }

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

// Reset truncates the frame slice to zero length without releasing capacity.
// Used by pool-backed Msg values when returning to the pool.
func (m *Msg) Reset() { m.frames = m.frames[:0] }
