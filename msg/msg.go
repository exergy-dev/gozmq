// Package msg defines Msg, the multipart ZMTP message exchanged by every
// gozmq socket. The root package re-exports it as [github.com/exergy-dev/gozmq.Msg],
// so applications normally reach these methods through that alias and never
// import this package directly. It exists as its own package so the method
// set is documented in one place and so the internal engine layers can
// reference it without an import cycle through the root package.
//
// # Frame ownership
//
// Frames are ordinary Go memory. A *Msg returned by Socket.RecvMsg owns
// freshly allocated frames that belong to the caller: they may be kept,
// sliced or re-sent for as long as the caller likes, and the garbage
// collector reclaims them. There is no buffer pool and nothing to release;
// [Msg.Release] and [Msg.Retain] are no-ops kept for API compatibility.
package msg

// Msg is a ZMTP message: an ordered sequence of binary frames. The zero value
// is an empty message.
//
// Frames are ordinary Go memory owned by whoever holds them; see the package
// doc. Frames passed in (FromBytes, AppendFrame) are retained by reference, so
// the caller must not mutate them while the message is in flight.
type Msg struct {
	frames       [][]byte
	inlineFrames [1][]byte
}

// New returns an empty Msg.
func New() *Msg {
	m := &Msg{}
	m.frames = m.inlineFrames[:0]
	return m
}

// FromBytes returns a single-frame Msg wrapping b without copying.
func FromBytes(b []byte) *Msg {
	m := &Msg{}
	m.inlineFrames[0] = b
	m.frames = m.inlineFrames[:1]
	return m
}

// FromFrames returns a Msg holding the provided frames, in order. The frame
// list is copied, so the caller may reuse the slice passed in; the frames
// themselves are retained by reference.
func FromFrames(frames ...[]byte) *Msg {
	out := &Msg{}
	if len(frames) <= len(out.inlineFrames) {
		copy(out.inlineFrames[:], frames)
		out.frames = out.inlineFrames[:len(frames)]
	} else {
		out.frames = make([][]byte, len(frames))
		copy(out.frames, frames)
	}
	return out
}

// Frames returns the backing frame slice.
func (m *Msg) Frames() [][]byte { return m.frames }

// NumFrames reports the number of frames in the message.
func (m *Msg) NumFrames() int { return len(m.frames) }

// Bytes returns the first frame for single-frame messages. Panics if the
// message has anything other than one frame; use SingleFrame for a safe
// single-frame accessor or Frames for multipart.
func (m *Msg) Bytes() []byte {
	if len(m.frames) != 1 {
		panic("gozmq: Msg.Bytes called on multipart message")
	}
	return m.frames[0]
}

// SingleFrame returns the only frame in m. The boolean is false when m is
// empty or multipart.
func (m *Msg) SingleFrame() ([]byte, bool) {
	if len(m.frames) != 1 {
		return nil, false
	}
	return m.frames[0], true
}

// AppendFrame appends a frame to the message. The frame is retained by
// reference — callers must not mutate b after calling.
func (m *Msg) AppendFrame(b []byte) {
	m.frames = append(m.frames, b)
}

// Retain does nothing. It is retained for compatibility with code written
// against the pooled-buffer API; frames are ordinary Go memory owned by
// the caller.
func (m *Msg) Retain() {}

// Release does nothing. It is retained for compatibility with code written
// against the pooled-buffer API; frames are ordinary Go memory owned by
// the caller, so there is nothing to give back and calling it is optional.
func (m *Msg) Release() {}

// TotalSize returns the sum of all frame lengths in bytes.
func (m *Msg) TotalSize() int {
	n := 0
	for _, f := range m.frames {
		n += len(f)
	}
	return n
}
