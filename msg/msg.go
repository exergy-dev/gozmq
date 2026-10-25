// Package msg defines Msg, the multipart ZMTP message exchanged by every
// gozmq socket. The root package re-exports it as [github.com/exergy-dev/gozmq.Msg],
// so applications normally reach these methods through that alias and never
// import this package directly. It exists as its own package so the method
// set is documented in one place and so the internal engine layers can
// reference it without an import cycle through the root package.
//
// # Frame ownership
//
// By default frames are ordinary Go memory. A *Msg returned by
// Socket.RecvMsg owns freshly allocated frames that belong to the caller:
// they may be kept, sliced or re-sent for as long as the caller likes, and
// the garbage collector reclaims them. For such a plain message
// [Msg.Release] and [Msg.Retain] do nothing.
//
// # Lent frames
//
// A message can instead carry lent frames: memory that must go back to its
// owner once nobody uses it. [NewWithRelease] builds one, and a socket
// created with gozmq.WithRecvBufferPool(true) receives its large frames
// into pooled buffers this way. [Msg.Lent] tells the two kinds apart. The
// contract for a lent message is reference counting with explicit calls:
//
//   - Whoever creates or receives a lent message holds one reference to it.
//     [Msg.Retain] adds a reference; [Msg.Release] drops one.
//   - Its frames stay valid until the last reference is released — the
//     message's own, plus one held by every message [Msg.Derive]d from it.
//     After that the memory is handed back (for the pool, reused by later
//     receives), so neither the message nor any frame slice taken from it
//     may be used again.
//   - Releasing more references than were held panics; it never hands the
//     memory back early, because each message gives its hold on the frames
//     up exactly once.
//   - There is no finalizer. A lent message that is dropped without Release
//     is simply collected by the garbage collector: safe, just not recycled.
//   - Socket.SendMsg takes its own reference for the send, released once the
//     message is written (or, for an inproc peer, by the receiver's
//     Release), so sending a message and then releasing it is always
//     correct, whatever the order relative to the actual write.
//   - Wrapping lent frames in a new message with [FromBytes] or [FromFrames]
//     drops the connection to their owner; use [Msg.Derive] instead.
package msg

import "sync/atomic"

// Msg is a ZMTP message: an ordered sequence of binary frames. The zero value
// is an empty message.
//
// Frames are ordinary Go memory owned by whoever holds them unless the
// message is [Msg.Lent]; see the package doc. Frames passed in (FromBytes,
// AppendFrame) are retained by reference, so the caller must not mutate
// them while the message is in flight.
type Msg struct {
	frames       [][]byte
	inlineFrames [1][]byte

	// lease is nil for plain memory, which keeps Release and Retain a nil
	// check on the default path. For lent frames it is shared by every
	// message holding them.
	lease *lease
	// refs counts the references to this *Msg (only meaningful with a
	// lease). The 1→0 transition gives up this message's single hold on
	// the lease, so an over-release cannot steal another message's hold.
	refs atomic.Int32
}

// lease tracks the messages still holding a set of lent frames.
type lease struct {
	holders atomic.Int32
	release func()
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
	out.setFrames(frames)
	return out
}

// FromFramesCopy returns a Msg holding copies of frames, in order, so the
// caller may reuse or mutate the inputs afterwards. Small messages cost a
// single allocation: the Msg, its frame list and the frame bytes share one
// object whose size is a Go allocator size class (one frame up to 256
// bytes in total, two frames up to 144). Anything larger takes two
// allocations (the bytes, and the Msg with its list), or three past two
// frames; still fewer than copying each frame separately.
//
// Every frame is carved from the shared memory with its capacity capped at
// its length, so appending to one frame reallocates instead of overwriting
// its neighbour. An empty frame is non-nil and empty. The shared object is
// one scanned allocation, so the frame bytes are kept alive as long as any
// frame, or the Msg, is; that is negligible at these sizes. The result is
// a plain (not lent) message.
func FromFramesCopy(frames ...[]byte) *Msg {
	total := 0
	for _, f := range frames {
		total += len(f)
	}
	switch len(frames) {
	case 0:
		return FromFrames()
	case 1:
		switch {
		case total <= 32:
			x := &struct {
				Msg
				buf [32]byte
			}{}
			x.carve1(x.buf[:], frames[0])
			return &x.Msg
		case total <= 64:
			x := &struct {
				Msg
				buf [64]byte
			}{}
			x.carve1(x.buf[:], frames[0])
			return &x.Msg
		case total <= 128:
			x := &struct {
				Msg
				buf [128]byte
			}{}
			x.carve1(x.buf[:], frames[0])
			return &x.Msg
		case total <= 256:
			x := &struct {
				Msg
				buf [256]byte
			}{}
			x.carve1(x.buf[:], frames[0])
			return &x.Msg
		}
	case 2:
		switch {
		case total <= 32:
			x := &struct {
				Msg
				list [2][]byte
				buf  [32]byte
			}{}
			x.carve2(&x.list, x.buf[:], frames)
			return &x.Msg
		case total <= 48:
			x := &struct {
				Msg
				list [2][]byte
				buf  [48]byte
			}{}
			x.carve2(&x.list, x.buf[:], frames)
			return &x.Msg
		case total <= 80:
			x := &struct {
				Msg
				list [2][]byte
				buf  [80]byte
			}{}
			x.carve2(&x.list, x.buf[:], frames)
			return &x.Msg
		case total <= 144:
			x := &struct {
				Msg
				list [2][]byte
				buf  [144]byte
			}{}
			x.carve2(&x.list, x.buf[:], frames)
			return &x.Msg
		}
	}
	// Larger messages: one buffer for the bytes, plus the Msg and, past
	// one frame, the list (co-allocated with the Msg for two frames).
	buf := make([]byte, total)
	var m *Msg
	switch len(frames) {
	case 1:
		m = &Msg{}
		m.carve1(buf, frames[0])
		return m
	case 2:
		x := &struct {
			Msg
			list [2][]byte
		}{}
		x.carve2(&x.list, buf, frames)
		return &x.Msg
	}
	m = &Msg{frames: make([][]byte, len(frames))}
	off := 0
	for i, f := range frames {
		off += copy(buf[off:], f)
		m.frames[i] = buf[off-len(f) : off : off]
	}
	return m
}

// carve1 copies f into the front of buf and stores it as the only frame.
func (m *Msg) carve1(buf, f []byte) {
	n := copy(buf, f)
	m.inlineFrames[0] = buf[:n:n]
	m.frames = m.inlineFrames[:1]
}

// carve2 copies the two frames back to back into buf and stores them in
// list, which becomes the frame list.
func (m *Msg) carve2(list *[2][]byte, buf []byte, frames [][]byte) {
	n0 := copy(buf, frames[0])
	n1 := copy(buf[n0:], frames[1])
	list[0] = buf[:n0:n0]
	list[1] = buf[n0 : n0+n1 : n0+n1]
	m.frames = list[:]
}

// setFrames stores a copy of the frame list, inline when it fits.
func (m *Msg) setFrames(frames [][]byte) {
	if len(frames) <= len(m.inlineFrames) {
		copy(m.inlineFrames[:], frames)
		m.frames = m.inlineFrames[:len(frames)]
	} else {
		m.frames = make([][]byte, len(frames))
		copy(m.frames, frames)
	}
}

// NewWithRelease returns a lent Msg holding frames (the list is copied as
// in FromFrames) and one reference to them. release is called exactly
// once, after the last reference is released: this message's own and
// those of every message derived from it (see [Msg.Derive]), including
// the ones sockets take while sending it. Until then the frames must stay
// untouched. release may run on any goroutine, including a socket's
// internal writer, so it must not block. A nil release makes a plain
// message, like FromFrames.
func NewWithRelease(release func(), frames ...[]byte) *Msg {
	if release == nil {
		return FromFrames(frames...)
	}
	// One allocation for the message and its lease.
	ml := &struct {
		Msg
		lease lease
	}{}
	m := &ml.Msg
	m.setFrames(frames)
	ml.lease.release = release
	ml.lease.holders.Store(1)
	m.lease = &ml.lease
	m.refs.Store(1)
	return m
}

// Lent reports whether m's frames are lent memory that Release gives back
// (see the package doc): true for NewWithRelease messages, messages
// derived from them, and messages whose frames a socket received into
// pooled buffers. For every other message Release and Retain do nothing.
func (m *Msg) Lent() bool { return m.lease != nil }

// Derive returns a new message holding frames — typically some of m's
// frames, possibly with others added, as when stripping or adding a
// routing envelope. If m is lent the new message is too and keeps m's
// lent frames valid until it is itself released, independently of m. m is
// not modified and keeps its own reference, so the caller still releases
// it as usual. For a plain m, Derive is FromFrames.
//
// Deriving from a lent message the caller no longer holds a reference to
// panics.
func (m *Msg) Derive(frames ...[]byte) *Msg {
	d := FromFrames(frames...)
	if l := m.lease; l != nil {
		if m.refs.Load() <= 0 {
			panic("gozmq: Msg.Derive on a released message")
		}
		// The caller's live reference keeps holders above zero, so this
		// cannot revive a lease that has already been given back.
		l.holders.Add(1)
		d.lease = l
		d.refs.Store(1)
	}
	return d
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

// Retain adds a reference to a lent message, to be dropped by a matching
// Release; use it to hand the same *Msg to another goroutine that releases
// it independently. It panics if m has already been fully released. For a
// plain message it does nothing.
func (m *Msg) Retain() {
	if m.lease == nil {
		return
	}
	if m.refs.Add(1) <= 1 {
		panic("gozmq: Msg.Retain on a released message")
	}
}

// Release drops one reference to a lent message. When m's last reference
// goes, m gives up its hold on the frames, and once no message holds them
// they are handed back to their owner: neither m nor its frames may be
// used after the caller's final Release. Releasing more references than
// were held panics (without handing the frames back early). For a plain
// message Release does nothing, so calling it unconditionally on every
// received message is always correct.
func (m *Msg) Release() {
	l := m.lease
	if l == nil {
		return
	}
	switch n := m.refs.Add(-1); {
	case n > 0:
	case n == 0:
		if l.holders.Add(-1) == 0 {
			l.release()
		}
	default:
		panic("gozmq: Msg.Release called more times than the message was retained")
	}
}

// TotalSize returns the sum of all frame lengths in bytes.
func (m *Msg) TotalSize() int {
	n := 0
	for _, f := range m.frames {
		n += len(f)
	}
	return n
}
