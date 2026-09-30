// Package msg defines Msg, the multipart ZMTP message exchanged by every
// gozmq socket. The root package re-exports it as [github.com/exergy-dev/gozmq.Msg],
// so applications normally reach these methods through that alias and never
// import this package directly. It exists as its own package so the method
// set is documented in one place and so the internal engine layers can
// reference it without an import cycle through the root package.
//
// # Compatibility
//
// The methods in the "Supported API" section of this package are covered by
// the module's compatibility promise. The methods marked "Internal plumbing"
// are exported only because gozmq's own internal packages must call them
// across a package boundary; they are NOT covered by the compatibility
// promise and may change or disappear in any release. Applications must not
// call them.
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
	// subscription marks this *Msg as a genuine ZMTP SUBSCRIBE/CANCEL
	// command surfaced by the session reader (handleCommand), as opposed
	// to ordinary data that merely happens to begin with 0x01/0x00. It is
	// a process-local hint that never crosses the wire: the receiving SUB
	// (drop) / XSUB (surface) Recv path checks it instead of sniffing the
	// payload, so data frames starting with 0x00/0x01 are delivered
	// intact. The PUB/XPUB receive side still classifies by content
	// because legacy peers send subscriptions as plain 0x00/0x01 data
	// frames (ZMTP 3.0) which never carry this flag.
	subscription bool
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

// FromFrames returns a Msg whose frames are a COPY of the provided slices, in
// order. Use FromFramesOwned when the caller built the slice locally and can
// hand off ownership without the copy.
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

// Internal plumbing — NOT covered by the compatibility promise; do not
// call from application code.
//
// FromFramesOwned returns a Msg that adopts the frames slice header directly,
// without copying it. The caller must not retain or mutate frames afterward.
// This avoids the redundant [][]byte allocation FromFrames makes for
// multi-frame messages built locally and immediately handed off (the common
// envelope-prepend/strip case in REQ/REP/ROUTER).
func FromFramesOwned(frames [][]byte) *Msg {
	return &Msg{frames: frames}
}

// Internal plumbing — NOT covered by the compatibility promise; do not
// call from application code.
//
// MarkSubscription flags m as a genuine subscription command (see the
// subscription field). Set by the session reader when it converts a
// wire SUBSCRIBE/CANCEL command into a synthetic single-frame message.
func (m *Msg) MarkSubscription() { m.subscription = true }

// IsSubscription reports whether m was flagged as a genuine subscription
// command via MarkSubscription. The SUB/XSUB Recv path uses this instead
// of payload sniffing so ordinary data starting with 0x00/0x01 is not
// mistaken for a subscription.
func (m *Msg) IsSubscription() bool { return m.subscription }

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
