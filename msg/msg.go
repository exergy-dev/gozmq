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
// # Frame ownership and the refcount
//
// Most *Msg values own ordinary heap-allocated frame buffers — these need
// no explicit reclamation. The session reader's wire-read path, however,
// optionally hands a *Msg one or more frames backed by buffers drawn from
// an internal size-tiered pool. Those buffers can be returned to the pool
// when the *Msg is no longer needed.
//
// Reclamation is driven by an atomic refcount on the *Msg. The reader
// installs the initial reference on the first pooled frame; the consumer of
// the *Msg calls [Msg.Release] when done. When the count reaches zero the
// pooled buffers are returned to the pool. A runtime finalizer remains as a
// safety net for callers that drop a *Msg without calling Release; it is
// not the primary reclamation path and should not be relied on by
// performance-sensitive code.
//
// Callers that consume a *Msg returned by Socket.RecvMsg and care about
// large-frame throughput should call Release when finished. *Msg values
// that never carry pooled frames (New / FromBytes / FromFrames, and the
// gozmq.NewMsg* constructors) treat Release as a no-op, so calling Release
// unconditionally on every received *Msg is always safe.
package msg

import (
	"runtime"
	"sync/atomic"

	"github.com/exergy-dev/gozmq/internal/bufpool"
)

// Msg is a ZMTP message: an ordered sequence of binary frames. The zero value
// is an empty message.
//
// Frame ownership: the user does not need to manage frame buffers for
// *Msg values they construct themselves. *Msg values returned by
// Socket.RecvMsg may carry pool-backed frames; call Release when done to
// return those buffers to the pool. Release is always safe to call (it
// is a no-op for *Msg values without pooled frames).
type Msg struct {
	frames       [][]byte
	inlineFrames [1][]byte
	// pooled holds the original pool-allocated buffers that back
	// entries in frames (possibly subsliced). Populated only by the
	// session reader via AppendPooledFrame; reclamation walks this
	// slice on Release / Reset, returning each entry to the buffer pool.
	pooled [][]byte
	// refs tracks outstanding references for pooled-frame reclamation.
	// Zero means "no pooled frames or already reclaimed". The first
	// AppendPooledFrame call installs the initial ref (1) and a
	// safety-net finalizer; Retain bumps the count and Release
	// decrements. When refs drops to zero, the pooled buffers are
	// returned to their size-tiered pool.
	refs atomic.Int32
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

// New returns an empty Msg. The *Msg struct is plain-allocated (not pooled):
// a sync.Pool of *Msg was measured net-negative because messages are produced
// on one goroutine and consumed on another, giving a ~0% pool hit rate while
// still paying the cross-P poolChain steal cost. The size-tiered FRAME-BUFFER
// pool (internal/bufpool) is unaffected — that one earns its keep on the
// large-frame wire-read path.
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
// Recycle releases one reference to m, returning its pool-backed frame
// buffers to the size-tiered frame pool when the last reference drops
// (it is the function-form equivalent of Release). The *Msg struct itself
// is left to the GC (it is not pooled). A no-op for *Msg values that never
// carried pooled frames, and safe on nil. The caller must not access m's
// frames after Recycle.
//
// Refcount-aware and safe for concurrent use: when an engine fans one
// *Msg out to N pipes it Retains N-1 extra references, and the N session
// writers may Recycle concurrently post-flush — exactly one of them
// (the last) reclaims the pooled buffers. The fast path for the common
// single-reference case is one atomic decrement.
func Recycle(m *Msg) {
	if m == nil {
		return
	}
	m.Release()
}

// Internal plumbing — NOT covered by the compatibility promise; do not
// call from application code.
//
// IsPooled reports whether m carries any pool-backed frame buffers that
// require reclamation (via Recycle/Release). It is false for every
// caller-constructed message (New / FromBytes / FromFrames / FromFramesOwned)
// and true only for large inbound frames the session reader backed with a
// pool buffer. The writer uses this to skip deferred-recycle bookkeeping for
// the common non-pooled case, where Recycle would be a no-op anyway.
func (m *Msg) IsPooled() bool { return m.pooled != nil }

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

// Internal plumbing — NOT covered by the compatibility promise; do not
// call from application code.
//
// AppendPooledFrame appends frame to the message and remembers pool as the
// pool-allocated backing buffer to return to the buffer pool when the Msg is
// released. frame may be a subslice of pool. Intended for the session
// reader only.
//
// The first call installs the initial refcount (1) and a safety-net
// finalizer. Subsequent calls just append to the pooled-buffer list; the
// refcount stays at 1 because the consumer still holds exactly one
// reference to the whole *Msg.
func (m *Msg) AppendPooledFrame(frame, pool []byte) {
	if m.pooled == nil {
		m.refs.Store(1)
		runtime.SetFinalizer(m, finalizeMsg)
	}
	m.frames = append(m.frames, frame)
	m.pooled = append(m.pooled, pool)
}

// Retain atomically increments the refcount. Callers that stash a *Msg
// past the lifetime of the goroutine that received it should Retain
// before handing the *Msg off and arrange for an additional Release when
// the stash is dropped.
//
// Retain on a *Msg with no pooled frames is a cheap no-op-equivalent: the
// counter still increments but Release is also a no-op until the *Msg
// acquires a pooled frame (which only the session reader does).
func (m *Msg) Retain() {
	m.refs.Add(1)
}

// Release decrements the refcount. When the count reaches zero and the
// *Msg has pooled-backed frames, those buffers are returned to the
// size-tiered pool. Concurrent Release calls from holders of distinct
// references are safe: exactly one (the last) reclaims. Release after
// reclamation is a no-op (pooled is nil by then); releasing more
// references than were ever held is a programming error, though it
// cannot double-reclaim.
func (m *Msg) Release() {
	if m.pooled == nil {
		// Construction path never installed a refcount; nothing to do.
		return
	}
	if m.refs.Add(-1) > 0 {
		return
	}
	m.reclaimPooled()
}

// reclaimPooled returns every pool-backed buffer carried by m to the
// pool and clears the pooled slice so subsequent calls are no-ops.
// Caller must have observed refs <= 0 (or be holding a unique reference
// in a finalizer / Reset path).
func (m *Msg) reclaimPooled() {
	for _, b := range m.pooled {
		bufpool.Put(b)
	}
	clear(m.pooled)
	m.pooled = nil
	// Clear the finalizer so we don't double-reclaim if the *Msg
	// becomes unreachable later.
	runtime.SetFinalizer(m, nil)
}

// finalizeMsg is the safety net for *Msg values that carry pooled frames
// but were dropped without an explicit Release (e.g. by callers from an
// earlier API version). It reclaims iff the refcount is still positive,
// which means reclamation has not already happened via Release.
func finalizeMsg(m *Msg) {
	if m.pooled == nil {
		return
	}
	if m.refs.Load() <= 0 {
		return
	}
	m.reclaimPooled()
}

// Internal plumbing — NOT covered by the compatibility promise; do not
// call from application code.
//
// TransferPool moves pool ownership from src to m, so that a newly
// constructed *Msg (e.g. from FromFrames) inherits the pooled-buffer
// bookkeeping of the original wire-read *Msg. This prevents use-after-free
// when socket engines strip envelope frames and return a new *Msg to the
// caller — the new *Msg becomes the owner of the pooled buffers, and the
// original is disarmed.
//
// Per-*Msg state and how each field is handled across the ownership
// primitives (keep in sync when adding fields):
//
//   - frames / inlineFrames: the message payload. Set by the constructors
//     and Append*; cleared by Reset.
//   - pooled / refs: pooled-buffer reclamation bookkeeping. Installed by
//     AppendPooledFrame; MOVED here (transferred, src disarmed); reclaimed
//     by Release/Reset/finalizeMsg.
//   - subscription: process-local SUBSCRIBE/CANCEL hint (see the field
//     doc). Propagated here so a stripped/re-wrapped *Msg keeps the flag;
//     cleared by Reset. Never crosses the wire.
//
// TransferPool returns early when src carries no pooled frames; the
// subscription flag is propagated regardless so the hint survives an
// envelope rewrite even on non-pooled messages.
func (m *Msg) TransferPool(src *Msg) {
	if src == nil {
		return
	}
	m.subscription = src.subscription
	if src.pooled == nil {
		return
	}
	m.pooled = src.pooled
	m.refs.Store(src.refs.Load())
	src.pooled = nil
	src.refs.Store(0)
	runtime.SetFinalizer(src, nil)
	runtime.SetFinalizer(m, finalizeMsg)
}

// TotalSize returns the sum of all frame lengths in bytes.
func (m *Msg) TotalSize() int {
	n := 0
	for _, f := range m.frames {
		n += len(f)
	}
	return n
}

// Internal plumbing — NOT covered by the compatibility promise; do not
// call from application code.
//
// Reset truncates the frame slice to zero length without releasing capacity
// and forces immediate reclamation of any pooled buffers. Used internally
// by tests and helper code that reuses a message value directly; callers
// assert that no other reference to the *Msg remains.
func (m *Msg) Reset() {
	if m.pooled != nil {
		m.refs.Store(0)
		m.reclaimPooled()
	}
	clear(m.frames)
	m.frames = m.frames[:0]
	m.inlineFrames = [1][]byte{}
	m.subscription = false
}
