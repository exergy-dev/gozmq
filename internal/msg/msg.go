// Package msg holds the raw multipart-message type used across the internal
// layers. It lives in internal/ rather than the public root so the iface
// package can reference it without creating an import cycle with the public
// gozmq package (which re-exports Msg via a type alias).
//
// Frame ownership and the refcount.
//
// Most *Msg values own ordinary heap-allocated frame buffers — these need
// no explicit reclamation. The session reader's wire-read path, however,
// optionally hands a *Msg one or more frames backed by buffers drawn from
// the size-tiered pool (PoolGet / PoolPut). Those buffers can be returned
// to the pool when the *Msg is no longer needed.
//
// Reclamation is driven by an atomic refcount on the *Msg.
// AppendPooledFrame sets the refcount to 1 on the first pooled frame; the
// consumer of the *Msg must call Release when done. When the count reaches
// zero the pooled buffers are returned to the pool. A runtime finalizer
// remains as a safety net for callers that drop a *Msg without calling
// Release; it is not the primary reclamation path and should not be
// relied on by performance-sensitive code.
//
// BREAKING: callers that consume a *Msg returned by Socket.RecvMsg and
// care about large-frame throughput should call Release when finished.
// *Msg values that never carry pooled frames (NewMsg / NewMsgFromBytes /
// NewMsgFromFrames) treat Release as a no-op, so calling Release
// unconditionally on every received *Msg is always safe.
package msg

import (
	"runtime"
	"sync"
	"sync/atomic"
)

var msgPool = sync.Pool{New: func() any { return &Msg{} }}

// Msg is a ZMTP message: an ordered sequence of binary frames. The zero value
// is an empty message.
//
// Frame ownership: the user does not need to manage frame buffers for
// *Msg values they construct themselves. *Msg values returned by
// Socket.RecvMsg may carry pool-backed frames; call Release when done to
// return those buffers to the pool. Release is always safe to call (it
// is a no-op for *Msg values without pooled frames).
type Msg struct {
	frames [][]byte
	inlineFrames [1][]byte
	// pooled holds the original pool-allocated buffers that back
	// entries in frames (possibly subsliced). Populated only by the
	// session reader via AppendPooledFrame; reclamation walks this
	// slice on Release / Reset and PoolPuts each entry.
	pooled [][]byte
	// refs tracks outstanding references for pooled-frame reclamation.
	// Zero means "no pooled frames or already reclaimed". The first
	// AppendPooledFrame call installs the initial ref (1) and a
	// safety-net finalizer; Retain bumps the count and Release
	// decrements. When refs drops to zero, the pooled buffers are
	// returned to their size-tiered pool.
	refs atomic.Int32
}

// New returns an empty Msg from the pool.
func New() *Msg {
	m := msgPool.Get().(*Msg)
	m.frames = m.inlineFrames[:0]
	return m
}

// FromBytes returns a single-frame Msg wrapping b without copying.
func FromBytes(b []byte) *Msg {
	m := msgPool.Get().(*Msg)
	m.inlineFrames[0] = b
	m.frames = m.inlineFrames[:1]
	return m
}

// FromFrames returns a Msg whose frames are the provided slices, in order.
func FromFrames(frames ...[]byte) *Msg {
	out := msgPool.Get().(*Msg)
	if len(frames) <= len(out.inlineFrames) {
		copy(out.inlineFrames[:], frames)
		out.frames = out.inlineFrames[:len(frames)]
	} else {
		out.frames = make([][]byte, len(frames))
		copy(out.frames, frames)
	}
	return out
}

// Recycle returns the Msg struct to the pool after clearing all fields.
// The caller must not access m after calling Recycle. Safe to call on
// any *Msg, including nil (no-op).
func Recycle(m *Msg) {
	if m == nil {
		return
	}
	if m.pooled != nil {
		m.reclaimPooled()
	}
	m.refs.Store(0)
	for i := range m.inlineFrames {
		m.inlineFrames[i] = nil
	}
	m.frames = nil
	m.pooled = nil
	msgPool.Put(m)
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
// pool-allocated backing buffer to return via PoolPut when the Msg is
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
// size-tiered pool. Release is idempotent only in the sense that it is
// safe to call on a *Msg with no pooled frames (it is a no-op there);
// double-Release of a pooled *Msg drives the count negative and is a
// programming error.
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
		PoolPut(b)
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

// TransferPool moves pool ownership from src to m, so that a newly
// constructed *Msg (e.g. from FromFrames) inherits the pooled-buffer
// bookkeeping of the original wire-read *Msg. This prevents use-after-free
// when socket engines strip envelope frames and return a new *Msg to the
// caller — the new *Msg becomes the owner of the pooled buffers, and the
// original is disarmed.
func (m *Msg) TransferPool(src *Msg) {
	if src == nil || src.pooled == nil {
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

// Reset truncates the frame slice to zero length without releasing capacity
// and forces immediate reclamation of any pooled buffers. Used internally
// by tests and helper code that reuses a message value directly; callers
// assert that no other reference to the *Msg remains.
func (m *Msg) Reset() {
	if m.pooled != nil {
		m.refs.Store(0)
		runtime.SetFinalizer(m, nil)
		for _, b := range m.pooled {
			PoolPut(b)
		}
		clear(m.pooled)
		m.pooled = nil
	}
	clear(m.frames)
	m.frames = m.frames[:0]
	m.inlineFrames = [1][]byte{}
}
