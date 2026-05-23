package msg

import (
	"bytes"
	"testing"
)

func TestMsgAppendFrame(t *testing.T) {
	m := New()
	m.AppendFrame([]byte("a"))
	m.AppendFrame([]byte("bb"))
	if m.NumFrames() != 2 {
		t.Fatalf("NumFrames = %d, want 2", m.NumFrames())
	}
	if !bytes.Equal(m.Frames()[0], []byte("a")) || !bytes.Equal(m.Frames()[1], []byte("bb")) {
		t.Fatalf("frames = %q", m.Frames())
	}
	if m.TotalSize() != 3 {
		t.Fatalf("TotalSize = %d, want 3", m.TotalSize())
	}
}

func TestMsgBytesSingleFrame(t *testing.T) {
	m := FromBytes([]byte("hello"))
	if got := string(m.Bytes()); got != "hello" {
		t.Fatalf("Bytes = %q", got)
	}
}

func TestMsgBytesPanicsOnMultipart(t *testing.T) {
	m := FromFrames([]byte("a"), []byte("b"))
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on multipart Bytes")
		}
	}()
	_ = m.Bytes()
}

func TestMsgReset(t *testing.T) {
	m := FromFrames([]byte("x"), []byte("y"))
	m.Reset()
	if m.NumFrames() != 0 {
		t.Fatalf("NumFrames after Reset = %d", m.NumFrames())
	}
}

// TestMsgReleaseNoPooledIsNoop ensures Release on a *Msg with no pooled
// frames is safe (no panic, no negative refcount surprise on later use).
func TestMsgReleaseNoPooledIsNoop(t *testing.T) {
	m := FromBytes([]byte("hello"))
	m.Release()
	m.Release() // double-release with no pool is still a no-op
	if m.NumFrames() != 1 {
		t.Fatalf("NumFrames after Release = %d, want 1", m.NumFrames())
	}
}

// TestMsgRetainReleaseDecrements verifies Retain bumps the count and the
// pooled buffers survive until the final Release.
func TestMsgRetainReleaseDecrements(t *testing.T) {
	// 4 KiB tier buffer used as a pooled frame.
	buf := PoolGet(4 * 1024)
	for i := range buf {
		buf[i] = byte(i)
	}
	m := New()
	m.AppendPooledFrame(buf[:1024], buf)
	if got := m.refs.Load(); got != 1 {
		t.Fatalf("initial refs = %d, want 1", got)
	}
	m.Retain()
	if got := m.refs.Load(); got != 2 {
		t.Fatalf("refs after Retain = %d, want 2", got)
	}
	// First Release: refs -> 1, buffer must NOT be reclaimed yet.
	m.Release()
	if got := m.refs.Load(); got != 1 {
		t.Fatalf("refs after first Release = %d, want 1", got)
	}
	if m.pooled == nil {
		t.Fatalf("pooled buffers reclaimed too early")
	}
	// Second Release: refs -> 0, buffer reclaimed.
	m.Release()
	if got := m.refs.Load(); got != 0 {
		t.Fatalf("refs after second Release = %d, want 0", got)
	}
	if m.pooled != nil {
		t.Fatalf("pooled buffers not reclaimed after final Release")
	}
}

// TestMsgReleaseReturnsBuffersToPool drains all 4 KiB pool entries, then
// performs an AppendPooledFrame + Release cycle and verifies that the
// next PoolGet of the same tier returns a slice with the same backing
// array (i.e., the Release path put it back).
func TestMsgReleaseReturnsBuffersToPool(t *testing.T) {
	// Drain the 4 KiB tier so we have deterministic ownership.
	const tier = 4 * 1024
	drained := make([][]byte, 0, 8)
	for i := 0; i < 8; i++ {
		drained = append(drained, PoolGet(tier))
	}
	defer func() {
		for _, b := range drained {
			PoolPut(b)
		}
	}()

	buf := PoolGet(tier)
	bufAddr := &buf[:1][0]
	m := New()
	m.AppendPooledFrame(buf[:tier], buf)
	m.Release()

	// Pull from the tier; sync.Pool's LIFO behaviour isn't guaranteed
	// across GC cycles but in a hot test with no GC the same backing
	// array typically returns first. We accept any pool-tier slice and
	// just verify the pool isn't empty after release by checking that
	// at least one Get returns our address within a small bounded loop.
	found := false
	probes := make([][]byte, 0, 4)
	for i := 0; i < 4; i++ {
		p := PoolGet(tier)
		probes = append(probes, p)
		if &p[:1][0] == bufAddr {
			found = true
			break
		}
	}
	for _, p := range probes {
		PoolPut(p)
	}
	if !found {
		t.Fatalf("Release did not return pooled buffer to its tier (addr %p never reappeared)", bufAddr)
	}
}

// TestMsgResetForcesReclamation verifies Reset on a *Msg with pooled
// frames returns them immediately regardless of refcount.
func TestMsgResetForcesReclamation(t *testing.T) {
	buf := PoolGet(4 * 1024)
	m := New()
	m.AppendPooledFrame(buf[:512], buf)
	m.Retain() // bump to 2 to simulate a stash
	m.Reset()
	if m.pooled != nil {
		t.Fatalf("Reset did not reclaim pooled buffers")
	}
	if m.NumFrames() != 0 {
		t.Fatalf("Reset did not truncate frames: %d", m.NumFrames())
	}
}

// TestMsgReleaseIdempotentAfterReclamation ensures a Release after the
// pool buffers have been reclaimed (e.g. via Reset) is harmless.
func TestMsgReleaseIdempotentAfterReclamation(t *testing.T) {
	buf := PoolGet(4 * 1024)
	m := New()
	m.AppendPooledFrame(buf[:32], buf)
	m.Reset()
	// pooled is nil now → Release is a no-op even though refs was reset.
	m.Release()
}
