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

func TestMsgResetReleasesPooledFrames(t *testing.T) {
	pool := PoolGet(1024)
	m := New()
	m.AppendPooledFrame(pool[:16], pool)
	holder := m.pooled
	if len(holder) != 1 {
		t.Fatalf("pooled buffers after AppendPooledFrame = %d, want 1", len(holder))
	}

	m.Reset()
	if m.NumFrames() != 0 {
		t.Fatalf("NumFrames after Reset = %d, want 0", m.NumFrames())
	}
	if m.pooled != nil {
		t.Fatal("pooled buffers still attached after Reset")
	}

	m.Reset()
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
	m.Release()
	if got := m.refs.Load(); got != 1 {
		t.Fatalf("refs after first Release = %d, want 1", got)
	}
	if m.pooled == nil {
		t.Fatalf("pooled buffers reclaimed too early")
	}
	m.Release()
	if got := m.refs.Load(); got != 0 {
		t.Fatalf("refs after second Release = %d, want 0", got)
	}
	if m.pooled != nil {
		t.Fatalf("pooled buffers not reclaimed after final Release")
	}
}

// TestMsgResetForcesReclamation verifies Reset on a *Msg with pooled
// frames returns them immediately regardless of refcount.
func TestMsgResetForcesReclamation(t *testing.T) {
	buf := PoolGet(4 * 1024)
	m := New()
	m.AppendPooledFrame(buf[:512], buf)
	m.Retain()
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
	m.Release()
}
