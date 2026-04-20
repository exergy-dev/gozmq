package msg

import (
	"bytes"
	"testing"
)

func TestMsgRelease_IdempotentOwnedBuffers(t *testing.T) {
	m := New()
	buf := PoolGet(16)
	copy(buf, []byte("owned"))
	m.AppendOwnedFrame(buf[:5], buf)

	if got := string(m.Bytes()); got != "owned" {
		t.Fatalf("Bytes = %q, want owned", got)
	}

	m.Release()
	m.Release()

	if m.NumFrames() != 0 {
		t.Fatalf("NumFrames after release = %d, want 0", m.NumFrames())
	}
}

func TestMsgRelease_InvalidatesHeapFrames(t *testing.T) {
	m := FromBytes([]byte("heap"))
	m.Release()
	if m.NumFrames() != 0 {
		t.Fatalf("NumFrames after release = %d, want 0", m.NumFrames())
	}
}

func TestMsgCloneIndependentOfRelease(t *testing.T) {
	m := New()
	buf := PoolGet(16)
	copy(buf, []byte("clone"))
	m.AppendOwnedFrame(buf[:5], buf)

	clone := m.Clone()
	m.Release()

	if !bytes.Equal(clone.Bytes(), []byte("clone")) {
		t.Fatalf("clone bytes = %q, want clone", clone.Bytes())
	}
}
