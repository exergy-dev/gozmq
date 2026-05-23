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
