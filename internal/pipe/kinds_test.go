package pipe

import "testing"

// kind describes one pipe implementation under test. span is a message
// count past which that kind changes storage (queue chunk, or channel into
// overflow), so tests probe it.
type kind struct {
	name    string
	newPair func(aToB, bToA int, aInfo, bInfo PeerInfo) (a, b *End)
	span    int
}

var kinds = []kind{
	{"queue", NewPair, chunkSize},
	{"channel", NewChannelPair, unboundedChanCap},
}

// forEachKind runs fn as a subtest against every pipe kind.
func forEachKind(t *testing.T, fn func(t *testing.T, k kind)) {
	t.Helper()
	for _, k := range kinds {
		t.Run(k.name, func(t *testing.T) { fn(t, k) })
	}
}

// forEachKindB runs fn as a sub-benchmark against every pipe kind.
func forEachKindB(b *testing.B, fn func(b *testing.B, k kind)) {
	b.Helper()
	for _, k := range kinds {
		b.Run(k.name, func(b *testing.B) { fn(b, k) })
	}
}
