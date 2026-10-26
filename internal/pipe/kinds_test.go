package pipe

import "testing"

// kind describes one pipe implementation under test. span is a message
// count past which a queue-kind direction changes storage (a new chunk),
// so tests probe it; HWM-0 channel pairs use that kind too.
type kind struct {
	name    string
	newPair func(aToB, bToA int, aIdentity, bIdentity []byte) (a, b *End)
	span    int
}

var kinds = []kind{
	{"queue", NewPair, chunkSize},
	{"channel", NewChannelPair, chunkSize},
}

// runner is *testing.T or *testing.B: a TB that runs subtests of its own type.
type runner[TB testing.TB] interface {
	testing.TB
	Run(string, func(TB)) bool
}

// forEachKind runs fn as a subtest (or sub-benchmark) against every pipe kind.
func forEachKind[TB runner[TB]](tb TB, fn func(TB, kind)) {
	tb.Helper()
	for _, k := range kinds {
		tb.Run(k.name, func(tb TB) { fn(tb, k) })
	}
}
