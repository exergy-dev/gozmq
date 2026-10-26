// Package bufpool is the size-tiered []byte pool behind the opt-in receive
// buffer pool (gozmq.WithRecvBufferPool): the session reader reads large
// inbound frame bodies into buffers from Get and hands them to the
// application as lent msg.Msg frames whose release func Puts them back.
package bufpool

import (
	"math/bits"
	"sync"
)

// Tiers are the powers of two from 8 KiB to 4 MiB, so a buffer is at most
// twice the frame it holds. Smaller frames never get here (the reader's
// 4 KiB scratch takes them), and larger ones are rare enough that a plain
// allocation is fine: Get falls back to make and Put ignores the result.
const (
	minTierShift = 13 // 8 KiB
	maxTierShift = 22 // 4 MiB
)

// pools holds one sync.Pool of *[]byte per tier; each buffer has exactly
// its tier's capacity.
var pools [maxTierShift - minTierShift + 1]sync.Pool

// headers recycles the *[]byte wrappers, so neither Get nor Put allocates
// once warm.
var headers = sync.Pool{New: func() any { return new([]byte) }}

// tier returns the pool index for a buffer of n bytes, or -1 if n exceeds
// the largest tier.
func tier(n int) int {
	shift := bits.Len(uint(n - 1))
	switch {
	case shift <= minTierShift:
		return 0
	case shift > maxTierShift:
		return -1
	}
	return shift - minTierShift
}

// Get returns a buffer of length n. Its contents are not zeroed: the reader
// overwrites all n bytes with io.ReadFull.
func Get(n int) []byte {
	i := tier(n)
	if i < 0 {
		return make([]byte, n)
	}
	if bp, ok := pools[i].Get().(*[]byte); ok {
		b := (*bp)[:n]
		*bp = nil
		headers.Put(bp)
		return b
	}
	return make([]byte, n, 1<<(minTierShift+i))
}

// Put returns b to the tier matching its capacity. Buffers whose capacity
// is not exactly a tier (Get's oversize fallback) are left to the GC. The
// caller must not use b afterwards.
func Put(b []byte) {
	c := cap(b)
	i := tier(c)
	if i < 0 || 1<<(minTierShift+i) != c {
		return
	}
	bp := headers.Get().(*[]byte) //nolint:errcheck // private pool, New returns only *[]byte
	*bp = b[:0]
	pools[i].Put(bp)
}

// Shared is the package-level pool as a value, for callers that take a
// pool interface (zmtp.BodyPool).
type Shared struct{}

// Get calls the package Get.
func (Shared) Get(n int) []byte { return Get(n) }

// Put calls the package Put.
func (Shared) Put(b []byte) { Put(b) }
