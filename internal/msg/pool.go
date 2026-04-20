package msg

import "sync"

// Design §11 calls for a size-tiered sync.Pool of []byte buffers reused for
// inbound ZMTP frame bodies on the read path. The tiers below match the
// 256 B / 4 KiB / 64 KiB / 1 MiB schedule documented in design.md.
//
// Requests larger than the top tier fall through to make([]byte, size) so
// we do not oversize-pool megabyte-scale outliers. Put silently ignores
// slices whose cap doesn't match a tier (e.g. oversize fallbacks or
// caller-supplied buffers) so ownership is never ambiguous.
//
// Concurrency-safety comes from sync.Pool alone; no additional locking.
// Inbound frame bodies are not user-secret, so we do not zero on release.
//
// NOTE: The pool is read-path only. The writer-side (pipe->wire) still
// hands user-provided buffers verbatim to the codec.

var bufTierSizes = [...]int{
	256,
	4 * 1024,
	64 * 1024,
	1024 * 1024,
}

// bufPools holds one sync.Pool per tier. Each pool's New returns a zero-
// length slice with the full tier capacity so Get's caller can reslice to
// any length <= tier.
var bufPools = [len(bufTierSizes)]sync.Pool{}

func init() {
	for i := range bufPools {
		size := bufTierSizes[i]
		bufPools[i].New = func() any {
			b := make([]byte, 0, size)
			return &b
		}
	}
}

// tierIndexFor returns the smallest tier index whose size is >= n, or -1 if
// n exceeds the largest tier.
func tierIndexFor(n int) int {
	for i, s := range bufTierSizes {
		if n <= s {
			return i
		}
	}
	return -1
}

// tierIndexForCap returns the tier index whose size exactly equals c, or -1
// if c is not a tier capacity. Used by PoolPut to route a slice back to its
// origin pool and to reject non-pool slices without data corruption.
func tierIndexForCap(c int) int {
	for i, s := range bufTierSizes {
		if c == s {
			return i
		}
	}
	return -1
}

// PoolGet returns a []byte with len == size and cap >= the tier for that
// size. For size > the largest tier, PoolGet falls back to make([]byte,
// size) with cap == size; the caller can still Put that slice but the pool
// will ignore it (see PoolPut).
//
// The returned bytes are not zeroed; callers treat the buffer as
// uninitialized storage and write into it (ZMTP body reads do io.ReadFull
// which overwrites every byte).
func PoolGet(size int) []byte {
	if size < 0 {
		panic("gozmq/internal/msg: PoolGet size < 0")
	}
	i := tierIndexFor(size)
	if i < 0 {
		return make([]byte, size)
	}
	bp := bufPools[i].Get().(*[]byte)
	b := (*bp)[:size]
	// Release the pointer wrapper; reuse happens on Put.
	*bp = nil
	return b
}

// PoolPut returns b to the tier pool whose size matches cap(b). Slices
// with non-tier capacity (including oversize fallbacks or user-provided
// buffers not drawn from PoolGet) are silently ignored so callers can be
// conservative about ownership.
//
// Put preserves capacity and resets length to zero; contents are not
// zeroed — inbound frame bodies are already exposed to the peer, so
// zeroing would waste cycles without adding any security.
func PoolPut(b []byte) {
	if b == nil {
		return
	}
	c := cap(b)
	i := tierIndexForCap(c)
	if i < 0 {
		return
	}
	b = b[:0]
	// Store via pointer to avoid the interface-boxing allocation the
	// sync.Pool documentation flags for non-pointer slice values.
	bufPools[i].Put(&b)
}
