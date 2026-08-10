// Package bufpool holds the size-tiered []byte pool that backs inbound ZMTP
// frame bodies on the session read path. It lives in internal/ because
// buffer recycling is an implementation detail of the engine layers: the
// public msg package exposes ownership through (*Msg).Retain / Release, not
// through raw Get / Put.
package bufpool

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
// if c is not a tier capacity. Used by Put to route a slice back to its
// origin pool and to reject non-pool slices without data corruption.
func tierIndexForCap(c int) int {
	for i, s := range bufTierSizes {
		if c == s {
			return i
		}
	}
	return -1
}

// Get returns a []byte with len == size and cap >= the tier for that
// size. For size > the largest tier, Get falls back to make([]byte,
// size) with cap == size; the caller can still Put that slice but the pool
// will ignore it (see Put).
//
// The returned bytes are not zeroed; callers treat the buffer as
// uninitialized storage and write into it (ZMTP body reads do io.ReadFull
// which overwrites every byte).
// slicePtrPool recycles *[]byte wrappers so Put can avoid a heap
// escape on every call. Get returns wrappers here; Put draws
// from here. After warm-up the pool is self-sustaining.
var slicePtrPool = sync.Pool{New: func() any { var b []byte; return &b }}

func Get(size int) []byte {
	if size < 0 {
		panic("gozmq/internal/bufpool: Get size < 0")
	}
	i := tierIndexFor(size)
	if i < 0 {
		return make([]byte, size)
	}
	bp := bufPools[i].Get().(*[]byte)
	b := (*bp)[:size]
	*bp = nil
	slicePtrPool.Put(bp)
	return b
}

// Put returns b to the tier pool whose size matches cap(b). Slices
// with non-tier capacity (including oversize fallbacks or user-provided
// buffers not drawn from Get) are silently ignored so callers can be
// conservative about ownership.
//
// Put preserves capacity and resets length to zero; contents are not
// zeroed — inbound frame bodies are already exposed to the peer, so
// zeroing would waste cycles without adding any security.
func Put(b []byte) {
	if b == nil {
		return
	}
	c := cap(b)
	i := tierIndexForCap(c)
	if i < 0 {
		return
	}
	b = b[:0]
	p := slicePtrPool.Get().(*[]byte)
	*p = b
	bufPools[i].Put(p)
}
