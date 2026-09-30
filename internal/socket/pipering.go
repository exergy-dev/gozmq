package socket

import (
	"slices"

	"github.com/exergy-dev/gozmq/internal/pipe"
)

// pipeRing is a round-robin set of pipes with a persistent cursor, shared by
// the engines that load-balance outbound sends (LoadBalancer, reqEngine).
// It owns exactly the pipe-set bookkeeping and the cursor invariant — nothing
// else (no locking, no signalling, no single-peer snapshot). Callers serialize
// access via their own mutex; the methods assume single-threaded entry.
//
// The cursor always points at the peer to try next. remove keeps it pointing
// at the same logical "next" peer across deletions, and advancePast steps it
// forward only when the peer it just used is still where it was — the two
// rules that were previously duplicated, and easy to get subtly wrong,
// across the load-balancing engines.
type pipeRing struct {
	pipes  []*pipe.End
	cursor int
}

// len reports the number of attached pipes.
func (r *pipeRing) len() int { return len(r.pipes) }

// add appends p unless it is already present. Returns true if p was added,
// false if it was a duplicate (no-op).
func (r *pipeRing) add(p *pipe.End) bool {
	if slices.Contains(r.pipes, p) {
		return false
	}
	r.pipes = append(r.pipes, p)
	return true
}

// remove deletes p from the ring and fixes the cursor so round-robin order
// is preserved: if an element before the cursor was removed the cursor shifts
// left, and it is wrapped back into range (or reset to 0 when the ring
// empties). Returns true if p was present.
func (r *pipeRing) remove(p *pipe.End) bool {
	for i, existing := range r.pipes {
		if existing == p {
			r.pipes = slices.Delete(r.pipes, i, i+1)
			if len(r.pipes) == 0 {
				r.cursor = 0
			} else {
				if i < r.cursor {
					r.cursor--
				}
				r.cursor %= len(r.pipes)
			}
			return true
		}
	}
	return false
}

// current returns the index and pipe the cursor points at. Callers must
// ensure len() > 0.
func (r *pipeRing) current() (int, *pipe.End) {
	idx := r.cursor % len(r.pipes)
	return idx, r.pipes[idx]
}

// advancePast steps the cursor to the peer after idx, but only if the pipe
// at idx is still p — guarding against a concurrent remove that already
// shifted the set while the caller had the lock released.
func (r *pipeRing) advancePast(idx int, p *pipe.End) {
	if idx < len(r.pipes) && r.pipes[idx] == p {
		r.cursor = (idx + 1) % len(r.pipes)
	}
}
