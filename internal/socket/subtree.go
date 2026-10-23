package socket

import (
	"sync"
	"sync/atomic"
)

// SubTree is a reference-counted byte-prefix trie: PUB/XPUB keep one per
// peer with the peer's subscriptions, SUB/XSUB keep one with the local
// filter. It follows libzmq's semantics: Add and Remove are reference
// counted (two Add("foo") need two Remove("foo") before the prefix is
// gone), and Match reports whether any stored prefix is a prefix of the
// topic, the empty prefix matching every topic.
//
// A PUB matches every peer's tree on every Send, so Match is the path that
// matters. The common case, a subscription to everything, is answered from
// an atomic flag without taking the lock; a selective tree is walked under
// the read lock. Writers hold the write lock and update in place, so a
// subscription change costs only the nodes it adds. Memory stays
// proportional to the stored prefixes: a node holds only the next-byte
// transitions that exist.
type SubTree struct {
	mu   sync.RWMutex
	root subNode
	all  atomic.Bool // root.refs > 0: a subscription to everything
}

// subNode is one trie node: refs > 0 means a subscription ends here, and
// children holds the transitions.
type subNode struct {
	refs     int
	children map[byte]*subNode
}

// NewSubTree returns an empty SubTree.
func NewSubTree() *SubTree { return &SubTree{} }

// Add inserts prefix with refcount +1 and reports whether it is new to the
// tree (the first Add for it since it was last removed).
func (t *SubTree) Add(prefix []byte) (first bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := &t.root
	for _, b := range prefix {
		if n.children == nil {
			n.children = make(map[byte]*subNode)
		}
		child, ok := n.children[b]
		if !ok {
			child = &subNode{}
			n.children[b] = child
		}
		n = child
	}
	n.refs++
	if n == &t.root {
		t.all.Store(true)
	}
	return n.refs == 1
}

// Remove decrements prefix's refcount. held reports whether the prefix was
// present (so a decrement happened), gone whether that dropped it from the
// tree. Removing an absent prefix is a no-op.
func (t *SubTree) Remove(prefix []byte) (held, gone bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Walk down, recording the path so empty nodes can be pruned on the
	// way back up.
	path := make([]*subNode, 0, len(prefix)+1)
	n := &t.root
	path = append(path, n)
	for _, b := range prefix {
		child, ok := n.children[b]
		if !ok {
			return false, false
		}
		path = append(path, child)
		n = child
	}
	if n.refs == 0 {
		return false, false
	}
	n.refs--
	if n.refs > 0 {
		return true, false
	}
	if n == &t.root {
		t.all.Store(false)
	}
	// Prune empty nodes back up the path; the root stays.
	for i := len(prefix); i >= 1; i-- {
		child, parent := path[i], path[i-1]
		if child.refs > 0 || len(child.children) > 0 {
			break
		}
		delete(parent.children, prefix[i-1])
		if len(parent.children) == 0 {
			parent.children = nil
		}
	}
	return true, true
}

// Match reports whether any stored prefix is a prefix of topic.
func (t *SubTree) Match(topic []byte) bool {
	if t.all.Load() {
		return true
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	n := &t.root
	for {
		if n.refs > 0 {
			return true
		}
		if len(topic) == 0 || n.children == nil {
			return false
		}
		child, ok := n.children[topic[0]]
		if !ok {
			return false
		}
		n, topic = child, topic[1:]
	}
}

// Walk calls fn for every stored prefix with its refcount, in no
// particular order, under the read lock: fn must not call Add or Remove
// and must copy any prefix it keeps, since the slice is reused.
func (t *SubTree) Walk(fn func(prefix []byte, refs int)) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	walk(&t.root, nil, fn)
}

func walk(n *subNode, prefix []byte, fn func([]byte, int)) {
	if n.refs > 0 {
		fn(prefix, n.refs)
	}
	for b, child := range n.children {
		walk(child, append(prefix, b), fn)
	}
}
