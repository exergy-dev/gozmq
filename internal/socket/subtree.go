package socket

import "sync"

// SubTree is a reference-counted byte-prefix radix tree used by PUB/XPUB
// to track subscriptions for a single peer, and by SUB/XSUB to match
// incoming messages against the local subscription list.
//
// Operations:
//   - Add(prefix)      — insert prefix, incrementing its refcount.
//   - Remove(prefix)   — decrement the refcount; drop the prefix when it
//     hits zero; return whether the prefix was fully removed.
//   - Match(topic)     — report whether any stored prefix is a prefix of
//     topic.
//   - Size()           — number of distinct prefixes currently stored.
//
// libzmq semantics:
//   - Add and Remove are reference counted: two Add("foo") calls require
//     two Remove("foo") calls before the prefix disappears from the tree.
//   - Match returns true if ANY prefix in the tree is a prefix of the
//     candidate topic. The empty prefix ([]byte{}) matches every topic.
//   - Add/Remove/Match/Size are all goroutine-safe.
//
// Implementation: a node-per-byte trie with per-node child maps so memory
// stays proportional to stored subscriptions (subscription trees are
// usually sparse away from the root). Match runs in O(L) in the topic
// length with no allocation.
type SubTree struct {
	mu   sync.RWMutex
	root subNode
	size int
}

// subNode is one node in the trie. refs>0 means a subscription ends at
// this node; children holds the next-byte transitions. A map is used
// rather than a fixed-size [256]*subNode to keep memory proportional to
// actual subscriptions — subscription trees are usually sparse away from
// the root.
type subNode struct {
	refs     int
	children map[byte]*subNode
}

// NewSubTree returns an empty SubTree.
func NewSubTree() *SubTree { return &SubTree{} }

// Add inserts prefix into the tree with refcount +1. A duplicate Add
// bumps the refcount at the terminal node.
func (t *SubTree) Add(prefix []byte) {
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
	if n.refs == 0 {
		t.size++
	}
	n.refs++
}

// Remove decrements the refcount for prefix. Returns true if the
// subscription is now fully removed (refcount dropped to zero); false if
// a higher refcount remained or the prefix was not present.
//
// Removing a prefix that isn't in the tree is a no-op (returns false).
func (t *SubTree) Remove(prefix []byte) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Walk down, recording the path so we can prune on the way back up.
	path := make([]*subNode, 0, len(prefix)+1)
	path = append(path, &t.root)
	n := &t.root
	for _, b := range prefix {
		if n.children == nil {
			return false
		}
		child, ok := n.children[b]
		if !ok {
			return false
		}
		path = append(path, child)
		n = child
	}
	if n.refs == 0 {
		return false
	}
	n.refs--
	if n.refs > 0 {
		return false
	}
	t.size--
	// Prune empty nodes back up the path. We cannot prune the root.
	for i := len(prefix); i >= 1; i-- {
		child := path[i]
		parent := path[i-1]
		if child.refs == 0 && len(child.children) == 0 {
			delete(parent.children, prefix[i-1])
			if len(parent.children) == 0 {
				parent.children = nil
			}
			continue
		}
		break
	}
	return true
}

// Match reports whether any subscription prefix in the tree is a prefix
// of topic. The empty prefix ([]byte{}) is considered a prefix of every
// topic, so a tree containing only "" matches everything.
func (t *SubTree) Match(topic []byte) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n := &t.root
	if n.refs > 0 {
		return true
	}
	for _, b := range topic {
		if n.children == nil {
			return false
		}
		child, ok := n.children[b]
		if !ok {
			return false
		}
		if child.refs > 0 {
			return true
		}
		n = child
	}
	return false
}

// Size reports the number of distinct prefixes currently in the tree
// (duplicates increment refcount, not size). Returns 0 for an empty
// tree.
func (t *SubTree) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.size
}
