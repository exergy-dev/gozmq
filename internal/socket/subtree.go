package socket

import (
	"maps"
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
// The tree is copy-on-write. Nodes are immutable once published; Add and
// Remove build a new path from the root and publish it with one atomic
// store, so Match and Walk read a consistent snapshot with no lock. A PUB
// matches every peer's tree on every Send, so that read path is the one
// that matters; subscriptions change rarely, and their writers serialize
// on mu. Memory stays proportional to the stored prefixes: a node holds
// only the next-byte transitions that exist.
type SubTree struct {
	mu   sync.Mutex
	root atomic.Pointer[subNode] // nil is the empty tree
}

// subNode is one trie node: refs > 0 means a subscription ends here, and
// children holds the transitions. Both are fixed once the node is reachable
// from a published root.
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
	root, first := add(t.root.Load(), prefix)
	t.root.Store(root)
	return first
}

// Remove decrements prefix's refcount. held reports whether the prefix was
// present (so a decrement happened), gone whether that dropped it from the
// tree. Removing an absent prefix is a no-op.
func (t *SubTree) Remove(prefix []byte) (held, gone bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	root, held, gone := remove(t.root.Load(), prefix)
	if held {
		t.root.Store(root)
	}
	return held, gone
}

// Match reports whether any stored prefix is a prefix of topic.
func (t *SubTree) Match(topic []byte) bool {
	n := t.root.Load()
	for n != nil {
		if n.refs > 0 {
			return true
		}
		if len(topic) == 0 {
			return false
		}
		n = n.children[topic[0]]
		topic = topic[1:]
	}
	return false
}

// Walk calls fn for every stored prefix with its refcount, in no
// particular order. prefix is reused between calls, so fn must copy what
// it keeps. The walk sees the tree as of the call; concurrent changes are
// not reflected.
func (t *SubTree) Walk(fn func(prefix []byte, refs int)) {
	walk(t.root.Load(), nil, fn)
}

func walk(n *subNode, prefix []byte, fn func([]byte, int)) {
	if n == nil {
		return
	}
	if n.refs > 0 {
		fn(prefix, n.refs)
	}
	for b, child := range n.children {
		walk(child, append(prefix, b), fn)
	}
}

// add returns the node replacing n once prefix has one more reference,
// and whether that reference is the first.
func add(n *subNode, prefix []byte) (*subNode, bool) {
	c := n.clone()
	if len(prefix) == 0 {
		c.refs++
		return c, c.refs == 1
	}
	child, first := add(n.child(prefix[0]), prefix[1:])
	c.setChild(prefix[0], child)
	return c, first
}

// remove returns the node replacing n (nil when it becomes empty) once
// prefix has one reference fewer, with Remove's held and gone results.
// n is returned as is when the prefix is absent.
func remove(n *subNode, prefix []byte) (_ *subNode, held, gone bool) {
	if n == nil {
		return nil, false, false
	}
	if len(prefix) == 0 {
		if n.refs == 0 {
			return n, false, false
		}
		c := n.clone()
		c.refs--
		return c.orNil(), true, c.refs == 0
	}
	child, held, gone := remove(n.child(prefix[0]), prefix[1:])
	if !held {
		return n, false, false
	}
	c := n.clone()
	c.setChild(prefix[0], child)
	return c.orNil(), true, gone
}

func (n *subNode) child(b byte) *subNode {
	if n == nil {
		return nil
	}
	return n.children[b]
}

// clone returns a mutable copy of n (an empty node for nil) whose
// children map is its own.
func (n *subNode) clone() *subNode {
	if n == nil {
		return &subNode{}
	}
	return &subNode{refs: n.refs, children: maps.Clone(n.children)}
}

// setChild sets or, for a nil child, deletes the transition on b. Only
// called on a clone.
func (n *subNode) setChild(b byte, child *subNode) {
	switch {
	case child != nil && n.children == nil:
		n.children = map[byte]*subNode{b: child}
	case child != nil:
		n.children[b] = child
	default:
		delete(n.children, b)
		if len(n.children) == 0 {
			n.children = nil
		}
	}
}

// orNil returns n, or nil when it holds nothing.
func (n *subNode) orNil() *subNode {
	if n.refs == 0 && len(n.children) == 0 {
		return nil
	}
	return n
}
