package socket

// Size reports the number of distinct prefixes currently in the tree
// (duplicates increment refcount, not size). Returns 0 for an empty
// tree. Test-only accessor: production code never reads the size field,
// so the method lives here rather than in subtree.go.
func (t *SubTree) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.size
}
