package socket

// Size reports the number of distinct prefixes currently in the tree
// (duplicates increment refcount, not size). Test-only accessor.
func (t *SubTree) Size() int {
	n := 0
	t.Walk(func([]byte, int) { n++ })
	return n
}
