package socket_test

import (
	"sync"
	"testing"

	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubTree_EmptyMatchesNothing(t *testing.T) {
	t.Parallel()
	tr := socket.NewSubTree()
	require.Equal(t, 0, tr.Size(), "fresh tree size")
	cases := [][]byte{
		nil, {}, []byte("x"), []byte("foo"), []byte{0x00},
	}
	for _, c := range cases {
		assert.False(t, tr.Match(c), "empty tree matched %q", c)
	}
}

func TestSubTree_EmptyPrefixMatchesEverything(t *testing.T) {
	t.Parallel()
	tr := socket.NewSubTree()
	tr.Add([]byte{})
	require.Equal(t, 1, tr.Size(), "size after Add(empty)")
	cases := [][]byte{
		nil, {}, []byte("x"), []byte("foo"), []byte{0x00, 0xff},
	}
	for _, c := range cases {
		assert.True(t, tr.Match(c), "empty-prefix tree did not match %q", c)
	}
}

func TestSubTree_PrefixMatching(t *testing.T) {
	t.Parallel()
	tr := socket.NewSubTree()
	tr.Add([]byte("foo"))

	tests := []struct {
		topic []byte
		want  bool
	}{
		{[]byte("foo"), true},
		{[]byte("foobar"), true},
		{[]byte("fo"), false},
		{[]byte("f"), false},
		{[]byte(""), false},
		{[]byte("bar"), false},
		{[]byte("Foo"), false}, // case-sensitive
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, tr.Match(tc.topic), "Match(%q)", tc.topic)
	}
}

func TestSubTree_MultipleDistinctPrefixes(t *testing.T) {
	t.Parallel()
	tr := socket.NewSubTree()
	tr.Add([]byte("foo"))
	tr.Add([]byte("bar"))
	tr.Add([]byte("fooX"))

	require.Equal(t, 3, tr.Size())
	for _, topic := range []string{"foo", "foobar", "fooX", "fooXy", "bar", "barbaz"} {
		assert.True(t, tr.Match([]byte(topic)), "Match(%q) = false, want true", topic)
	}
	for _, topic := range []string{"", "f", "fo", "ba", "baz"} {
		assert.False(t, tr.Match([]byte(topic)), "Match(%q) = true, want false", topic)
	}
}

func TestSubTree_RefcountAddRemoveParity(t *testing.T) {
	t.Parallel()
	tr := socket.NewSubTree()

	tr.Add([]byte("foo"))
	tr.Add([]byte("foo"))
	require.Equal(t, 1, tr.Size(), "size after two duplicate Adds")
	require.True(t, tr.Match([]byte("foobar")), "Match after two adds")
	// First Remove decrements refcount but the prefix is still present.
	require.False(t, tr.Remove([]byte("foo")), "first Remove should return false (refcount still >0)")
	require.True(t, tr.Match([]byte("foobar")), "Match after first Remove")
	require.Equal(t, 1, tr.Size(), "size after first Remove")
	// Second Remove actually removes it.
	require.True(t, tr.Remove([]byte("foo")), "second Remove should return true")
	require.False(t, tr.Match([]byte("foobar")), "Match after second Remove")
	require.Equal(t, 0, tr.Size(), "size after second Remove")
	// Removing an absent prefix is a no-op.
	require.False(t, tr.Remove([]byte("foo")), "Remove on absent prefix")
	require.False(t, tr.Remove([]byte("never-added")), "Remove on never-added prefix")
}

func TestSubTree_PartialPathPruning(t *testing.T) {
	t.Parallel()
	tr := socket.NewSubTree()
	// Add foo and foobar; remove foobar should not disturb foo.
	tr.Add([]byte("foo"))
	tr.Add([]byte("foobar"))
	require.Equal(t, 2, tr.Size())
	require.True(t, tr.Remove([]byte("foobar")), "Remove(foobar)")
	require.Equal(t, 1, tr.Size(), "size after remove foobar")
	require.True(t, tr.Match([]byte("foo")), "Match(foo)")
	require.True(t, tr.Match([]byte("foobar")), "Match(foobar) after removing foobar-sub but keeping foo-sub (foo is still a prefix)")
	require.True(t, tr.Remove([]byte("foo")), "Remove(foo)")
	require.False(t, tr.Match([]byte("foo")), "Match(foo) after removing both")
}

func TestSubTree_ConcurrentSafety(t *testing.T) {
	t.Parallel()
	tr := socket.NewSubTree()

	const goroutines = 8
	const iters = 500
	prefixes := [][]byte{
		[]byte(""), []byte("a"), []byte("ab"), []byte("abc"),
		[]byte("x"), []byte("xy"), []byte("xyz"), []byte("pqr"),
	}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(2)
		// Writer: repeatedly Add/Remove the same prefix, keeping refcount at 0 on exit.
		go func(id int) {
			defer wg.Done()
			p := prefixes[id%len(prefixes)]
			for i := 0; i < iters; i++ {
				tr.Add(p)
				tr.Remove(p)
			}
		}(g)
		// Reader: repeatedly Match / Size.
		go func(id int) {
			defer wg.Done()
			p := prefixes[(id+1)%len(prefixes)]
			for i := 0; i < iters; i++ {
				_ = tr.Match(p)
				_ = tr.Size()
			}
		}(g)
	}
	wg.Wait()

	// After balanced Add/Remove, tree should be empty.
	require.Equal(t, 0, tr.Size(), "size after balanced concurrent churn")
	for _, p := range prefixes {
		assert.False(t, tr.Match(p), "Match(%q) after churn = true, want false", p)
	}
}
