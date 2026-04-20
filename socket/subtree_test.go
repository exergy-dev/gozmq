package socket_test

import (
	"sync"
	"testing"

	"github.com/rkm/gozmq/socket"
)

func TestSubTree_EmptyMatchesNothing(t *testing.T) {
	t.Parallel()
	tr := socket.NewSubTree()
	if tr.Size() != 0 {
		t.Fatalf("fresh tree size = %d, want 0", tr.Size())
	}
	cases := [][]byte{
		nil, {}, []byte("x"), []byte("foo"), []byte{0x00},
	}
	for _, c := range cases {
		if tr.Match(c) {
			t.Errorf("empty tree matched %q", c)
		}
	}
}

func TestSubTree_EmptyPrefixMatchesEverything(t *testing.T) {
	t.Parallel()
	tr := socket.NewSubTree()
	tr.Add([]byte{})
	if tr.Size() != 1 {
		t.Fatalf("size after Add(empty) = %d, want 1", tr.Size())
	}
	cases := [][]byte{
		nil, {}, []byte("x"), []byte("foo"), []byte{0x00, 0xff},
	}
	for _, c := range cases {
		if !tr.Match(c) {
			t.Errorf("empty-prefix tree did not match %q", c)
		}
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
		if got := tr.Match(tc.topic); got != tc.want {
			t.Errorf("Match(%q) = %v, want %v", tc.topic, got, tc.want)
		}
	}
}

func TestSubTree_MultipleDistinctPrefixes(t *testing.T) {
	t.Parallel()
	tr := socket.NewSubTree()
	tr.Add([]byte("foo"))
	tr.Add([]byte("bar"))
	tr.Add([]byte("fooX"))

	if tr.Size() != 3 {
		t.Fatalf("size = %d, want 3", tr.Size())
	}
	for _, topic := range []string{"foo", "foobar", "fooX", "fooXy", "bar", "barbaz"} {
		if !tr.Match([]byte(topic)) {
			t.Errorf("Match(%q) = false, want true", topic)
		}
	}
	for _, topic := range []string{"", "f", "fo", "ba", "baz"} {
		if tr.Match([]byte(topic)) {
			t.Errorf("Match(%q) = true, want false", topic)
		}
	}
}

func TestSubTree_RefcountAddRemoveParity(t *testing.T) {
	t.Parallel()
	tr := socket.NewSubTree()

	tr.Add([]byte("foo"))
	tr.Add([]byte("foo"))
	if tr.Size() != 1 {
		t.Fatalf("size after two duplicate Adds = %d, want 1", tr.Size())
	}
	if !tr.Match([]byte("foobar")) {
		t.Fatal("Match after two adds = false, want true")
	}
	// First Remove decrements refcount but the prefix is still present.
	if rem := tr.Remove([]byte("foo")); rem {
		t.Fatal("first Remove returned true, want false (refcount still >0)")
	}
	if !tr.Match([]byte("foobar")) {
		t.Fatal("Match after first Remove = false, want true")
	}
	if tr.Size() != 1 {
		t.Fatalf("size after first Remove = %d, want 1", tr.Size())
	}
	// Second Remove actually removes it.
	if rem := tr.Remove([]byte("foo")); !rem {
		t.Fatal("second Remove returned false, want true")
	}
	if tr.Match([]byte("foobar")) {
		t.Fatal("Match after second Remove = true, want false")
	}
	if tr.Size() != 0 {
		t.Fatalf("size after second Remove = %d, want 0", tr.Size())
	}
	// Removing an absent prefix is a no-op.
	if rem := tr.Remove([]byte("foo")); rem {
		t.Fatal("Remove on absent prefix returned true, want false")
	}
	if rem := tr.Remove([]byte("never-added")); rem {
		t.Fatal("Remove on never-added prefix returned true, want false")
	}
}

func TestSubTree_PartialPathPruning(t *testing.T) {
	t.Parallel()
	tr := socket.NewSubTree()
	// Add foo and foobar; remove foobar should not disturb foo.
	tr.Add([]byte("foo"))
	tr.Add([]byte("foobar"))
	if tr.Size() != 2 {
		t.Fatalf("size = %d, want 2", tr.Size())
	}
	if !tr.Remove([]byte("foobar")) {
		t.Fatal("Remove(foobar) returned false")
	}
	if tr.Size() != 1 {
		t.Fatalf("size after remove foobar = %d, want 1", tr.Size())
	}
	if !tr.Match([]byte("foo")) {
		t.Fatal("Match(foo) = false, want true")
	}
	if !tr.Match([]byte("foobar")) {
		t.Fatal("Match(foobar) after removing foobar-sub but keeping foo-sub = false, want true (foo is still a prefix)")
	}
	if !tr.Remove([]byte("foo")) {
		t.Fatal("Remove(foo) returned false")
	}
	if tr.Match([]byte("foo")) {
		t.Fatal("Match(foo) after removing both = true, want false")
	}
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
	if tr.Size() != 0 {
		t.Fatalf("size after balanced concurrent churn = %d, want 0", tr.Size())
	}
	for _, p := range prefixes {
		if tr.Match(p) {
			t.Errorf("Match(%q) after churn = true, want false", p)
		}
	}
}
