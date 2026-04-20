package metrics

import (
	"expvar"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
)

// uniqueLabel returns a process-globally-unique test label. Needed
// because expvar.Publish is a permanent registration: running the
// same test twice (e.g. `go test -count=2`) would otherwise see the
// second call fall through to the collision-fallback path and the
// primary-key assertions would fail.
var labelSeq atomic.Int64

func uniqueLabel(t *testing.T, suffix string) string {
	return fmt.Sprintf("%s.%d.%s", t.Name(), labelSeq.Add(1), suffix)
}

// TestNewSocketMetrics_RegistersUnderLabel asserts the constructor
// publishes its *expvar.Map under the expected "gozmq.<label>" key
// and that every counter is reachable from the map.
func TestNewSocketMetrics_RegistersUnderLabel(t *testing.T) {
	label := uniqueLabel(t, "primary")
	sm := NewSocketMetrics(label)

	want := "gozmq." + label
	if sm.Label != want {
		t.Errorf("Label = %q, want %q", sm.Label, want)
	}
	got := expvar.Get(want)
	if got == nil {
		t.Fatalf("expvar.Get(%q) returned nil; publish did not happen", want)
	}
	m, ok := got.(*expvar.Map)
	if !ok {
		t.Fatalf("expvar.Get(%q) = %T, want *expvar.Map", want, got)
	}
	// Every counter the struct exposes must be reachable under its
	// field name — these names are the public API surface consumed
	// by monitoring tools.
	for _, name := range []string{
		"MessagesSent", "MessagesReceived",
		"BytesSent", "BytesReceived",
		"HandshakesSucceeded", "HandshakesFailed",
		"Reconnects", "HWMHits", "MonitorChanDropped",
	} {
		if v := m.Get(name); v == nil {
			t.Errorf("map entry %q missing", name)
		}
	}
}

// TestSocketMetrics_CountersAreAtomic exercises Add on a few
// representative counters and reads them back via expvar.Get, which
// is what external consumers do.
func TestSocketMetrics_CountersAreAtomic(t *testing.T) {
	sm := NewSocketMetrics(uniqueLabel(t, "counters"))
	sm.MessagesSent.Add(3)
	sm.BytesReceived.Add(42)
	sm.MonitorChanDropped.Add(7)

	root := expvar.Get(sm.Label).(*expvar.Map)
	if got := root.Get("MessagesSent").String(); got != "3" {
		t.Errorf("MessagesSent = %s, want 3", got)
	}
	if got := root.Get("BytesReceived").String(); got != "42" {
		t.Errorf("BytesReceived = %s, want 42", got)
	}
	if got := root.Get("MonitorChanDropped").String(); got != "7" {
		t.Errorf("MonitorChanDropped = %s, want 7", got)
	}
}

// TestNewSocketMetrics_CollisionFallback: two calls with the same
// label must both succeed. expvar.Publish would panic on a duplicate
// key; we rely on the ptr-hash fallback to give the second instance
// a distinct name.
func TestNewSocketMetrics_CollisionFallback(t *testing.T) {
	label := uniqueLabel(t, "collide")

	sm1 := NewSocketMetrics(label)
	sm2 := NewSocketMetrics(label)

	if sm1.Label == sm2.Label {
		t.Fatalf("collision not resolved: both labels = %q", sm1.Label)
	}
	if sm1.Label != "gozmq."+label {
		t.Errorf("sm1.Label = %q, want primary key %q", sm1.Label, "gozmq."+label)
	}
	if !strings.HasPrefix(sm2.Label, "gozmq."+label+".") {
		t.Errorf("sm2.Label = %q, want fallback prefix %q.", sm2.Label, "gozmq."+label+".")
	}

	// Mutate each independently and confirm they're separate Maps.
	sm1.MessagesSent.Add(1)
	sm2.MessagesSent.Add(100)

	if sm1.MessagesSent.Value() != 1 {
		t.Errorf("sm1.MessagesSent = %d, want 1", sm1.MessagesSent.Value())
	}
	if sm2.MessagesSent.Value() != 100 {
		t.Errorf("sm2.MessagesSent = %d, want 100", sm2.MessagesSent.Value())
	}
}

// TestNewSocketMetrics_MultipleDistinctLabels: the common case —
// different labels publish under distinct top-level keys without
// triggering the fallback path.
func TestNewSocketMetrics_MultipleDistinctLabels(t *testing.T) {
	labelA := uniqueLabel(t, "a")
	labelB := uniqueLabel(t, "b")
	sm1 := NewSocketMetrics(labelA)
	sm2 := NewSocketMetrics(labelB)

	if sm1.Label == sm2.Label {
		t.Fatalf("distinct labels produced same key: %q", sm1.Label)
	}
	if sm1.Label != "gozmq."+labelA {
		t.Errorf("sm1.Label = %q; fallback triggered for a unique label", sm1.Label)
	}
	if sm2.Label != "gozmq."+labelB {
		t.Errorf("sm2.Label = %q; fallback triggered for a unique label", sm2.Label)
	}
}
