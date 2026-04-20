// Package metrics publishes per-socket operational counters via the
// standard library's expvar mechanism, so any process that imports gozmq
// automatically surfaces its socket-level metrics at the default
// /debug/vars endpoint (assuming net/http/pprof or expvar.Handler is
// mounted by the host application).
//
// Each SocketMetrics is a composite registered under the key
// "gozmq.<label>" as an *expvar.Map. Within that map we publish per-
// counter entries by stable name (MessagesSent, BytesSent, …). Using
// an expvar.Map rather than a flat set of package-level *expvar.Int
// lets many sockets coexist in the same process without polluting the
// top-level expvar namespace and without relying on mutable globals.
//
// Label uniqueness: callers pick the label (commonly a pattern name
// plus an endpoint). If a collision occurs — expvar.Publish panics on
// duplicate keys — we fall back to "<label>.<ptrhash>" where the hash
// is derived from the returned struct's address. This guarantees
// forward progress without requiring callers to coordinate labels.
package metrics

import (
	"expvar"
	"fmt"
	"sync"
	"unsafe"
)

// SocketMetrics bundles every per-socket counter gozmq surfaces. All
// fields are *expvar.Int so the zero value is safe and Add/Set are
// atomic.
//
// Counters chosen per design §12:
//   - Message-level: MessagesSent, MessagesReceived.
//   - Byte-level: BytesSent, BytesReceived.
//   - Handshake outcomes: HandshakesSucceeded, HandshakesFailed.
//   - Connection churn: Reconnects.
//   - Backpressure: HWMHits (times a pipe refused/stalled on HWM).
//   - Observability health: MonitorChanDropped (events discarded
//     because the user's monitor channel was full).
type SocketMetrics struct {
	Label string
	Root  *expvar.Map

	MessagesSent       *expvar.Int
	MessagesReceived   *expvar.Int
	BytesSent          *expvar.Int
	BytesReceived      *expvar.Int
	HandshakesSucceeded *expvar.Int
	HandshakesFailed   *expvar.Int
	Reconnects         *expvar.Int
	HWMHits            *expvar.Int
	MonitorChanDropped *expvar.Int
}

// publishMu serialises Publish/Get around the narrow window where we
// check-then-publish a label. expvar itself is concurrency-safe, but
// it panics on duplicate Publish — we need an atomic check so two
// sockets created in parallel under the same label don't race to
// Publish and crash the process.
var publishMu sync.Mutex

// NewSocketMetrics constructs a SocketMetrics and registers it under
// "gozmq.<label>" via expvar.Publish. If that key is already in use
// (another socket published the same label earlier), we fall back to
// "gozmq.<label>.<ptrhash>" and retry. The returned struct always
// owns a freshly-published expvar.Map.
//
// Caller responsibility: pick a reasonably unique label. Reusing a
// single label for thousands of sockets will still succeed (the ptr-
// hash fallback ensures uniqueness) but the resulting /debug/vars
// namespace will be harder to read.
func NewSocketMetrics(socketLabel string) *SocketMetrics {
	m := &expvar.Map{}
	m.Init()

	sm := &SocketMetrics{
		Root:                m,
		MessagesSent:        new(expvar.Int),
		MessagesReceived:    new(expvar.Int),
		BytesSent:           new(expvar.Int),
		BytesReceived:       new(expvar.Int),
		HandshakesSucceeded: new(expvar.Int),
		HandshakesFailed:    new(expvar.Int),
		Reconnects:          new(expvar.Int),
		HWMHits:             new(expvar.Int),
		MonitorChanDropped:  new(expvar.Int),
	}

	m.Set("MessagesSent", sm.MessagesSent)
	m.Set("MessagesReceived", sm.MessagesReceived)
	m.Set("BytesSent", sm.BytesSent)
	m.Set("BytesReceived", sm.BytesReceived)
	m.Set("HandshakesSucceeded", sm.HandshakesSucceeded)
	m.Set("HandshakesFailed", sm.HandshakesFailed)
	m.Set("Reconnects", sm.Reconnects)
	m.Set("HWMHits", sm.HWMHits)
	m.Set("MonitorChanDropped", sm.MonitorChanDropped)

	// Attempt to publish under the preferred key; on collision fall
	// back to a pointer-hash suffix. We derive the hash from sm's
	// address, which is unique for the process lifetime of this
	// allocation — guaranteed non-colliding even if many sockets
	// share one label.
	key := "gozmq." + socketLabel
	publishMu.Lock()
	defer publishMu.Unlock()
	if expvar.Get(key) != nil {
		tag := ptrHash(sm)
		key = fmt.Sprintf("gozmq.%s.%x", socketLabel, tag)
		// In the astronomically-unlikely event of a second
		// collision (same process address reused within one run),
		// keep suffixing until we find a free slot.
		for i := 1; expvar.Get(key) != nil; i++ {
			key = fmt.Sprintf("gozmq.%s.%x.%d", socketLabel, tag, i)
		}
	}
	expvar.Publish(key, m)
	sm.Label = key
	return sm
}

// ptrHash returns a non-cryptographic tag derived from p's address,
// suitable only for disambiguating duplicate labels in the expvar
// namespace. The pointer is not dereferenced.
func ptrHash(p *SocketMetrics) uintptr {
	return uintptr(unsafe.Pointer(p))
}
