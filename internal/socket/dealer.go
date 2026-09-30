package socket

// NewDealerEngine returns a new DEALER socket engine.
//
// DEALER is an asynchronous request/reply peer — the symmetric counterpart
// to ROUTER, but without identity bookkeeping. It composes the two shared
// primitives:
//
//   - Send load-balances outbound messages round-robin across attached
//     peer pipes (reusing LoadBalancer; an HWM-full peer is rotated
//     past, blocking only when every peer is full).
//   - Recv fair-queues inbound messages across attached peer pipes
//     (reusing FairQueue).
//
// DEALER does not mangle identity envelopes or enforce a lock-step FSM:
// Send and Recv may interleave freely from multiple goroutines. Any
// REQ-style empty-delimiter framing required by a downstream ROUTER/REP
// peer is the application's responsibility.
//
// Thread-safe: Send, Recv, AttachPipe, DetachPipe, and Close are
// goroutine-safe via the underlying primitives.
func NewDealerEngine() Engine {
	return newQueuePairEngine()
}
