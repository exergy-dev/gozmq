package socket

// NewPullEngine returns a new PULL socket engine.
//
// PULL is recv-only. Inbound messages are fair-queued across the attached
// peer pipes by a FairQueue, which Recv reads round-robin. Recv blocks
// until some peer has a message, ctx cancels, or the engine closes. Send always
// returns zmqerr.ErrFSMViolation (wrapped); PULL has no outbound path.
func NewPullEngine() Engine {
	return newRecvOnlyEngine("pull")
}
