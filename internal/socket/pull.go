package socket

import "github.com/exergy-dev/gozmq/internal/iface"

// NewPullEngine returns a new PULL socket engine.
//
// PULL is recv-only. Inbound messages are fair-queued across the attached
// peer pipes by a shared FairQueue: each pipe gets a forwarder goroutine
// that pumps its Recv into a single channel, and PULL.Recv returns the
// next message from whichever peer produced one first. Recv blocks until
// some peer has a message, ctx cancels, or the engine closes. Send always
// returns zmqerr.ErrFSMViolation (wrapped); PULL has no outbound path.
func NewPullEngine() iface.SocketEngine {
	return newRecvOnlyEngine("pull")
}
