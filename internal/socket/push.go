package socket

import "github.com/rkm/gozmq/internal/iface"

// NewPushEngine returns a new PUSH socket engine.
//
// PUSH is send-only. Outbound messages are dispatched round-robin across
// the attached peer pipes using a shared LoadBalancer; if the chosen peer
// is at HWM, Send blocks (strict cursor — PUSH does not rotate past a
// full peer) until the peer drains, ctx cancels, or the engine closes.
// Recv always returns zmqerr.ErrFSMViolation (wrapped); PUSH has no
// inbound path.
func NewPushEngine() iface.SocketEngine {
	return newSendOnlyEngine("push")
}
