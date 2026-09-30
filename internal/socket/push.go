package socket

// NewPushEngine returns a new PUSH socket engine.
//
// PUSH is send-only. Outbound messages are dispatched round-robin across
// the attached peer pipes using a shared LoadBalancer; a peer at HWM is
// rotated past (libzmq lb_t parity), and Send blocks only when EVERY
// attached peer is full — until one drains, ctx cancels, or the engine
// closes. Recv always returns zmqerr.ErrFSMViolation (wrapped); PUSH has
// no inbound path.
func NewPushEngine() Engine {
	return newSendOnlyEngine("push")
}
