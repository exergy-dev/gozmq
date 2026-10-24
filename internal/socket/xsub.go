package socket

// NewXSubEngine returns a new XSUB socket engine.
//
// XSUB behaves like SUB on the inbound path (fair-queue across all
// attached peers + local subscription filter), but the subscription
// list is driven by the application sending synthetic single-frame
// subscribe/unsubscribe messages through Send rather than via explicit
// Subscribe/Unsubscribe API calls — matching libzmq's XSUB contract.
// The expected outbound wire shape is one frame whose first byte is
// 0x01 (SUBSCRIBE) or 0x00 (CANCEL) followed by the prefix bytes; any
// other message shape yields zmqerr.ErrFSMViolation. Inbound
// SUBSCRIBE/CANCEL commands from peers are dropped by the session (only
// PUB/XPUB consume them), as in libzmq; data frames reach Recv unchanged.
//
// Implementation: the same *SubEngine as NewSubEngine, with the
// surfacing flag set; Send is re-targeted to interpret the outbound
// message as a subscription command. Subscribe/Unsubscribe work too, and
// are how the socket layer applies WithSubscribe to an XSUB.
func NewXSubEngine() *SubEngine { return newSubEngineBase(true) }
