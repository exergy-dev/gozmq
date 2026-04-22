package socket

import (
	"github.com/rkm/gozmq/internal/iface"
)

// NewXSubEngine returns a new XSUB socket engine.
//
// XSUB behaves like SUB on the inbound path (fair-queue across all
// attached peers + local subscription filter), but the subscription
// list is driven by the application sending synthetic single-frame
// subscribe/unsubscribe messages through Send rather than via explicit
// Subscribe/Unsubscribe API calls — matching libzmq's XSUB contract.
// The expected outbound wire shape is one frame whose first byte is
// 0x01 (SUBSCRIBE) or 0x00 (CANCEL) followed by the prefix bytes; any
// other message shape yields zmqerr.ErrFSMViolation. Incoming
// SUBSCRIBE/CANCEL messages from peers (typically XPUB sources on the
// reverse path) are surfaced to the application via Recv.
//
// Implementation: the same *SubEngine as NewSubEngine, with the
// surfacing flag set; Send is re-targeted to interpret the outbound
// message as a subscription command. Subscribe/Unsubscribe remain
// callable on the returned *SubEngine for tests that want to drive the
// filter via the API path rather than through synthetic wire messages.
func NewXSubEngine() (iface.SocketEngine, *SubEngine) {
	e := newSubEngineBase(true)
	return e, e
}
