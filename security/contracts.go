package security

// Role distinguishes the initiator (client, from Connect) from the acceptor
// (server, from Bind) within a single session. Some security mechanisms and
// socket patterns branch on role.
type Role uint8

const (
	// RoleClient is the side that initiated the connection (Connect).
	RoleClient Role = iota
	// RoleServer is the side that accepted the connection (Bind).
	RoleServer
)

// Mech drives a single handshake exchange and, for mechanisms that
// require it (CURVE), wraps/unwraps every subsequent frame body.
//
// The handshake is modeled as a lock-step NextHandshake loop. The session
// layer drives it:
//
//	out, done, err := mech.NextHandshake(nil)              // seed
//	for !done {
//	    write(out); in := read()
//	    out, done, err = mech.NextHandshake(in)
//	}
//
// For NULL the handshake is a single READY exchange; for CURVE it is the
// five-step HELLO/WELCOME/INITIATE/READY/MESSAGE sequence.
//
// Implementing this interface out of tree is supported but is a serious
// undertaking: a Mech sits directly on the wire, and a mistake in
// NextHandshake, EncodeFrame, or DecodeFrame is a security bug, not a
// correctness bug. Embed [MechBase] to inherit the metadata and ZAP
// plumbing, read the relevant RFC (23 for the ZMTP framing, 24 for PLAIN,
// 25/26 for CURVE, 27 for ZAP), and test against libzmq. Prefer the shipped
// mechanisms — NULL, PLAIN, CURVE — plus a [ZAPHandler] for authorization
// policy, which is the extension point most applications actually want.
//
// A mechanism whose EncodeFrame and DecodeFrame leave bodies and flags
// unchanged (as NULL and PLAIN do) may also implement
//
//	IdentityFraming() bool
//
// returning true; the session then skips both calls and uses its
// zero-copy framing path. Without it, EncodeFrame and DecodeFrame run on
// every traffic-phase frame.
type Mech interface {
	// Name is the greeting mechanism token (e.g. "NULL", "PLAIN", "CURVE"),
	// unpadded; the greeting encoder pads to 20 bytes.
	Name() string

	// AsServer reports the value of the greeting as-server byte.
	AsServer() bool

	// NextHandshake advances the handshake. On the first call, in is nil.
	// Returns the next outbound command (may be empty on final step) and
	// done=true when the handshake is complete.
	NextHandshake(in []byte) (out []byte, done bool, err error)

	// EncodeFrame rewrites a plaintext frame body and its ZMTP flags into
	// their wire form. NULL/PLAIN preserve both body and flags. CURVE, as
	// libzmq does, moves MORE and COMMAND into the encrypted MESSAGE inner
	// flags byte and clears them from the wire flags.
	//
	// Ownership: the returned wire is either plain itself or memory the
	// mechanism neither reuses nor modifies after returning. The session
	// hands it to the kernel by reference, possibly after encoding later
	// frames. A mechanism that would otherwise recycle an output buffer
	// should implement [FrameAppender] instead.
	EncodeFrame(flags uint8, plain []byte) (wireFlags uint8, wire []byte, err error)

	// DecodeFrame reverses EncodeFrame. The returned flags are the logical
	// post-decode frame flags seen by the session layer. A mechanism that
	// authenticates frames must derive them from authenticated data only
	// (CURVE ignores the outer wire flags, which are not under the MAC, as
	// libzmq does). Returns ErrMalformedFrame on MAC failure.
	//
	// Ownership: the returned plain is either wire itself or memory the
	// mechanism neither reuses nor modifies after returning; in the latter
	// case the session delivers it to the application without a copy.
	DecodeFrame(flags uint8, wire []byte) (plainFlags uint8, plain []byte, err error)

	// Metadata returns local properties sent in READY. Must return after
	// the handshake is seeded.
	Metadata() map[string]string

	// PeerMetadata returns the properties the peer advertised during the
	// handshake. The session layer reads it once NextHandshake reports done,
	// to check Socket-Type compatibility and learn the peer's Identity; a
	// nil map at that point fails the session.
	PeerMetadata() map[string]string
}

// FrameAppender is optionally implemented by a mechanism whose EncodeFrame
// produces new bytes (CURVE does). AppendEncodeFrame appends the wire form
// of plain to dst and returns the grown slice, so the session seals each
// frame straight into its write buffer with no allocation per frame. It
// must produce exactly the bytes and flags EncodeFrame would.
type FrameAppender interface {
	AppendEncodeFrame(dst []byte, flags uint8, plain []byte) (wire []byte, wireFlags uint8, err error)
}
