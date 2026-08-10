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

// ZAPAuthorizer is the server-side authorization hook for mechanisms that
// delegate policy to RFC 27 ZAP. On success it returns a user-id plus
// optional metadata to merge into the peer properties. On rejection it
// should return an error wrapping gozmq.ErrAuthFailed.
type ZAPAuthorizer func(domain, address, mech string, credentials [][]byte) (userID string, metadata map[string]string, err error)

// ZapConfigurable is an optional extension implemented by security
// mechanisms that support server-side ZAP authorization. The session layer
// type-asserts for it and, when present, installs the Context's ZAP handler
// before driving the handshake.
type ZapConfigurable interface {
	ConfigureZAP(authorizer ZAPAuthorizer, domain, address string)
}

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
// mechanisms — NULL, PLAIN, CURVE — plus a [ZapPolicy] for authorization
// policy, which is the extension point most applications actually want.
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
	// their wire form. NULL/PLAIN preserve both body and flags. CURVE moves
	// multipart state into the encrypted MESSAGE inner flags byte while
	// preserving outer flags such as COMMAND.
	EncodeFrame(flags uint8, plain []byte) (wireFlags uint8, wire []byte, err error)

	// DecodeFrame reverses EncodeFrame. The returned flags are the logical
	// post-decode frame flags seen by the session layer. Returns
	// ErrMalformedFrame on MAC failure.
	DecodeFrame(flags uint8, wire []byte) (plainFlags uint8, plain []byte, err error)

	// Metadata returns local properties sent in READY. Must return after
	// the handshake is seeded.
	Metadata() map[string]string

	// SetPeerMetadata is called by the session layer once the peer's READY
	// has been parsed. Mechanisms may reject (return error) on mismatch
	// (e.g. Socket-Type incompatibility).
	SetPeerMetadata(props map[string]string) error
}
