// ZAP (ZeroMQ Authentication Protocol) handler-side codec.
//
// RFC 27 (https://rfc.zeromq.org/spec/27) defines an inproc authorization
// protocol: a security mechanism, on the server side of a connection,
// issues a ZAP request to a ZAP handler socket bound to
// "inproc://zeromq.zap.01". The handler replies with a status code that
// determines whether the connection is accepted, along with an optional
// user-id and a metadata property list forwarded to the mechanism.
//
// Scope
// -----
//
// This package owns the ZAP policy type and the request/response codec:
//
//   ZAPHandler (the policy) and ZAPHandler.ServeRequest (request -> response)
//   EncodeZAPRequest, DecodeZAPRequest
//   EncodeZAPResponse, DecodeZAPResponse
//
// all as pure logic over [][]byte frames, testable without any socket.
// gozmq does not bind "inproc://zeromq.zap.01": a ZAPHandler installed with
// gozmq.WithZAPHandler is called directly, in the handshake goroutine, by
// the server side of PLAIN, CURVE, and (when a ZAP domain is set) NULL. The
// codec is exported so an application can also serve RFC 27 on a real
// socket, e.g. for libzmq peers in the same process.
//
// Wire format per RFC 27 §3
//
// Request (multi-frame):
//
//	frame 0: version    — ASCII "1.0"
//	frame 1: request-id — opaque; handler copies verbatim into response
//	frame 2: domain     — ASCII, set by server via WithZAPDomain
//	frame 3: address    — ASCII string representation of peer's network address
//	frame 4: identity   — ZMTP Identity metadata from peer, or empty
//	frame 5: mechanism  — "NULL" | "PLAIN" | "CURVE"
//	frame 6..: credentials — mechanism-specific (PLAIN: user, password;
//	                                             CURVE: 32-byte public key)
//
// Response (multi-frame):
//
//	frame 0: version     — "1.0"
//	frame 1: request-id  — copied from request
//	frame 2: status-code — "200" OK | "300" temp fail | "400" client error | "500" server error
//	frame 3: status-text — human-readable
//	frame 4: user-id     — ASCII user identifier on success, empty on failure
//	frame 5: metadata    — ZMTP 3.1 property-list bytes (may be empty)
//
// Status codes
// ------------
//
// The RFC uses HTTP-style three-digit codes. We expose them as typed
// constants to let callers switch on them cleanly.

package security

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
)

// ZAP protocol version token (RFC 27 §3.1).
const zapVersion = "1.0"

// ZAP status codes (RFC 27 §3.2).
const (
	ZAPStatusOK          = "200"
	ZAPStatusTempError   = "300"
	ZAPStatusClientError = "400"
	ZAPStatusServerError = "500"
)

// ZAPRequest is the decoded form of a RFC 27 §3.1 request. All frames are
// kept as []byte because the identity and credentials are opaque binary.
type ZAPRequest struct {
	Version     string // must be "1.0"
	RequestID   []byte // opaque, echoed in response
	Domain      string
	Address     string
	Identity    []byte
	Mechanism   string   // "NULL" | "PLAIN" | "CURVE"
	Credentials [][]byte // mechanism-specific
}

// ZAPResponse is the decoded form of a RFC 27 §3.2 response.
type ZAPResponse struct {
	Version    string
	RequestID  []byte
	StatusCode string
	StatusText string
	UserID     string
	Metadata   map[string]string
}

// ZAPHandler is an RFC 27 authorization policy. It is given the ZAP domain,
// the peer's address, the mechanism name, and that mechanism's credentials
// (PLAIN: username and password; CURVE: the client's 32-byte long-term public
// key; NULL: none), and accepts the peer by returning a nil error.
//
// gozmq.WithZAPHandler installs one on a Context, which then calls it
// directly from the server side of each handshake, concurrently when
// handshakes overlap; a rejection fails the handshake with an error that
// wraps both gozmq.ErrAuthFailed and the one returned here. On success a
// non-empty userID becomes the peer's "User-Id" property, and metadata adds
// properties under names the peer did not send itself.
//
// ServeRequest adapts the same function to the RFC 27 wire protocol, mapping
// its error to a status code:
//
//   - nil error                      -> 200 OK
//   - error wrapping ErrZAPTemporary -> 300 Temporary Error
//   - error wrapping ErrAuthFailed   -> 400 Client Error
//   - any other error                -> 500 Server Error
//
// so, for wire interop, a rejection should wrap gozmq.ErrAuthFailed.
type ZAPHandler func(domain, address, mech string, credentials [][]byte) (userID string, metadata map[string]string, err error)

// ErrZAPTemporary, when wrapped by a ZAPHandler error, marks the failure as
// temporary: a 300 response instead of 400/500. Wrap it via
// fmt.Errorf("...: %w", security.ErrZAPTemporary).
var ErrZAPTemporary = errors.New("gozmq/security/zap: temporary authentication failure")

// ServeRequest executes one request/response cycle. It parses the raw frames,
// invokes h, and serializes the reply. Malformed input produces a
// 500 response per RFC 27 §3.2 so the peer mechanism sees a terminating
// failure rather than a timeout.
//
// The frames slice ownership is the caller's; this function does not retain
// or mutate it.
func (h ZAPHandler) ServeRequest(reqFrames [][]byte) [][]byte {
	req, err := DecodeZAPRequest(reqFrames)
	if err != nil {
		// Build a minimal 500 response. We try to echo the request-id if
		// it was readable (frame index 1), otherwise emit an empty one.
		var reqID []byte
		if len(reqFrames) >= 2 {
			reqID = bytes.Clone(reqFrames[1])
		}
		return EncodeZAPResponse(&ZAPResponse{
			Version:    zapVersion,
			RequestID:  reqID,
			StatusCode: ZAPStatusServerError,
			StatusText: "malformed ZAP request",
		})
	}

	userID, metadata, perr := h(req.Domain, req.Address, req.Mechanism, req.Credentials)
	resp := &ZAPResponse{
		Version:   zapVersion,
		RequestID: bytes.Clone(req.RequestID),
	}
	switch {
	case perr == nil:
		resp.StatusCode = ZAPStatusOK
		resp.StatusText = "OK"
		resp.UserID = userID
		resp.Metadata = metadata
	case errors.Is(perr, ErrZAPTemporary):
		resp.StatusCode = ZAPStatusTempError
		resp.StatusText = perr.Error()
	case errors.Is(perr, zmqerr.ErrAuthFailed):
		resp.StatusCode = ZAPStatusClientError
		resp.StatusText = perr.Error()
	default:
		resp.StatusCode = ZAPStatusServerError
		resp.StatusText = perr.Error()
	}
	return EncodeZAPResponse(resp)
}

// EncodeZAPRequest serializes req into the RFC 27 §3.1 multi-frame form. The
// returned slice holds the request frames in order and aliases req.RequestID,
// req.Identity, and each req.Credentials[i] — callers must not mutate those
// byte slices while the encoded frames are in flight.
func EncodeZAPRequest(req *ZAPRequest) [][]byte {
	frames := make([][]byte, 0, 6+len(req.Credentials))
	version := req.Version
	if version == "" {
		version = zapVersion
	}
	frames = append(frames,
		[]byte(version),
		req.RequestID,
		[]byte(req.Domain),
		[]byte(req.Address),
		req.Identity,
		[]byte(req.Mechanism),
	)
	frames = append(frames, req.Credentials...)
	return frames
}

// DecodeZAPRequest parses a multi-frame ZAP request. The returned
// ZAPRequest's byte-slice fields reference the input frames — callers that
// need independent ownership must copy.
//
// Errors wrap gozmq.ErrMalformedFrame. We are strict about version: only
// "1.0" is accepted, matching libzmq's zap_handler_v2.
func DecodeZAPRequest(frames [][]byte) (*ZAPRequest, error) {
	if len(frames) < 6 {
		return nil, fmt.Errorf("gozmq/security/zap: request needs >=6 frames, got %d: %w", len(frames), zmqerr.ErrMalformedFrame)
	}
	version := string(frames[0])
	if version != zapVersion {
		return nil, fmt.Errorf("gozmq/security/zap: unsupported version %q: %w", version, zmqerr.ErrMalformedFrame)
	}
	req := &ZAPRequest{
		Version:   version,
		RequestID: frames[1],
		Domain:    string(frames[2]),
		Address:   string(frames[3]),
		Identity:  frames[4],
		Mechanism: string(frames[5]),
	}
	if len(frames) > 6 {
		req.Credentials = frames[6:]
	}
	// Mechanism-specific validation: PLAIN credentials must be exactly two
	// frames (username + password). CURVE must be exactly one 32-byte public
	// key. NULL must carry zero credential frames. Everything else is
	// rejected with ErrMalformedFrame so a misconfigured server trips a
	// deterministic error rather than crashing the policy.
	switch req.Mechanism {
	case "NULL":
		if len(req.Credentials) != 0 {
			return nil, fmt.Errorf("gozmq/security/zap: NULL expects 0 credentials, got %d: %w", len(req.Credentials), zmqerr.ErrMalformedFrame)
		}
	case "PLAIN":
		if len(req.Credentials) != 2 {
			return nil, fmt.Errorf("gozmq/security/zap: PLAIN expects 2 credentials (user, pass), got %d: %w", len(req.Credentials), zmqerr.ErrMalformedFrame)
		}
	case "CURVE":
		if len(req.Credentials) != 1 {
			return nil, fmt.Errorf("gozmq/security/zap: CURVE expects 1 credential (public key), got %d: %w", len(req.Credentials), zmqerr.ErrMalformedFrame)
		}
		if len(req.Credentials[0]) != 32 {
			return nil, fmt.Errorf("gozmq/security/zap: CURVE public key must be 32 bytes, got %d: %w", len(req.Credentials[0]), zmqerr.ErrMalformedFrame)
		}
	default:
		return nil, fmt.Errorf("gozmq/security/zap: unknown mechanism %q: %w", req.Mechanism, zmqerr.ErrMalformedFrame)
	}
	return req, nil
}

// EncodeZAPResponse serializes resp into RFC 27 §3.2 multi-frame form. A nil
// Metadata map is treated as empty (zero-length frame 5). The returned slice
// does NOT alias resp's fields — we allocate fresh frames because responses
// typically flow on a socket that will pool/retain them.
//
// Metadata a property list cannot carry (a name over 255 bytes, or a value
// over 2^32-1) is a policy bug; rather than panic inside a handshake, the
// response is replaced with a 500 carrying no user-id or metadata.
func EncodeZAPResponse(resp *ZAPResponse) [][]byte {
	version := resp.Version
	if version == "" {
		version = zapVersion
	}
	statusCode, statusText, userID, meta := resp.StatusCode, resp.StatusText, resp.UserID, resp.Metadata
	if err := validateZAPMetadata(meta); err != nil {
		statusCode, statusText, userID, meta = ZAPStatusServerError, err.Error(), "", nil
	}

	return [][]byte{
		[]byte(version),
		bytes.Clone(resp.RequestID),
		[]byte(statusCode),
		[]byte(statusText),
		[]byte(userID),
		zmtp.EncodeMetadata(meta),
	}
}

// validateZAPMetadata reports whether meta fits a ZMTP property list, whose
// encoder panics otherwise.
func validateZAPMetadata(meta map[string]string) error {
	for name, value := range meta {
		if len(name) > 255 {
			return fmt.Errorf("gozmq/security/zap: metadata name length %d exceeds 255", len(name))
		}
		if uint64(len(value)) > 0xFFFFFFFF {
			return fmt.Errorf("gozmq/security/zap: metadata %q value length %d exceeds 2^32-1", name, len(value))
		}
	}
	return nil
}

// DecodeZAPResponse parses a multi-frame ZAP response. Version mismatch or a
// short frame count produces ErrMalformedFrame.
func DecodeZAPResponse(frames [][]byte) (*ZAPResponse, error) {
	if len(frames) != 6 {
		return nil, fmt.Errorf("gozmq/security/zap: response needs 6 frames, got %d: %w", len(frames), zmqerr.ErrMalformedFrame)
	}
	version := string(frames[0])
	if version != zapVersion {
		return nil, fmt.Errorf("gozmq/security/zap: unsupported response version %q: %w", version, zmqerr.ErrMalformedFrame)
	}
	meta, err := zmtp.DecodeMetadata(frames[5])
	if err != nil {
		return nil, err // already wraps ErrMalformedFrame
	}
	return &ZAPResponse{
		Version:    version,
		RequestID:  frames[1],
		StatusCode: string(frames[2]),
		StatusText: string(frames[3]),
		UserID:     string(frames[4]),
		Metadata:   meta,
	}, nil
}
