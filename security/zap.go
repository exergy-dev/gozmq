// ZAP (ZeroMQ Authentication Protocol) handler-side codec.
//
// RFC 27 (https://rfc.zeromq.org/spec/27) defines an inproc authorization
// protocol: a security mechanism (PLAIN or CURVE), on the server side of a
// connection, issues a ZAP request to a user-supplied ZAP handler socket
// bound to "inproc://zeromq.zap.01". The handler replies with a status code
// that determines whether the connection is accepted, along with an optional
// user-id and a metadata property list forwarded to the mechanism.
//
// Scope
// -----
//
// This package owns the ZAP request/response codec and the handler policy
// interface:
//
//   EncodeZapRequest, DecodeZapRequest
//   EncodeZapResponse, DecodeZapResponse
//   ZapHandler.ServeRequest (pure: request -> response)
//
// all as pure logic over [][]byte frames, testable without any socket. The
// ZAP handler itself is a user-supplied inproc socket (typically REP-style)
// bound to "inproc://zeromq.zap.01" on the Context; the authenticatee side
// of a security mechanism consults it during the handshake by routing
// frames through ServeRequest. The CURVE integration invokes the handler
// via that inproc endpoint.
//
// Wire format per RFC 27 §3
// -------------------------
//
// Request (multi-frame):
//
//	frame 0: version    — ASCII "1.0"
//	frame 1: request-id — opaque; handler copies verbatim into response
//	frame 2: domain     — ASCII, set by server via WithZapDomain
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
	"errors"
	"fmt"

	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/rkm/gozmq/internal/zmtp"
)

// ZAP protocol version token (RFC 27 §3.1).
const zapVersion = "1.0"

// ZAP status codes (RFC 27 §3.2).
const (
	ZapStatusOK          = "200"
	ZapStatusTempError   = "300"
	ZapStatusClientError = "400"
	ZapStatusServerError = "500"
)

// ZapRequest is the decoded form of a RFC 27 §3.1 request. All frames are
// kept as []byte because the identity and credentials are opaque binary.
type ZapRequest struct {
	Version     string // must be "1.0"
	RequestID   []byte // opaque, echoed in response
	Domain      string
	Address     string
	Identity    []byte
	Mechanism   string   // "NULL" | "PLAIN" | "CURVE"
	Credentials [][]byte // mechanism-specific
}

// ZapResponse is the decoded form of a RFC 27 §3.2 response.
type ZapResponse struct {
	Version    string
	RequestID  []byte
	StatusCode string
	StatusText string
	UserID     string
	Metadata   map[string]string
}

// ZapPolicy is the user-supplied authorizer. Implementations make an
// authorization decision based on the mechanism-specific credentials and
// return either a user-id + optional metadata (success) or an error
// (failure). ServeRequest translates the error into a ZAP status code:
//
//   - nil error                      -> 200 OK
//   - error wrapping ErrAuthFailed   -> 400 Client Error
//   - any other error                -> 500 Server Error
//
// A policy that wants to signal a temporary failure (300) can wrap
// ErrZapTemporary (declared below) in its returned error. This isolates
// policy code from the ZAP wire code.
type ZapPolicy interface {
	Authenticate(domain, address, mech string, credentials [][]byte) (userID string, metadata map[string]string, err error)
}

// ErrZapTemporary, when wrapped by a ZapPolicy.Authenticate error, produces a
// 300 "temporary error" response instead of 400/500. Callers should wrap it
// via fmt.Errorf("...: %w", security.ErrZapTemporary).
var ErrZapTemporary = errors.New("gozmq/security/zap: temporary authentication failure")

// ZapHandler is the handler-side runtime for request/response ZAP policy.
type ZapHandler struct {
	policy ZapPolicy
}

// NewZapHandler returns a ZapHandler that dispatches incoming requests to
// policy. A nil policy is programmer error.
func NewZapHandler(policy ZapPolicy) *ZapHandler {
	if policy == nil {
		panic("gozmq/security/zap: NewZapHandler requires a non-nil policy")
	}
	return &ZapHandler{policy: policy}
}

// Policy returns the installed ZapPolicy. Exposed for tests and diagnostics.
func (h *ZapHandler) Policy() ZapPolicy { return h.policy }

// ServeRequest executes one request/response cycle. It parses the raw frames,
// invokes the policy, and serializes the reply. Malformed input produces a
// 500 response per RFC 27 §3.2 so the peer mechanism sees a terminating
// failure rather than a timeout.
//
// The frames slice ownership is the caller's; this function does not retain
// or mutate it.
func (h *ZapHandler) ServeRequest(reqFrames [][]byte) [][]byte {
	req, err := DecodeZapRequest(reqFrames)
	if err != nil {
		// Build a minimal 500 response. We try to echo the request-id if
		// it was readable (frame index 1), otherwise emit an empty one.
		var reqID []byte
		if len(reqFrames) >= 2 {
			reqID = append([]byte(nil), reqFrames[1]...)
		}
		return EncodeZapResponse(&ZapResponse{
			Version:    zapVersion,
			RequestID:  reqID,
			StatusCode: ZapStatusServerError,
			StatusText: "malformed ZAP request",
		})
	}

	userID, metadata, perr := h.policy.Authenticate(req.Domain, req.Address, req.Mechanism, req.Credentials)
	resp := &ZapResponse{
		Version:   zapVersion,
		RequestID: append([]byte(nil), req.RequestID...),
	}
	switch {
	case perr == nil:
		resp.StatusCode = ZapStatusOK
		resp.StatusText = "OK"
		resp.UserID = userID
		resp.Metadata = metadata
	case errors.Is(perr, ErrZapTemporary):
		resp.StatusCode = ZapStatusTempError
		resp.StatusText = perr.Error()
	case errors.Is(perr, zmqerr.ErrAuthFailed):
		resp.StatusCode = ZapStatusClientError
		resp.StatusText = perr.Error()
	default:
		resp.StatusCode = ZapStatusServerError
		resp.StatusText = perr.Error()
	}
	return EncodeZapResponse(resp)
}

// ---------------------------------------------------------------------------
// Codec — request
// ---------------------------------------------------------------------------

// EncodeZapRequest serializes req into the RFC 27 §3.1 multi-frame form. The
// returned slice holds the request frames in order and aliases req.RequestID,
// req.Identity, and each req.Credentials[i] — callers must not mutate those
// byte slices while the encoded frames are in flight.
func EncodeZapRequest(req *ZapRequest) [][]byte {
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
	for _, cred := range req.Credentials {
		frames = append(frames, cred)
	}
	return frames
}

// DecodeZapRequest parses a multi-frame ZAP request. The returned
// ZapRequest's byte-slice fields reference the input frames — callers that
// need independent ownership must copy.
//
// Errors wrap zmqerr.ErrMalformedFrame. We are strict about version: only
// "1.0" is accepted, matching libzmq's zap_handler_v2.
func DecodeZapRequest(frames [][]byte) (*ZapRequest, error) {
	if len(frames) < 6 {
		return nil, fmt.Errorf("gozmq/security/zap: request needs >=6 frames, got %d: %w", len(frames), zmqerr.ErrMalformedFrame)
	}
	version := string(frames[0])
	if version != zapVersion {
		return nil, fmt.Errorf("gozmq/security/zap: unsupported version %q: %w", version, zmqerr.ErrMalformedFrame)
	}
	req := &ZapRequest{
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

// ---------------------------------------------------------------------------
// Codec — response
// ---------------------------------------------------------------------------

// EncodeZapResponse serializes resp into RFC 27 §3.2 multi-frame form. A nil
// Metadata map is treated as empty (zero-length frame 5). The returned slice
// does NOT alias resp's fields — we allocate fresh frames because responses
// typically flow on a socket that will pool/retain them.
func EncodeZapResponse(resp *ZapResponse) [][]byte {
	version := resp.Version
	if version == "" {
		version = zapVersion
	}
	metadataBytes := zmtp.MetadataCodec.Encode(resp.Metadata)

	return [][]byte{
		append([]byte(nil), []byte(version)...),
		append([]byte(nil), resp.RequestID...),
		append([]byte(nil), []byte(resp.StatusCode)...),
		append([]byte(nil), []byte(resp.StatusText)...),
		append([]byte(nil), []byte(resp.UserID)...),
		metadataBytes,
	}
}

// DecodeZapResponse parses a multi-frame ZAP response. Version mismatch or a
// short frame count produces ErrMalformedFrame.
func DecodeZapResponse(frames [][]byte) (*ZapResponse, error) {
	if len(frames) != 6 {
		return nil, fmt.Errorf("gozmq/security/zap: response needs 6 frames, got %d: %w", len(frames), zmqerr.ErrMalformedFrame)
	}
	version := string(frames[0])
	if version != zapVersion {
		return nil, fmt.Errorf("gozmq/security/zap: unsupported response version %q: %w", version, zmqerr.ErrMalformedFrame)
	}
	meta, err := zmtp.MetadataCodec.Decode(frames[5])
	if err != nil {
		return nil, err // already wraps ErrMalformedFrame
	}
	return &ZapResponse{
		Version:    version,
		RequestID:  frames[1],
		StatusCode: string(frames[2]),
		StatusText: string(frames[3]),
		UserID:     string(frames[4]),
		Metadata:   meta,
	}, nil
}
