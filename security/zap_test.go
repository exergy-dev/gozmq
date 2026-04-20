package security

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/rkm/gozmq/internal/zmqerr"
)

// mockPolicy lets tests script the policy response. It can return OK, an
// ErrAuthFailed-wrapping error, a server error, or a temporary-failure error.
type mockPolicy struct {
	userID   string
	metadata map[string]string
	err      error

	// observed is populated on each Authenticate call so tests can assert
	// on what the handler passed through.
	lastDomain    string
	lastAddress   string
	lastMechanism string
	lastCreds     [][]byte
}

func (p *mockPolicy) Authenticate(domain, address, mech string, creds [][]byte) (string, map[string]string, error) {
	p.lastDomain = domain
	p.lastAddress = address
	p.lastMechanism = mech
	p.lastCreds = creds
	return p.userID, p.metadata, p.err
}

// ---------------------------------------------------------------------------
// Codec round-trip tests
// ---------------------------------------------------------------------------

func TestZapRequestRoundTrip(t *testing.T) {
	req := &ZapRequest{
		Version:     "1.0",
		RequestID:   []byte{0x01, 0x02, 0x03},
		Domain:      "global",
		Address:     "127.0.0.1:5555",
		Identity:    []byte("peer-id"),
		Mechanism:   "PLAIN",
		Credentials: [][]byte{[]byte("alice"), []byte("hunter2")},
	}
	frames := EncodeZapRequest(req)
	if len(frames) != 8 {
		t.Fatalf("frames len: got %d, want 8", len(frames))
	}
	got, err := DecodeZapRequest(frames)
	if err != nil {
		t.Fatalf("DecodeZapRequest: %v", err)
	}
	if got.Version != req.Version || got.Domain != req.Domain || got.Address != req.Address || got.Mechanism != req.Mechanism {
		t.Fatalf("scalar fields mismatch: got %+v, want %+v", got, req)
	}
	if !bytes.Equal(got.RequestID, req.RequestID) {
		t.Fatalf("RequestID: got %x, want %x", got.RequestID, req.RequestID)
	}
	if !bytes.Equal(got.Identity, req.Identity) {
		t.Fatalf("Identity: got %x, want %x", got.Identity, req.Identity)
	}
	if len(got.Credentials) != 2 || !bytes.Equal(got.Credentials[0], []byte("alice")) || !bytes.Equal(got.Credentials[1], []byte("hunter2")) {
		t.Fatalf("Credentials mismatch: %v", got.Credentials)
	}
}

func TestZapResponseRoundTrip(t *testing.T) {
	resp := &ZapResponse{
		Version:    "1.0",
		RequestID:  []byte{0x10, 0x20},
		StatusCode: "200",
		StatusText: "OK",
		UserID:     "alice",
		Metadata:   map[string]string{"Role": "admin"},
	}
	frames := EncodeZapResponse(resp)
	if len(frames) != 6 {
		t.Fatalf("frames len: got %d, want 6", len(frames))
	}
	got, err := DecodeZapResponse(frames)
	if err != nil {
		t.Fatalf("DecodeZapResponse: %v", err)
	}
	if got.Version != resp.Version || got.StatusCode != resp.StatusCode || got.StatusText != resp.StatusText || got.UserID != resp.UserID {
		t.Fatalf("scalar mismatch: got %+v want %+v", got, resp)
	}
	if !bytes.Equal(got.RequestID, resp.RequestID) {
		t.Fatalf("RequestID: got %x want %x", got.RequestID, resp.RequestID)
	}
	if got.Metadata["Role"] != "admin" {
		t.Fatalf("Metadata: got %v", got.Metadata)
	}
}

func TestZapResponseEncodeIsolatesInput(t *testing.T) {
	// EncodeZapResponse must not alias resp's fields — callers commonly mutate
	// the scratch buffer after handing it off.
	reqID := []byte{0x01, 0x02}
	resp := &ZapResponse{
		RequestID:  reqID,
		StatusCode: "200",
		StatusText: "OK",
		UserID:     "u",
	}
	frames := EncodeZapResponse(resp)
	// Mutate caller's RequestID; the frame in `frames` must be unchanged.
	reqID[0] = 0xff
	if frames[1][0] == 0xff {
		t.Fatalf("EncodeZapResponse aliases input RequestID (frame[1]=%x)", frames[1])
	}
}

// ---------------------------------------------------------------------------
// ServeRequest tests
// ---------------------------------------------------------------------------

func TestZapServeValidRequest200(t *testing.T) {
	policy := &mockPolicy{
		userID:   "alice",
		metadata: map[string]string{"User-Id": "alice"},
	}
	h := NewZapHandler(policy)

	req := EncodeZapRequest(&ZapRequest{
		Version:     "1.0",
		RequestID:   []byte("req-1"),
		Domain:      "global",
		Address:     "127.0.0.1:5555",
		Identity:    nil,
		Mechanism:   "PLAIN",
		Credentials: [][]byte{[]byte("alice"), []byte("hunter2")},
	})
	respFrames := h.ServeRequest(req)

	resp, err := DecodeZapResponse(respFrames)
	if err != nil {
		t.Fatalf("DecodeZapResponse: %v", err)
	}
	if resp.StatusCode != ZapStatusOK {
		t.Fatalf("StatusCode: got %q, want %q", resp.StatusCode, ZapStatusOK)
	}
	if resp.UserID != "alice" {
		t.Fatalf("UserID: got %q, want %q", resp.UserID, "alice")
	}
	if !bytes.Equal(resp.RequestID, []byte("req-1")) {
		t.Fatalf("RequestID echo: got %x, want %x", resp.RequestID, []byte("req-1"))
	}
	if resp.Metadata["User-Id"] != "alice" {
		t.Fatalf("Metadata: got %v", resp.Metadata)
	}

	// The policy saw the raw credentials and mechanism.
	if policy.lastMechanism != "PLAIN" {
		t.Fatalf("policy.lastMechanism: got %q", policy.lastMechanism)
	}
	if len(policy.lastCreds) != 2 || !bytes.Equal(policy.lastCreds[0], []byte("alice")) {
		t.Fatalf("policy.lastCreds: got %v", policy.lastCreds)
	}
}

func TestZapServeInvalidCreds400(t *testing.T) {
	policy := &mockPolicy{
		err: fmt.Errorf("bad password: %w", zmqerr.ErrAuthFailed),
	}
	h := NewZapHandler(policy)

	req := EncodeZapRequest(&ZapRequest{
		Version:     "1.0",
		RequestID:   []byte("req-2"),
		Domain:      "global",
		Address:     "127.0.0.1:5555",
		Mechanism:   "PLAIN",
		Credentials: [][]byte{[]byte("alice"), []byte("WRONG")},
	})
	respFrames := h.ServeRequest(req)

	resp, err := DecodeZapResponse(respFrames)
	if err != nil {
		t.Fatalf("DecodeZapResponse: %v", err)
	}
	if resp.StatusCode != ZapStatusClientError {
		t.Fatalf("StatusCode: got %q, want %q", resp.StatusCode, ZapStatusClientError)
	}
	if resp.UserID != "" {
		t.Fatalf("UserID on failure: got %q, want empty", resp.UserID)
	}
	if resp.StatusText == "" {
		t.Fatal("StatusText: empty; want policy error message")
	}
}

func TestZapServeMalformedRequest500(t *testing.T) {
	// Policy must not be called on a malformed request.
	policy := &mockPolicy{
		userID: "must-not-see",
	}
	h := NewZapHandler(policy)

	cases := []struct {
		name   string
		frames [][]byte
	}{
		{
			name: "too-few-frames",
			frames: [][]byte{
				[]byte("1.0"),
				[]byte("req-x"),
				[]byte("global"),
			},
		},
		{
			name: "wrong-version",
			frames: EncodeZapRequest(&ZapRequest{
				Version:     "2.0", // unsupported
				RequestID:   []byte("req-x"),
				Domain:      "global",
				Address:     "addr",
				Mechanism:   "PLAIN",
				Credentials: [][]byte{[]byte("u"), []byte("p")},
			}),
		},
		{
			name: "unknown-mechanism",
			frames: EncodeZapRequest(&ZapRequest{
				Version:   "1.0",
				RequestID: []byte("req-x"),
				Domain:    "global",
				Address:   "addr",
				Mechanism: "WEIRD",
			}),
		},
		{
			name: "plain-wrong-cred-count",
			frames: EncodeZapRequest(&ZapRequest{
				Version:     "1.0",
				RequestID:   []byte("req-x"),
				Domain:      "global",
				Address:     "addr",
				Mechanism:   "PLAIN",
				Credentials: [][]byte{[]byte("only-one-frame")},
			}),
		},
		{
			name: "curve-wrong-key-length",
			frames: EncodeZapRequest(&ZapRequest{
				Version:     "1.0",
				RequestID:   []byte("req-x"),
				Domain:      "global",
				Address:     "addr",
				Mechanism:   "CURVE",
				Credentials: [][]byte{[]byte("too-short")},
			}),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policy.lastMechanism = ""
			respFrames := h.ServeRequest(tc.frames)
			resp, err := DecodeZapResponse(respFrames)
			if err != nil {
				t.Fatalf("DecodeZapResponse: %v", err)
			}
			if resp.StatusCode != ZapStatusServerError {
				t.Fatalf("StatusCode: got %q, want %q", resp.StatusCode, ZapStatusServerError)
			}
			if policy.lastMechanism != "" {
				t.Fatalf("policy was called on malformed request (mech=%q)", policy.lastMechanism)
			}
		})
	}
}

func TestZapServeTemporaryFailure300(t *testing.T) {
	policy := &mockPolicy{
		err: fmt.Errorf("backend unreachable: %w", ErrZapTemporary),
	}
	h := NewZapHandler(policy)

	req := EncodeZapRequest(&ZapRequest{
		Version:     "1.0",
		RequestID:   []byte("req-3"),
		Domain:      "global",
		Address:     "addr",
		Mechanism:   "PLAIN",
		Credentials: [][]byte{[]byte("u"), []byte("p")},
	})
	respFrames := h.ServeRequest(req)
	resp, err := DecodeZapResponse(respFrames)
	if err != nil {
		t.Fatalf("DecodeZapResponse: %v", err)
	}
	if resp.StatusCode != ZapStatusTempError {
		t.Fatalf("StatusCode: got %q, want %q", resp.StatusCode, ZapStatusTempError)
	}
}

func TestZapServeGenericErrorIsServerError(t *testing.T) {
	policy := &mockPolicy{
		err: errors.New("bug in policy"),
	}
	h := NewZapHandler(policy)

	req := EncodeZapRequest(&ZapRequest{
		Version:     "1.0",
		RequestID:   []byte("req-4"),
		Domain:      "global",
		Address:     "addr",
		Mechanism:   "PLAIN",
		Credentials: [][]byte{[]byte("u"), []byte("p")},
	})
	respFrames := h.ServeRequest(req)
	resp, err := DecodeZapResponse(respFrames)
	if err != nil {
		t.Fatalf("DecodeZapResponse: %v", err)
	}
	if resp.StatusCode != ZapStatusServerError {
		t.Fatalf("StatusCode: got %q, want %q", resp.StatusCode, ZapStatusServerError)
	}
}

func TestZapServeRequestIDEcho(t *testing.T) {
	// Malformed requests should still echo the request-id when frame 1 is
	// readable, so the peer can correlate the failure with its request.
	policy := &mockPolicy{}
	h := NewZapHandler(policy)

	// Too few frames but frame 1 (request-id) is present.
	respFrames := h.ServeRequest([][]byte{
		[]byte("1.0"),
		[]byte("req-echo"),
	})
	resp, err := DecodeZapResponse(respFrames)
	if err != nil {
		t.Fatalf("DecodeZapResponse: %v", err)
	}
	if !bytes.Equal(resp.RequestID, []byte("req-echo")) {
		t.Fatalf("RequestID echo on malformed: got %x, want %x", resp.RequestID, []byte("req-echo"))
	}
	if resp.StatusCode != ZapStatusServerError {
		t.Fatalf("StatusCode: got %q, want %q", resp.StatusCode, ZapStatusServerError)
	}
}

func TestZapNullMechanismAllowsZeroCreds(t *testing.T) {
	// NULL authentication over ZAP is allowed (the server still wants an
	// authorization hook even if no credentials flow). The policy receives
	// zero credentials.
	policy := &mockPolicy{
		userID: "anon",
	}
	h := NewZapHandler(policy)

	req := EncodeZapRequest(&ZapRequest{
		Version:   "1.0",
		RequestID: []byte("req-5"),
		Domain:    "global",
		Address:   "addr",
		Mechanism: "NULL",
	})
	respFrames := h.ServeRequest(req)
	resp, err := DecodeZapResponse(respFrames)
	if err != nil {
		t.Fatalf("DecodeZapResponse: %v", err)
	}
	if resp.StatusCode != ZapStatusOK {
		t.Fatalf("StatusCode: got %q, want %q", resp.StatusCode, ZapStatusOK)
	}
	if len(policy.lastCreds) != 0 {
		t.Fatalf("policy.lastCreds: got %v, want empty", policy.lastCreds)
	}
}

func TestZapCurveMechanismRequiresKey(t *testing.T) {
	policy := &mockPolicy{userID: "pk"}
	h := NewZapHandler(policy)

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	req := EncodeZapRequest(&ZapRequest{
		Version:     "1.0",
		RequestID:   []byte("req-6"),
		Domain:      "global",
		Address:     "addr",
		Mechanism:   "CURVE",
		Credentials: [][]byte{key},
	})
	respFrames := h.ServeRequest(req)
	resp, err := DecodeZapResponse(respFrames)
	if err != nil {
		t.Fatalf("DecodeZapResponse: %v", err)
	}
	if resp.StatusCode != ZapStatusOK {
		t.Fatalf("StatusCode: got %q, want %q", resp.StatusCode, ZapStatusOK)
	}
	if !bytes.Equal(policy.lastCreds[0], key) {
		t.Fatalf("policy.lastCreds[0]: got %x, want %x", policy.lastCreds[0], key)
	}
}

func TestNewZapHandlerPanicsOnNilPolicy(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("NewZapHandler(nil): expected panic")
		}
	}()
	_ = NewZapHandler(nil)
}
