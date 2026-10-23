package security

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
)

// mockPolicy lets tests script the policy response. It can return OK, an
// ErrAuthFailed-wrapping error, a server error, or a temporary-failure error.
type mockPolicy struct {
	userID   string
	metadata map[string]string
	err      error

	// observed is populated on each Authenticate call so tests can assert
	// on what the handler passed through.
	lastMechanism string
	lastCreds     [][]byte
}

func (p *mockPolicy) Authenticate(_, _, mech string, creds [][]byte) (string, map[string]string, error) {
	p.lastMechanism = mech
	p.lastCreds = creds
	return p.userID, p.metadata, p.err
}

func TestZapRequestRoundTrip(t *testing.T) {
	req := &ZAPRequest{
		Version:     "1.0",
		RequestID:   []byte{0x01, 0x02, 0x03},
		Domain:      "global",
		Address:     "127.0.0.1:5555",
		Identity:    []byte("peer-id"),
		Mechanism:   "PLAIN",
		Credentials: [][]byte{[]byte("alice"), []byte("hunter2")},
	}
	frames := EncodeZAPRequest(req)
	require.Len(t, frames, 8)
	got, err := DecodeZAPRequest(frames)
	require.NoError(t, err)
	require.Equal(t, req.Version, got.Version)
	require.Equal(t, req.Domain, got.Domain)
	require.Equal(t, req.Address, got.Address)
	require.Equal(t, req.Mechanism, got.Mechanism)
	require.Equal(t, req.RequestID, got.RequestID)
	require.Equal(t, req.Identity, got.Identity)
	require.Len(t, got.Credentials, 2)
	require.Equal(t, []byte("alice"), got.Credentials[0])
	require.Equal(t, []byte("hunter2"), got.Credentials[1])
}

func TestZapResponseRoundTrip(t *testing.T) {
	resp := &ZAPResponse{
		Version:    "1.0",
		RequestID:  []byte{0x10, 0x20},
		StatusCode: "200",
		StatusText: "OK",
		UserID:     "alice",
		Metadata:   map[string]string{"Role": "admin"},
	}
	frames := EncodeZAPResponse(resp)
	require.Len(t, frames, 6)
	got, err := DecodeZAPResponse(frames)
	require.NoError(t, err)
	require.Equal(t, resp.Version, got.Version)
	require.Equal(t, resp.StatusCode, got.StatusCode)
	require.Equal(t, resp.StatusText, got.StatusText)
	require.Equal(t, resp.UserID, got.UserID)
	require.Equal(t, resp.RequestID, got.RequestID)
	require.Equal(t, "admin", got.Metadata["Role"])
}

func TestZapResponseEncodeIsolatesInput(t *testing.T) {
	// EncodeZAPResponse must not alias resp's fields — callers commonly mutate
	// the scratch buffer after handing it off.
	reqID := []byte{0x01, 0x02}
	resp := &ZAPResponse{
		RequestID:  reqID,
		StatusCode: "200",
		StatusText: "OK",
		UserID:     "u",
	}
	frames := EncodeZAPResponse(resp)
	// Mutate caller's RequestID; the frame in `frames` must be unchanged.
	reqID[0] = 0xff
	require.NotEqual(t, byte(0xff), frames[1][0], "EncodeZAPResponse aliases input RequestID")
}

func TestZapServeValidRequest200(t *testing.T) {
	policy := &mockPolicy{
		userID:   "alice",
		metadata: map[string]string{"User-Id": "alice"},
	}
	h := ZAPHandler(policy.Authenticate)

	req := EncodeZAPRequest(&ZAPRequest{
		Version:     "1.0",
		RequestID:   []byte("req-1"),
		Domain:      "global",
		Address:     "127.0.0.1:5555",
		Identity:    nil,
		Mechanism:   "PLAIN",
		Credentials: [][]byte{[]byte("alice"), []byte("hunter2")},
	})
	respFrames := h.ServeRequest(req)

	resp, err := DecodeZAPResponse(respFrames)
	require.NoError(t, err)
	require.Equal(t, ZAPStatusOK, resp.StatusCode)
	require.Equal(t, "alice", resp.UserID)
	require.Equal(t, []byte("req-1"), resp.RequestID)
	require.Equal(t, "alice", resp.Metadata["User-Id"])

	// The policy saw the raw credentials and mechanism.
	require.Equal(t, "PLAIN", policy.lastMechanism)
	require.Len(t, policy.lastCreds, 2)
	require.Equal(t, []byte("alice"), policy.lastCreds[0])
}

func TestZapServeInvalidCreds400(t *testing.T) {
	policy := &mockPolicy{
		err: fmt.Errorf("bad password: %w", zmqerr.ErrAuthFailed),
	}
	h := ZAPHandler(policy.Authenticate)

	req := EncodeZAPRequest(&ZAPRequest{
		Version:     "1.0",
		RequestID:   []byte("req-2"),
		Domain:      "global",
		Address:     "127.0.0.1:5555",
		Mechanism:   "PLAIN",
		Credentials: [][]byte{[]byte("alice"), []byte("WRONG")},
	})
	respFrames := h.ServeRequest(req)

	resp, err := DecodeZAPResponse(respFrames)
	require.NoError(t, err)
	require.Equal(t, ZAPStatusClientError, resp.StatusCode)
	require.Empty(t, resp.UserID, "UserID on failure should be empty")
	require.NotEmpty(t, resp.StatusText, "StatusText should contain policy error message")
}

func TestZapServeMalformedRequest500(t *testing.T) {
	// Policy must not be called on a malformed request.
	policy := &mockPolicy{
		userID: "must-not-see",
	}
	h := ZAPHandler(policy.Authenticate)

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
			frames: EncodeZAPRequest(&ZAPRequest{
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
			frames: EncodeZAPRequest(&ZAPRequest{
				Version:   "1.0",
				RequestID: []byte("req-x"),
				Domain:    "global",
				Address:   "addr",
				Mechanism: "WEIRD",
			}),
		},
		{
			name: "plain-wrong-cred-count",
			frames: EncodeZAPRequest(&ZAPRequest{
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
			frames: EncodeZAPRequest(&ZAPRequest{
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
			resp, err := DecodeZAPResponse(respFrames)
			require.NoError(t, err)
			require.Equal(t, ZAPStatusServerError, resp.StatusCode)
			require.Empty(t, policy.lastMechanism, "policy was called on malformed request")
		})
	}
}

func TestZapServeTemporaryFailure300(t *testing.T) {
	policy := &mockPolicy{
		err: fmt.Errorf("backend unreachable: %w", ErrZAPTemporary),
	}
	h := ZAPHandler(policy.Authenticate)

	req := EncodeZAPRequest(&ZAPRequest{
		Version:     "1.0",
		RequestID:   []byte("req-3"),
		Domain:      "global",
		Address:     "addr",
		Mechanism:   "PLAIN",
		Credentials: [][]byte{[]byte("u"), []byte("p")},
	})
	respFrames := h.ServeRequest(req)
	resp, err := DecodeZAPResponse(respFrames)
	require.NoError(t, err)
	require.Equal(t, ZAPStatusTempError, resp.StatusCode)
}

func TestZapServeGenericErrorIsServerError(t *testing.T) {
	policy := &mockPolicy{
		err: errors.New("bug in policy"),
	}
	h := ZAPHandler(policy.Authenticate)

	req := EncodeZAPRequest(&ZAPRequest{
		Version:     "1.0",
		RequestID:   []byte("req-4"),
		Domain:      "global",
		Address:     "addr",
		Mechanism:   "PLAIN",
		Credentials: [][]byte{[]byte("u"), []byte("p")},
	})
	respFrames := h.ServeRequest(req)
	resp, err := DecodeZAPResponse(respFrames)
	require.NoError(t, err)
	require.Equal(t, ZAPStatusServerError, resp.StatusCode)
}

func TestZapServeRequestIDEcho(t *testing.T) {
	// Malformed requests should still echo the request-id when frame 1 is
	// readable, so the peer can correlate the failure with its request.
	policy := &mockPolicy{}
	h := ZAPHandler(policy.Authenticate)

	// Too few frames but frame 1 (request-id) is present.
	respFrames := h.ServeRequest([][]byte{
		[]byte("1.0"),
		[]byte("req-echo"),
	})
	resp, err := DecodeZAPResponse(respFrames)
	require.NoError(t, err)
	require.Equal(t, []byte("req-echo"), resp.RequestID)
	require.Equal(t, ZAPStatusServerError, resp.StatusCode)
}

func TestZapNullMechanismAllowsZeroCreds(t *testing.T) {
	// NULL authentication over ZAP is allowed (the server still wants an
	// authorization hook even if no credentials flow). The policy receives
	// zero credentials.
	policy := &mockPolicy{
		userID: "anon",
	}
	h := ZAPHandler(policy.Authenticate)

	req := EncodeZAPRequest(&ZAPRequest{
		Version:   "1.0",
		RequestID: []byte("req-5"),
		Domain:    "global",
		Address:   "addr",
		Mechanism: "NULL",
	})
	respFrames := h.ServeRequest(req)
	resp, err := DecodeZAPResponse(respFrames)
	require.NoError(t, err)
	require.Equal(t, ZAPStatusOK, resp.StatusCode)
	require.Empty(t, policy.lastCreds)
}

func TestZapCurveMechanismRequiresKey(t *testing.T) {
	policy := &mockPolicy{userID: "pk"}
	h := ZAPHandler(policy.Authenticate)

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	req := EncodeZAPRequest(&ZAPRequest{
		Version:     "1.0",
		RequestID:   []byte("req-6"),
		Domain:      "global",
		Address:     "addr",
		Mechanism:   "CURVE",
		Credentials: [][]byte{key},
	})
	respFrames := h.ServeRequest(req)
	resp, err := DecodeZAPResponse(respFrames)
	require.NoError(t, err)
	require.Equal(t, ZAPStatusOK, resp.StatusCode)
	require.Equal(t, key, policy.lastCreds[0])
}
