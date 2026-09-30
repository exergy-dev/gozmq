package security

import (
	"bytes"
	"testing"
)

// Fuzz targets for everything in this package that parses peer-controlled
// bytes before authentication. The invariant is no panics; the ZAP codec
// additionally has to round-trip what it accepts.

// FuzzPlainServerHandshake feeds a PLAIN server two arbitrary inbound
// commands (HELLO, then INITIATE when HELLO was accepted).
func FuzzPlainServerHandshake(f *testing.F) {
	hello, _ := encodeHello("u", "p")
	f.Add(hello, encodeCommand(plainCmdInitiate, EncodeMetadata(map[string]string{"Socket-Type": "PAIR"})))
	f.Add([]byte{5, 'H', 'E', 'L', 'L', 'O', 1, 'u'}, []byte{8, 'I', 'N', 'I', 'T', 'I', 'A', 'T', 'E'})
	f.Add([]byte{}, []byte{0x5E, 'R', 'R', 'O', 'R'})
	f.Fuzz(func(t *testing.T, a, b []byte) {
		for _, allow := range []bool{true, false} {
			s := NewPlainServer(func(string, string) bool { return allow }, nil)
			if _, _, err := s.NextHandshake(nil); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if _, _, err := s.NextHandshake(a); err == nil {
				_, _, _ = s.NextHandshake(b)
			}
		}
	})
}

// FuzzPlainClientHandshake feeds a PLAIN client arbitrary WELCOME and READY
// replies.
func FuzzPlainClientHandshake(f *testing.F) {
	f.Add(encodeCommand(plainCmdWelcome, nil), EncodeReady(map[string]string{"Socket-Type": "PAIR"}))
	f.Add(encodeCommand(plainCmdError, encodeErrorReason("no")), []byte{})
	f.Add([]byte{0x5E, 'R', 'R', 'O', 'R', 0x03, '4', '0', '0'}, []byte{5, 'E', 'R', 'R', 'O', 'R', 9})
	f.Fuzz(func(t *testing.T, a, b []byte) {
		c := NewPlainClient("u", "p", nil)
		if _, _, err := c.NextHandshake(nil); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if _, _, err := c.NextHandshake(a); err == nil {
			_, _, _ = c.NextHandshake(b)
		}
	})
}

// FuzzNullHandshake feeds both NULL roles an arbitrary peer command.
func FuzzNullHandshake(f *testing.F) {
	f.Add(EncodeReady(map[string]string{"Socket-Type": "PAIR"}))
	f.Add(encodeCommand(plainCmdError, encodeErrorReason("no")))
	f.Fuzz(func(t *testing.T, a []byte) {
		for _, role := range []Role{RoleClient, RoleServer} {
			m := NewNull(role, nil)
			if _, _, err := m.NextHandshake(nil); err != nil {
				t.Fatalf("seed: %v", err)
			}
			_, _, _ = m.NextHandshake(a)
		}
		_, _, _ = DecodeError(a)
	})
}

// splitFrames turns one fuzz input into a multi-frame message: each frame
// is a one-byte length followed by that many bytes (short final frame
// allowed).
func splitFrames(b []byte) [][]byte {
	var frames [][]byte
	for len(b) > 0 {
		n := min(int(b[0]), len(b)-1)
		frames = append(frames, b[1:1+n])
		b = b[1+n:]
	}
	return frames
}

// joinFrames is the inverse of splitFrames for frames up to 255 bytes.
func joinFrames(frames [][]byte) []byte {
	var out []byte
	for _, fr := range frames {
		out = append(out, byte(len(fr)))
		out = append(out, fr...)
	}
	return out
}

// FuzzDecodeZAPRequest checks DecodeZAPRequest and a full ServeRequest.
func FuzzDecodeZAPRequest(f *testing.F) {
	f.Add(joinFrames(EncodeZAPRequest(&ZAPRequest{RequestID: []byte("1"), Mechanism: "NULL"})))
	f.Add(joinFrames(EncodeZAPRequest(&ZAPRequest{RequestID: []byte("2"), Mechanism: "PLAIN",
		Credentials: [][]byte{[]byte("u"), []byte("p")}})))
	f.Add(joinFrames(EncodeZAPRequest(&ZAPRequest{RequestID: []byte("3"), Mechanism: "CURVE",
		Credentials: [][]byte{make([]byte, 32)}})))
	h := ZAPHandler(func(string, string, string, [][]byte) (string, map[string]string, error) {
		return "u", map[string]string{"k": "v"}, nil
	})
	f.Fuzz(func(t *testing.T, raw []byte) {
		frames := splitFrames(raw)
		_, _ = DecodeZAPRequest(frames)
		if _, err := DecodeZAPResponse(h.ServeRequest(frames)); err != nil {
			t.Fatalf("ServeRequest produced an undecodable response: %v", err)
		}
	})
}

// FuzzDecodeZAPResponse checks that an accepted response re-encodes to
// something that decodes to the same thing.
func FuzzDecodeZAPResponse(f *testing.F) {
	f.Add(joinFrames(EncodeZAPResponse(&ZAPResponse{RequestID: []byte("1"), StatusCode: ZAPStatusOK,
		StatusText: "OK", UserID: "u", Metadata: map[string]string{"k": "v"}})))
	f.Add(joinFrames(EncodeZAPResponse(&ZAPResponse{StatusCode: ZAPStatusClientError})))
	f.Fuzz(func(t *testing.T, raw []byte) {
		resp, err := DecodeZAPResponse(splitFrames(raw))
		if err != nil {
			return
		}
		again, err := DecodeZAPResponse(EncodeZAPResponse(resp))
		if err != nil {
			t.Fatalf("re-decode: %v", err)
		}
		if again.StatusCode != resp.StatusCode || again.UserID != resp.UserID ||
			!bytes.Equal(again.RequestID, resp.RequestID) || len(again.Metadata) != len(resp.Metadata) {
			t.Fatalf("round trip changed the response: %+v -> %+v", resp, again)
		}
	})
}
