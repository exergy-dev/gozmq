package zmtp

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/rkm/gozmq/internal/zmqerr"
)

func TestCommandRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		cmd  Command
	}{
		{name: "ready-empty", cmd: Command{Name: CmdReady, Body: nil}},
		{name: "ready-body", cmd: Command{Name: CmdReady, Body: []byte("\x0bSocket-Type\x00\x00\x00\x03REQ")}},
		{name: "error", cmd: Command{Name: CmdError, Body: []byte("auth failed")}},
		{name: "subscribe-prefix", cmd: Command{Name: CmdSubscribe, Body: []byte("logs.")}},
		{name: "cancel-prefix", cmd: Command{Name: CmdCancel, Body: []byte("logs.")}},
		{name: "disconnect-empty", cmd: Command{Name: CmdDisconnect, Body: nil}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw := EncodeCommand(tc.cmd)
			got, err := DecodeCommand(raw)
			if err != nil {
				t.Fatalf("DecodeCommand: %v", err)
			}
			if got.Name != tc.cmd.Name {
				t.Fatalf("name: got %q want %q", got.Name, tc.cmd.Name)
			}
			if !bytes.Equal(got.Body, tc.cmd.Body) {
				t.Fatalf("body: got %x want %x", got.Body, tc.cmd.Body)
			}
		})
	}
}

func TestCommandWireFormat(t *testing.T) {
	t.Parallel()
	raw := EncodeCommand(Command{Name: CmdReady, Body: []byte("payload")})
	want := append([]byte{byte(len(CmdReady))}, []byte(CmdReady)...)
	want = append(want, []byte("payload")...)
	if !bytes.Equal(raw, want) {
		t.Fatalf("got %x want %x", raw, want)
	}
}

func TestCommandRejects(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		raw  []byte
	}{
		{name: "empty", raw: []byte{}},
		{name: "zero-name", raw: []byte{0x00}},
		{name: "name-overruns", raw: []byte{0x05, 'R', 'E', 'A'}},
		{name: "non-printable-name", raw: []byte{0x03, 'R', 0x01, 'Y'}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := DecodeCommand(tc.raw)
			if !errors.Is(err, zmqerr.ErrMalformedFrame) {
				t.Fatalf("err: got %v want ErrMalformedFrame", err)
			}
		})
	}
}

func TestEncodeCommandPanicsOnHugeName(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic")
		}
	}()
	EncodeCommand(Command{Name: strings.Repeat("X", MaxCommandNameLen+1)})
}

func TestPingPongRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		ttl  uint16
		ctx  []byte
	}{
		{name: "no-context", ttl: 0, ctx: nil},
		{name: "default-ttl", ttl: 3000, ctx: []byte{}},
		{name: "with-context", ttl: 1234, ctx: []byte("hello")},
		{name: "max-context", ttl: 0xFFFF, ctx: bytes.Repeat([]byte{0xAB}, PingPongMaxContext)},
	}
	for _, tc := range cases {
		tc := tc
		t.Run("ping-"+tc.name, func(t *testing.T) {
			t.Parallel()
			cmd, err := EncodePing(tc.ttl, tc.ctx)
			if err != nil {
				t.Fatalf("EncodePing: %v", err)
			}
			if cmd.Name != CmdPing {
				t.Fatalf("name: got %q", cmd.Name)
			}
			ttl, ctx, err := DecodePing(cmd)
			if err != nil {
				t.Fatalf("DecodePing: %v", err)
			}
			if ttl != tc.ttl {
				t.Fatalf("ttl: got %d want %d", ttl, tc.ttl)
			}
			if !bytes.Equal(ctx, tc.ctx) && !(len(ctx) == 0 && len(tc.ctx) == 0) {
				t.Fatalf("ctx: got %x want %x", ctx, tc.ctx)
			}
		})
		t.Run("pong-"+tc.name, func(t *testing.T) {
			t.Parallel()
			cmd, err := EncodePong(tc.ttl, tc.ctx)
			if err != nil {
				t.Fatalf("EncodePong: %v", err)
			}
			if cmd.Name != CmdPong {
				t.Fatalf("name: got %q", cmd.Name)
			}
			ttl, ctx, err := DecodePong(cmd)
			if err != nil {
				t.Fatalf("DecodePong: %v", err)
			}
			if ttl != tc.ttl {
				t.Fatalf("ttl: got %d want %d", ttl, tc.ttl)
			}
			if !bytes.Equal(ctx, tc.ctx) && !(len(ctx) == 0 && len(tc.ctx) == 0) {
				t.Fatalf("ctx: got %x want %x", ctx, tc.ctx)
			}
		})
	}
}

func TestPingPongRejects(t *testing.T) {
	t.Parallel()
	t.Run("encode-context-too-long", func(t *testing.T) {
		_, err := EncodePing(0, bytes.Repeat([]byte{0}, PingPongMaxContext+1))
		if !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("err: got %v want ErrMalformedFrame", err)
		}
		_, err = EncodePong(0, bytes.Repeat([]byte{0}, PingPongMaxContext+1))
		if !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("err: got %v want ErrMalformedFrame", err)
		}
	})
	t.Run("decode-too-short", func(t *testing.T) {
		_, _, err := DecodePing(Command{Name: CmdPing, Body: []byte{0x00}})
		if !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("err: got %v want ErrMalformedFrame", err)
		}
		_, _, err = DecodePong(Command{Name: CmdPong, Body: []byte{0x00}})
		if !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("err: got %v want ErrMalformedFrame", err)
		}
	})
	t.Run("decode-too-long", func(t *testing.T) {
		body := append([]byte{0, 0}, bytes.Repeat([]byte{0xCC}, PingPongMaxContext+1)...)
		_, _, err := DecodePing(Command{Name: CmdPing, Body: body})
		if !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("err: got %v want ErrMalformedFrame", err)
		}
	})
	t.Run("decode-wrong-name", func(t *testing.T) {
		if _, _, err := DecodePing(Command{Name: CmdPong, Body: []byte{0, 0}}); !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("err: got %v want ErrMalformedFrame", err)
		}
		if _, _, err := DecodePong(Command{Name: CmdPing, Body: []byte{0, 0}}); !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("err: got %v want ErrMalformedFrame", err)
		}
	})
}

// FuzzCommand: arbitrary input bytes interpreted as a COMMAND-frame body must
// either decode cleanly (and round-trip via EncodeCommand) or be rejected. No
// panics, no infinite loops.
func FuzzCommand(f *testing.F) {
	f.Add([]byte{0x05, 'R', 'E', 'A', 'D', 'Y'})
	f.Add([]byte{0x04, 'P', 'I', 'N', 'G', 0x00, 0x10, 'c', 't', 'x'})
	f.Add([]byte{})       // empty
	f.Add([]byte{0xFF})   // length > buffer
	f.Add([]byte{0x00})   // zero name length
	f.Add([]byte{0x01, 0x01, 0x02, 0x03}) // non-printable name byte

	f.Fuzz(func(t *testing.T, data []byte) {
		c, err := DecodeCommand(data)
		if err != nil {
			return
		}
		// Re-encode and re-decode must be stable.
		raw := EncodeCommand(c)
		c2, err2 := DecodeCommand(raw)
		if err2 != nil {
			t.Fatalf("re-decode failed: %v", err2)
		}
		if c.Name != c2.Name || !bytes.Equal(c.Body, c2.Body) {
			t.Fatalf("round-trip drift: %+v → %+v", c, c2)
		}
		// If the decoded command is a PING/PONG with a body that fits the
		// helper schema, the typed helpers must agree.
		if c.Name == CmdPing && len(c.Body) >= pingPongTTLBytes && len(c.Body) <= pingPongTTLBytes+PingPongMaxContext {
			if _, _, err := DecodePing(c); err != nil {
				t.Fatalf("DecodePing on accepted PING: %v", err)
			}
		}
	})
}
