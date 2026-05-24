package zmtp

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

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
			require.NoError(t, err, "DecodeCommand")
			require.Equal(t, tc.cmd.Name, got.Name, "name")
			if len(tc.cmd.Body) == 0 {
				require.Empty(t, got.Body, "body")
			} else {
				require.Equal(t, tc.cmd.Body, got.Body, "body")
			}
		})
	}
}

func TestCommandWireFormat(t *testing.T) {
	t.Parallel()
	raw := EncodeCommand(Command{Name: CmdReady, Body: []byte("payload")})
	want := append([]byte{byte(len(CmdReady))}, []byte(CmdReady)...)
	want = append(want, []byte("payload")...)
	require.Equal(t, want, raw)
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
			require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
		})
	}
}

func TestEncodeCommandPanicsOnHugeName(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() {
		EncodeCommand(Command{Name: strings.Repeat("X", MaxCommandNameLen+1)})
	})
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
			require.NoError(t, err, "EncodePing")
			require.Equal(t, CmdPing, cmd.Name, "name")
			ttl, ctx, err := DecodePing(cmd)
			require.NoError(t, err, "DecodePing")
			require.Equal(t, tc.ttl, ttl, "ttl")
			if len(ctx) != 0 || len(tc.ctx) != 0 {
				require.Equal(t, tc.ctx, ctx, "ctx")
			}
		})
		t.Run("pong-"+tc.name, func(t *testing.T) {
			t.Parallel()
			cmd, err := EncodePong(tc.ttl, tc.ctx)
			require.NoError(t, err, "EncodePong")
			require.Equal(t, CmdPong, cmd.Name, "name")
			ttl, ctx, err := DecodePong(cmd)
			require.NoError(t, err, "DecodePong")
			require.Equal(t, tc.ttl, ttl, "ttl")
			if len(ctx) != 0 || len(tc.ctx) != 0 {
				require.Equal(t, tc.ctx, ctx, "ctx")
			}
		})
	}
}

func TestPingPongRejects(t *testing.T) {
	t.Parallel()
	t.Run("encode-context-too-long", func(t *testing.T) {
		_, err := EncodePing(0, bytes.Repeat([]byte{0}, PingPongMaxContext+1))
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
		_, err = EncodePong(0, bytes.Repeat([]byte{0}, PingPongMaxContext+1))
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})
	t.Run("decode-too-short", func(t *testing.T) {
		_, _, err := DecodePing(Command{Name: CmdPing, Body: []byte{0x00}})
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
		_, _, err = DecodePong(Command{Name: CmdPong, Body: []byte{0x00}})
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})
	t.Run("decode-too-long", func(t *testing.T) {
		body := append([]byte{0, 0}, bytes.Repeat([]byte{0xCC}, PingPongMaxContext+1)...)
		_, _, err := DecodePing(Command{Name: CmdPing, Body: body})
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})
	t.Run("decode-wrong-name", func(t *testing.T) {
		_, _, err := DecodePing(Command{Name: CmdPong, Body: []byte{0, 0}})
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
		_, _, err = DecodePong(Command{Name: CmdPing, Body: []byte{0, 0}})
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})
}

// FuzzCommand: arbitrary input bytes interpreted as a COMMAND-frame body must
// either decode cleanly (and round-trip via EncodeCommand) or be rejected. No
// panics, no infinite loops.
func FuzzCommand(f *testing.F) {
	f.Add([]byte{0x05, 'R', 'E', 'A', 'D', 'Y'})
	f.Add([]byte{0x04, 'P', 'I', 'N', 'G', 0x00, 0x10, 'c', 't', 'x'})
	f.Add([]byte{})                       // empty
	f.Add([]byte{0xFF})                   // length > buffer
	f.Add([]byte{0x00})                   // zero name length
	f.Add([]byte{0x01, 0x01, 0x02, 0x03}) // non-printable name byte

	f.Fuzz(func(t *testing.T, data []byte) {
		c, err := DecodeCommand(data)
		if err != nil {
			return
		}
		// Re-encode and re-decode must be stable.
		raw := EncodeCommand(c)
		c2, err2 := DecodeCommand(raw)
		require.NoError(t, err2, "re-decode failed")
		require.Equal(t, c.Name, c2.Name, "round-trip name drift")
		require.Equal(t, c.Body, c2.Body, "round-trip body drift")
		// If the decoded command is a PING/PONG with a body that fits the
		// helper schema, the typed helpers must agree.
		if c.Name == CmdPing && len(c.Body) >= pingPongTTLBytes && len(c.Body) <= pingPongTTLBytes+PingPongMaxContext {
			_, _, err := DecodePing(c)
			require.NoError(t, err, "DecodePing on accepted PING")
		}
	})
}
