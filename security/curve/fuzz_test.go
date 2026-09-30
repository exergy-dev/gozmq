package curve

import (
	"bytes"
	"testing"

	"github.com/exergy-dev/gozmq/security"
)

// Fuzz targets for the CURVE parsers that see peer-controlled bytes. The
// invariant is no panics; Z85 additionally has to round-trip.

// FuzzCurveServerHandshake feeds a CURVE server an arbitrary HELLO and, if
// that is accepted, an arbitrary INITIATE.
func FuzzCurveServerHandshake(f *testing.F) {
	sPub, sSec, err := GenerateKeyPair()
	if err != nil {
		f.Fatal(err)
	}
	cPub, cSec, err := GenerateKeyPair()
	if err != nil {
		f.Fatal(err)
	}
	cli, err := newMech(security.RoleClient, cSec, cPub, sPub, nil, nil)
	if err != nil {
		f.Fatal(err)
	}
	hello, _, err := cli.NextHandshake(nil)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(hello, []byte{8, 'I', 'N', 'I', 'T', 'I', 'A', 'T', 'E'})
	f.Add([]byte{5, 'H', 'E', 'L', 'L', 'O'}, []byte{})
	f.Fuzz(func(t *testing.T, a, b []byte) {
		srv, err := newMech(security.RoleServer, sSec, Key{}, Key{}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := srv.NextHandshake(nil); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if _, _, err := srv.NextHandshake(a); err == nil {
			_, _, _ = srv.NextHandshake(b)
		}
	})
}

// FuzzCurveClientHandshake feeds a CURVE client an arbitrary WELCOME/ERROR.
func FuzzCurveClientHandshake(f *testing.F) {
	sPub, _, err := GenerateKeyPair()
	if err != nil {
		f.Fatal(err)
	}
	cPub, cSec, err := GenerateKeyPair()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(errorCommand("no"))
	f.Add([]byte{0x5E, 'R', 'R', 'O', 'R', 0x03, '4', '0', '0'})
	f.Add(append([]byte{7}, cmdWelcome...))
	f.Fuzz(func(t *testing.T, a []byte) {
		c, err := newMech(security.RoleClient, cSec, cPub, sPub, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.NextHandshake(nil); err != nil {
			t.Fatalf("seed: %v", err)
		}
		_, _, _ = c.NextHandshake(a)
	})
}

// FuzzCurveDecodeFrame feeds a post-handshake CURVE mechanism arbitrary
// MESSAGE bodies and flags.
func FuzzCurveDecodeFrame(f *testing.F) {
	client, server := runHandshake(f)
	_, msg, err := client.EncodeFrame(0, []byte("hello"))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(uint8(0), msg)
	f.Add(uint8(0x04), msg[:len(msg)-1])
	base := server.(*curveMech)
	f.Fuzz(func(t *testing.T, flags uint8, wire []byte) {
		m := *base // fresh nonce state per input; DecodeFrame mutates only counters
		_, _, _ = m.DecodeFrame(flags, wire)
	})
}

// FuzzZ85Decode checks that Z85Decode never panics and that whatever it
// accepts re-encodes to the same text.
func FuzzZ85Decode(f *testing.F) {
	f.Add("HelloWorld")
	f.Add("rq:rM>}U?@Lns47E1%kR.o@n%FcmmsL/@{H8]yf7")
	f.Add("")
	f.Add("abcd")
	f.Fuzz(func(t *testing.T, s string) {
		b, err := Z85Decode(s)
		if err != nil {
			return
		}
		if got := Z85Encode(b); got != s {
			t.Fatalf("Z85 round trip: %q -> %x -> %q", s, b, got)
		}
		if again, err := Z85Decode(Z85Encode(b)); err != nil || !bytes.Equal(again, b) {
			t.Fatalf("Z85 re-decode: %x -> %x, %v", b, again, err)
		}
	})
}
