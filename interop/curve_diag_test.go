//go:build interop

package interop

import (
	"context"
	"testing"
	"time"

	"github.com/rkm/gozmq"
	"github.com/rkm/gozmq/security/curve"
)

// TestCurve_Diag_LibzmqFlags is a diagnostic-only helper. Un-skip locally
// to capture the flag byte sequences both peers emit over CURVE-encrypted
// traffic; useful when debugging a wire-format disagreement.
//
// The file intentionally covers two variants — libzmq↔libzmq and
// gozmq↔libzmq — and logs results via t.Logf so a diff of the two flag
// streams immediately flags any encoding-level divergence.
func TestCurve_Diag_LibzmqFlags(t *testing.T) {
	t.Skip("diagnostic-only — un-skip locally when reverse-engineering wire format")
	gozmqClient_libzmqServer(t)
}

func TestCurve_Diag_LibzmqLibzmq(t *testing.T) {
	t.Skip("diagnostic-only — un-skip locally when reverse-engineering wire format")
	libzmqClient_libzmqServer(t)
}

func libzmqClient_libzmqServer(t *testing.T) {
	serverPub, serverSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	clientPub, clientSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}

	libCtx := newLibCtx(t)
	server := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server.Close() })
	_ = server.SetOption(ZMQ_CURVE_SERVER, 1)
	_ = server.SetOption(ZMQ_CURVE_SECRETKEY, serverSec[:])
	_ = server.SetOption(ZMQ_CURVE_PUBLICKEY, serverPub[:])
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	upstream, err := server.LastEndpoint()
	if err != nil {
		t.Fatal(err)
	}
	proxy := newFlagSniffingProxy(t, upstream)

	client := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = client.Close() })
	_ = client.SetOption(ZMQ_CURVE_SERVERKEY, serverPub[:])
	_ = client.SetOption(ZMQ_CURVE_PUBLICKEY, clientPub[:])
	_ = client.SetOption(ZMQ_CURVE_SECRETKEY, clientSec[:])
	if err := client.Connect(proxy.Addr()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(handshakeSettle)

	_ = client.Send([][]byte{[]byte("hi")})
	type recvRes struct {
		frames [][]byte
		err    error
	}
	ch := make(chan recvRes, 1)
	go func() { f, e := server.Recv(); ch <- recvRes{f, e} }()
	select {
	case r := <-ch:
		t.Logf("libzmq-libzmq server recv: frames=%d err=%v", len(r.frames), r.err)
	case <-time.After(500 * time.Millisecond):
		t.Log("libzmq-libzmq server recv: timed out")
	}
	time.Sleep(50 * time.Millisecond)
	c2s, s2c := proxy.snapshot()
	t.Logf("libzmq-libzmq c2s flags: [%s]", formatFlags(c2s))
	t.Logf("libzmq-libzmq s2c flags: [%s]", formatFlags(s2c))
	cb, sb := proxy.bodiesSnapshot()
	for i, b := range cb {
		if len(b) > 64 {
			t.Logf("  c2s[%d] (%d bytes): %x...%x", i, len(b), b[:48], b[len(b)-16:])
		} else {
			t.Logf("  c2s[%d] (%d bytes): %x", i, len(b), b)
		}
	}
	for i, b := range sb {
		if len(b) > 64 {
			t.Logf("  s2c[%d] (%d bytes): %x...%x", i, len(b), b[:48], b[len(b)-16:])
		} else {
			t.Logf("  s2c[%d] (%d bytes): %x", i, len(b), b)
		}
	}
}

func gozmqClient_libzmqServer(t *testing.T) {
	serverPub, serverSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	clientPub, clientSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}

	libCtx := newLibCtx(t)
	server := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server.Close() })
	_ = server.SetOption(ZMQ_CURVE_SERVER, 1)
	_ = server.SetOption(ZMQ_CURVE_SECRETKEY, serverSec[:])
	_ = server.SetOption(ZMQ_CURVE_PUBLICKEY, serverPub[:])
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	upstream, err := server.LastEndpoint()
	if err != nil {
		t.Fatal(err)
	}

	proxy := newFlagSniffingProxy(t, upstream)

	gCtx := newGozmqCtx(t)
	client, err := gCtx.NewSocket(gozmq.Pair,
		gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:]))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(proxy.Addr()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(handshakeSettle)

	callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("from-gozmq"))); err != nil {
		t.Logf("gozmq send err: %v", err)
	}
	// Bound the libzmq recv so the test can finish even if the MESSAGE
	// never lands.
	type recvRes struct {
		frames [][]byte
		err    error
	}
	ch := make(chan recvRes, 1)
	go func() { f, e := server.Recv(); ch <- recvRes{f, e} }()
	select {
	case r := <-ch:
		t.Logf("libzmq server recv: frames=%d err=%v", len(r.frames), r.err)
	case <-time.After(500 * time.Millisecond):
		t.Log("libzmq server recv: timed out")
	}

	time.Sleep(50 * time.Millisecond)
	c2s, s2c := proxy.snapshot()
	t.Logf("gozmq→libzmq client→server flag bytes: [%s] (len=%d)", formatFlags(c2s), len(c2s))
	t.Logf("gozmq→libzmq server→client flag bytes: [%s] (len=%d)", formatFlags(s2c), len(s2c))
	cb, sb := proxy.bodiesSnapshot()
	for i, b := range cb {
		if len(b) > 128 {
			t.Logf("  c2s[%d] (%d bytes, truncated): %x...", i, len(b), b[:128])
		} else {
			t.Logf("  c2s[%d] (%d bytes): %x", i, len(b), b)
		}
	}
	for i, b := range sb {
		if len(b) > 128 {
			t.Logf("  s2c[%d] (%d bytes, truncated): %x...", i, len(b), b[:128])
		} else {
			t.Logf("  s2c[%d] (%d bytes): %x", i, len(b), b)
		}
	}
}
