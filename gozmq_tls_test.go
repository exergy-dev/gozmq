package gozmq_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/transport"
)

// newSelfSignedTLSConfig generates a throwaway self-signed ed25519
// certificate for 127.0.0.1/localhost and returns a single *tls.Config
// usable for BOTH sides of a loopback handshake: Certificates for the
// server role and RootCAs pinning the same cert for client verification.
// One combined config suits the socket layer, where a single registered
// TLSTransport serves the Context's Bind (Listen) and Connect (Dial).
func newSelfSignedTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "gozmq-root-tls-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, pub, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{der},
			PrivateKey:  priv,
			Leaf:        cert,
		}},
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	}
}

// TestTLSTransport_SocketRoundTrip wires the tls+tcp transport into a
// Context via RegisterTransport and proves end-to-end PUSH/PULL delivery
// over a real TLS session: Bind resolves a tls+tcp endpoint, Connect
// dials it (server cert verified against the pinned root), and messages
// round-trip in order.
func TestTLSTransport_SocketRoundTrip(t *testing.T) {
	tr, err := transport.NewTLSTransport(newSelfSignedTLSConfig(t))
	if err != nil {
		t.Fatalf("NewTLSTransport: %v", err)
	}

	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()
	ctxZmq.RegisterTransport(tr)

	puller, err := ctxZmq.NewSocket(gozmq.Pull)
	if err != nil {
		t.Fatalf("new pull: %v", err)
	}
	defer puller.Close()
	if err := puller.Bind("tls+tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bind tls+tcp: %v", err)
	}
	ep := puller.LastEndpoint()
	if !strings.HasPrefix(ep, "tls+tcp://127.0.0.1:") {
		t.Fatalf("LastEndpoint = %q, want resolved tls+tcp://127.0.0.1:<port>", ep)
	}

	pusher, err := ctxZmq.NewSocket(gozmq.Push)
	if err != nil {
		t.Fatalf("new push: %v", err)
	}
	defer pusher.Close()
	if err := pusher.Connect(ep); err != nil {
		t.Fatalf("connect tls+tcp: %v", err)
	}

	callCtx := testutil.Ctx(t)
	const n = 5
	for i := 0; i < n; i++ {
		if err := pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{byte(i)})); err != nil {
			t.Fatalf("send[%d]: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		m, err := puller.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("recv[%d]: %v", i, err)
		}
		if len(m.Bytes()) != 1 || m.Bytes()[0] != byte(i) {
			t.Fatalf("recv[%d] = %x, want %x", i, m.Bytes(), byte(i))
		}
		m.Release()
	}
}
