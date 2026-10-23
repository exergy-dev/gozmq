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

	"github.com/stretchr/testify/require"

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
	require.NoError(t, err, "generate key")
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
	require.NoError(t, err, "create certificate")
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err, "parse certificate")
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
	require.NoError(t, err, "NewTLSTransport")

	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()
	ctxZmq.RegisterTransport(tr)

	puller, err := ctxZmq.NewSocket(gozmq.Pull)
	require.NoError(t, err, "new pull")
	defer puller.Close()
	require.NoError(t, puller.Bind("tls+tcp://127.0.0.1:0"), "bind tls+tcp")
	ep := puller.LastEndpoint()
	require.True(t, strings.HasPrefix(ep, "tls+tcp://127.0.0.1:"),
		"LastEndpoint = %q, want resolved tls+tcp://127.0.0.1:<port>", ep)

	pusher, err := ctxZmq.NewSocket(gozmq.Push)
	require.NoError(t, err, "new push")
	defer pusher.Close()
	require.NoError(t, pusher.Connect(ep), "connect tls+tcp")

	callCtx := testutil.Ctx(t)
	const n = 5
	for i := 0; i < n; i++ {
		require.NoError(t, pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{byte(i)})), "send[%d]", i)
	}
	for i := 0; i < n; i++ {
		m, err := puller.RecvMsg(callCtx)
		require.NoError(t, err, "recv[%d]", i)
		require.Equal(t, []byte{byte(i)}, m.Bytes(), "recv[%d]", i)
		m.Release()
	}
}
