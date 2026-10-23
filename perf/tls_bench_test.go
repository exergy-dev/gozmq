package perf_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
	gztransport "github.com/exergy-dev/gozmq/transport"
)

// benchTLSConfig returns one self-signed loopback config for both ends,
// generated once per process.
var benchTLSConfig = sync.OnceValue(func() *tls.Config {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "gozmq-bench"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, pub, priv)
	if err != nil {
		panic(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		panic(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: priv, Leaf: cert}},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS12,
	}
})

// newTLSPair is newPair over a loopback TLS transport registered on the
// pair's Context.
func newTLSPair(b *testing.B, serverType, clientType gozmq.SocketType, serverOpts, clientOpts []gozmq.Option) (server, client *gozmq.Socket) {
	b.Helper()
	tr, err := gztransport.NewTLSTransport(benchTLSConfig())
	if err != nil {
		b.Fatalf("tls transport: %v", err)
	}
	ctx := gozmq.NewContext()
	b.Cleanup(func() { _ = ctx.Close() })
	ctx.RegisterTransport(tr)
	srv := newSocket(b, ctx, serverType, serverOpts)
	if err := srv.Bind("tls+tcp://127.0.0.1:0"); err != nil {
		b.Fatalf("bind %v: %v", serverType, err)
	}
	cli := newSocket(b, ctx, clientType, clientOpts)
	if err := cli.Connect(srv.LastEndpoint()); err != nil {
		b.Fatalf("connect %v: %v", clientType, err)
	}
	return srv, cli
}

// BenchmarkPushPullThroughput_TLS is BenchmarkPushPullThroughput_TCP over
// the TLS transport, where a write batch becomes one TLS record.
func BenchmarkPushPullThroughput_TLS(b *testing.B) {
	for _, sz := range sizes {
		b.Run(sz.name, func(b *testing.B) {
			b.StopTimer()
			pull, push := newTLSPair(b, gozmq.Pull, gozmq.Push, nil, nil)
			warmStream(b, push, sz.n, pull)
			benchStream(b, push, []*gozmq.Socket{pull}, sz.n, drainWait)
		})
	}
}
