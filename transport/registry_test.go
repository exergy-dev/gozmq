package transport

import (
	"context"
	"testing"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_RegistersStandardSchemes(t *testing.T) {
	r := New()
	for _, scheme := range []string{"tcp", "tcp6", "inproc"} {
		tr, err := r.Lookup(scheme + "://nope:0")
		if !assert.NoError(t, err, "Lookup(%q)", scheme) {
			continue
		}
		assert.Equal(t, scheme, tr.Scheme(), "Lookup(%q) returned wrong scheme", scheme)
	}
}

func TestRegistry_UnknownScheme(t *testing.T) {
	r := New()
	_, err := r.Lookup("frob://something")
	require.ErrorIs(t, err, zmqerr.ErrTransportUnknown)
}

func TestRegistry_BadEndpoint(t *testing.T) {
	r := New()
	for _, ep := range []string{"", "no-scheme-here", "://missing"} {
		_, err := r.Lookup(ep)
		assert.ErrorIs(t, err, zmqerr.ErrEndpointInvalid, "Lookup(%q)", ep)
	}
}

func TestRegistry_RegisterOverwrites(t *testing.T) {
	r := NewRegistry()
	r.Register(NewTCPTransport())
	first, _ := r.Lookup("tcp://x:1")

	// Registering a second TCPTransport for the same scheme should replace
	// the first.
	replacement := NewTCPTransport()
	r.Register(replacement)
	got, _ := r.Lookup("tcp://x:1")

	require.False(t, first == got, "Register did not overwrite existing entry")
}

// TestRegistry_ConcurrentAccess is a -race smoke that the registry tolerates
// concurrent Register / Lookup. Tiny but enough for the data-race detector.
func TestRegistry_ConcurrentAccess(t *testing.T) {
	r := New()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			r.Register(NewTCPTransport())
		}
		close(done)
	}()
	for i := 0; i < 200; i++ {
		_, _ = r.Lookup("tcp://h:1")
	}
	<-done
}

// TestRegistry_LookupHonorsScheme confirms that the underlying parse only
// considers the scheme prefix and never returns the wrong transport.
func TestRegistry_LookupHonorsScheme(t *testing.T) {
	r := New()
	tr, err := r.Lookup("inproc://name")
	require.NoError(t, err)
	require.Equal(t, "inproc", tr.Scheme())
}

// TestRegistry_DialThroughLookup is a tiny end-to-end smoke that the
// registry-returned transport actually serves Dial. We use inproc because
// it does not need a real network port.
func TestRegistry_DialThroughLookup(t *testing.T) {
	r := New()
	tr, err := r.Lookup("inproc://reg-smoke")
	require.NoError(t, err)
	l, err := tr.Listen(context.Background(), "inproc://reg-smoke")
	require.NoError(t, err, "Listen")
	defer l.Close()

	ctx := context.Background()
	c, err := tr.Dial(ctx, "inproc://reg-smoke")
	require.NoError(t, err, "Dial")
	defer c.Close()

	srv, err := l.Accept(ctx)
	require.NoError(t, err, "Accept")
	defer srv.Close()
}
