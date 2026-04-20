package transport

import (
	"context"
	"errors"
	"testing"

	"github.com/rkm/gozmq/internal/zmqerr"
)

func TestNew_RegistersStandardSchemes(t *testing.T) {
	r := New()
	for _, scheme := range []string{"tcp", "tcp6", "inproc"} {
		tr, err := r.Lookup(scheme + "://nope:0")
		if err != nil {
			t.Errorf("Lookup(%q) err = %v, want nil", scheme, err)
			continue
		}
		if tr.Scheme() != scheme {
			t.Errorf("Lookup(%q) returned scheme %q", scheme, tr.Scheme())
		}
	}
}

func TestRegistry_UnknownScheme(t *testing.T) {
	r := New()
	_, err := r.Lookup("frob://something")
	if !errors.Is(err, zmqerr.ErrTransportUnknown) {
		t.Fatalf("err = %v, want wraps ErrTransportUnknown", err)
	}
}

func TestRegistry_BadEndpoint(t *testing.T) {
	r := New()
	for _, ep := range []string{"", "no-scheme-here", "://missing"} {
		_, err := r.Lookup(ep)
		if !errors.Is(err, zmqerr.ErrEndpointInvalid) {
			t.Errorf("Lookup(%q) err = %v, want wraps ErrEndpointInvalid", ep, err)
		}
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

	if first == got {
		t.Fatalf("Register did not overwrite existing entry")
	}
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
	if err != nil {
		t.Fatal(err)
	}
	if tr.Scheme() != "inproc" {
		t.Fatalf("got scheme %q", tr.Scheme())
	}
}

// TestRegistry_DialThroughLookup is a tiny end-to-end smoke that the
// registry-returned transport actually serves Dial. We use inproc because
// it does not need a real network port.
func TestRegistry_DialThroughLookup(t *testing.T) {
	r := New()
	tr, err := r.Lookup("inproc://reg-smoke")
	if err != nil {
		t.Fatal(err)
	}
	l, err := tr.Listen(context.Background(), "inproc://reg-smoke")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer l.Close()

	ctx := context.Background()
	c, err := tr.Dial(ctx, "inproc://reg-smoke")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	srv, err := l.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	defer srv.Close()
}
