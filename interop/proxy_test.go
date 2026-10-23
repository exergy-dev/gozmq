//go:build interop

package interop

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// SniffingProxy is a minimal in-process TCP splicer that sits between two
// peers so a test can mutate frames on the wire. It forwards the 64-byte
// ZMTP greeting verbatim, then reads each frame's flags + length and hands
// the body to a mutator before forwarding it.
type SniffingProxy struct {
	t       testing.TB
	listen  net.Listener
	upstrm  string
	stopped chan struct{}

	startOnce sync.Once
	stopOnce  sync.Once

	mu      sync.Mutex
	active  []net.Conn
	mutator func(clientToServer bool, frameIdx int, body []byte) []byte
}

// NewSniffingProxy constructs a proxy bound to a random loopback port. Call
// Start before connecting a peer, and Stop to tear it down.
func NewSniffingProxy(t testing.TB, upstream string) *SniffingProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	return &SniffingProxy{t: t, listen: ln, upstrm: upstream, stopped: make(chan struct{})}
}

// Start begins accepting the single proxied connection.
func (p *SniffingProxy) Start() { p.startOnce.Do(func() { go p.accept() }) }

// Stop tears the proxy down and waits for its goroutines to exit.
func (p *SniffingProxy) Stop() {
	p.stopOnce.Do(func() {
		_ = p.listen.Close()
		p.mu.Lock()
		for _, c := range p.active {
			_ = c.Close()
		}
		p.mu.Unlock()
		<-p.stopped
	})
}

// Addr returns the tcp:// endpoint callers connect to so traffic routes
// through the proxy.
func (p *SniffingProxy) Addr() string { return "tcp://" + p.listen.Addr().String() }

// SetMutator installs a per-frame body mutator. It is called for every
// post-greeting frame body in both directions; a non-nil return replaces the
// body on the wire and must be the same length.
func (p *SniffingProxy) SetMutator(fn func(clientToServer bool, frameIdx int, body []byte) []byte) {
	p.mu.Lock()
	p.mutator = fn
	p.mu.Unlock()
}

func (p *SniffingProxy) accept() {
	defer close(p.stopped)
	conn, err := p.listen.Accept()
	if err != nil {
		return
	}
	defer conn.Close()

	up, err := net.DialTimeout("tcp", p.upstrm[len("tcp://"):], 2*time.Second)
	if err != nil {
		return
	}
	defer up.Close()

	p.mu.Lock()
	p.active = append(p.active, conn, up)
	p.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); p.pipe(conn, up, true) }()
	go func() { defer wg.Done(); p.pipe(up, conn, false) }()
	wg.Wait()
}

func (p *SniffingProxy) pipe(r, w net.Conn, clientToServer bool) {
	forwarded := 0
	buf := make([]byte, 64)
	for forwarded < 64 {
		n, err := r.Read(buf[:64-forwarded])
		if err != nil {
			return
		}
		if _, werr := w.Write(buf[:n]); werr != nil {
			return
		}
		forwarded += n
	}

	for frameIdx := 0; ; frameIdx++ {
		hdr := make([]byte, 2)
		if _, err := io.ReadFull(r, hdr); err != nil {
			return
		}
		var size uint64
		full := hdr
		if hdr[0]&0x02 != 0 { // long frame: 8-byte length follows the flags
			more := make([]byte, 7)
			if _, err := io.ReadFull(r, more); err != nil {
				return
			}
			full = append(hdr, more...)
			for i := 0; i < 8; i++ {
				size = (size << 8) | uint64(full[1+i])
			}
		} else {
			size = uint64(hdr[1])
		}
		body := make([]byte, size)
		if size > 0 {
			if _, err := io.ReadFull(r, body); err != nil {
				return
			}
		}
		if _, err := w.Write(full); err != nil {
			return
		}
		if size == 0 {
			continue
		}
		out := body
		p.mu.Lock()
		mutator := p.mutator
		p.mu.Unlock()
		if mutator != nil {
			if m := mutator(clientToServer, frameIdx, body); m != nil {
				if len(m) != len(body) {
					p.t.Errorf("mutator returned %d bytes, want %d (length must be preserved)", len(m), len(body))
					return
				}
				out = m
			}
		}
		if _, err := w.Write(out); err != nil {
			return
		}
	}
}

func newFlagSniffingProxy(t *testing.T, upstream string) *SniffingProxy {
	t.Helper()
	p := NewSniffingProxy(t, upstream)
	p.Start()
	t.Cleanup(func() { p.Stop() })
	return p
}
