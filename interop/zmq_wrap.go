//go:build interop

// Package interop exposes a thin CGO wrapper around libzmq that the
// cross-implementation tests in this directory use to drive a real libzmq
// peer against `gozmq`. The wrapper only implements the minimum surface
// needed by the test suites: contexts, sockets, bind/connect, multipart
// send/recv, a handful of socket options, and LAST_ENDPOINT recovery.
//
// This file is gated behind the `interop` build tag so CGO never enters
// the default build. See interop/README.md for run instructions.
package interop

/*
#cgo pkg-config: libzmq
#include <stdlib.h>
#include <string.h>
#include <zmq.h>

// Small shim wrappers. zmq_strerror returns `const char*` and zmq_errno
// is a macro on some platforms; wrapping them produces stable cgo-visible
// functions with plain Go types.
static const char *gozmq_strerror(int errnum) { return zmq_strerror(errnum); }
static int gozmq_errno(void) { return zmq_errno(); }
*/
import "C"

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"unsafe"
)

// Socket type constants mirrored from zmq.h. Exposing them as explicit Go
// constants keeps test bodies readable and gives a build-time signal if
// libzmq ever renumbers (which would itself be a wire-compat event worth
// flagging).
const (
	ZMQ_PAIR   = 0
	ZMQ_PUB    = 1
	ZMQ_SUB    = 2
	ZMQ_REQ    = 3
	ZMQ_REP    = 4
	ZMQ_DEALER = 5
	ZMQ_ROUTER = 6
	ZMQ_PULL   = 7
	ZMQ_PUSH   = 8
	ZMQ_XPUB   = 9
	ZMQ_XSUB   = 10
	ZMQ_STREAM = 11
)

// Selected zmq_setsockopt / zmq_getsockopt option codes. Sourced from the
// cgo-resolved zmq.h to stay in sync with whatever libzmq is installed.
var (
	ZMQ_SUBSCRIBE         = int(C.ZMQ_SUBSCRIBE)
	ZMQ_UNSUBSCRIBE       = int(C.ZMQ_UNSUBSCRIBE)
	ZMQ_LINGER            = int(C.ZMQ_LINGER)
	ZMQ_IDENTITY          = int(C.ZMQ_IDENTITY)
	ZMQ_ROUTING_ID        = int(C.ZMQ_IDENTITY) // alias used by newer libzmq
	ZMQ_PLAIN_SERVER      = int(C.ZMQ_PLAIN_SERVER)
	ZMQ_PLAIN_USERNAME    = int(C.ZMQ_PLAIN_USERNAME)
	ZMQ_PLAIN_PASSWORD    = int(C.ZMQ_PLAIN_PASSWORD)
	ZMQ_CURVE_SERVER      = int(C.ZMQ_CURVE_SERVER)
	ZMQ_CURVE_PUBLICKEY   = int(C.ZMQ_CURVE_PUBLICKEY)
	ZMQ_CURVE_SECRETKEY   = int(C.ZMQ_CURVE_SECRETKEY)
	ZMQ_CURVE_SERVERKEY   = int(C.ZMQ_CURVE_SERVERKEY)
	ZMQ_ZAP_DOMAIN        = int(C.ZMQ_ZAP_DOMAIN)
	ZMQ_ROUTER_MANDATORY  = int(C.ZMQ_ROUTER_MANDATORY)
	ZMQ_XPUB_VERBOSE      = int(C.ZMQ_XPUB_VERBOSE)
	ZMQ_HEARTBEAT_IVL     = int(C.ZMQ_HEARTBEAT_IVL)
	ZMQ_HEARTBEAT_TTL     = int(C.ZMQ_HEARTBEAT_TTL)
	ZMQ_HEARTBEAT_TIMEOUT = int(C.ZMQ_HEARTBEAT_TIMEOUT)
	ZMQ_RCVTIMEO          = int(C.ZMQ_RCVTIMEO)
	ZMQ_SNDTIMEO          = int(C.ZMQ_SNDTIMEO)
	ZMQ_CONNECT_TIMEOUT   = int(C.ZMQ_CONNECT_TIMEOUT)
	ZMQ_RECONNECT_IVL     = int(C.ZMQ_RECONNECT_IVL)
	ZMQ_MAXMSGSIZE        = int(C.ZMQ_MAXMSGSIZE)
	ZMQ_RCVMORE           = int(C.ZMQ_RCVMORE)
	ZMQ_LAST_ENDPOINT     = int(C.ZMQ_LAST_ENDPOINT)
	ZMQ_SNDMORE           = int(C.ZMQ_SNDMORE)
)

// lastError wraps the current libzmq errno into a Go error with the
// standard "libzmq: <op>: <strerror>" prefix.
func lastError(op string) error {
	eno := C.gozmq_errno()
	if eno == 0 {
		return fmt.Errorf("libzmq: %s: unknown error", op)
	}
	s := C.GoString(C.gozmq_strerror(eno))
	return fmt.Errorf("libzmq: %s: %s (errno %d)", op, s, int(eno))
}

// Ctx wraps a libzmq context (`zmq_ctx_t`). Safe for concurrent use across
// goroutines as long as Close is the last call on it.
type Ctx struct {
	ptr unsafe.Pointer

	mu     sync.Mutex
	closed bool
}

// Version returns the runtime libzmq version as (major, minor, patch). This
// is a package-level helper rather than a Ctx method because zmq_version does
// not require a context.
func Version() (major, minor, patch int) {
	var maj, min, pat C.int
	C.zmq_version(&maj, &min, &pat)
	return int(maj), int(min), int(pat)
}

// NewCtx allocates a new libzmq context.
func NewCtx() *Ctx {
	p := C.zmq_ctx_new()
	if p == nil {
		// zmq_ctx_new only fails on ENOMEM; panic is fine here since this
		// is a test helper and the caller has nothing recoverable to do.
		panic(lastError("zmq_ctx_new"))
	}
	return &Ctx{ptr: p}
}

// Close terminates the context. Blocks until lingering sockets drain per
// their LINGER option; tests should Close every socket first.
func (c *Ctx) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if rc := C.zmq_ctx_term(c.ptr); rc != 0 {
		return lastError("zmq_ctx_term")
	}
	c.ptr = nil
	return nil
}

// Sock is a libzmq socket handle. libzmq sockets are NOT thread-safe;
// callers that touch a Sock from multiple goroutines must serialize
// externally. The methods here take a mutex as cheap insurance against
// accidental concurrent close.
type Sock struct {
	ptr unsafe.Pointer

	mu     sync.Mutex
	closed bool
}

// NewSocket creates a socket of the given type on this Ctx.
func (c *Ctx) NewSocket(typ int) *Sock {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		panic(errors.New("libzmq: NewSocket on closed Ctx"))
	}
	ptr := C.zmq_socket(c.ptr, C.int(typ))
	c.mu.Unlock()
	if ptr == nil {
		panic(lastError("zmq_socket"))
	}
	return &Sock{ptr: ptr}
}

// Close destroys the socket. Subsequent calls are no-ops.
func (s *Sock) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	// Force LINGER=0 so a misbehaved test cannot hang ctx_term. Callers
	// who want to exercise LINGER set it before Close via SetOption.
	linger := C.int(0)
	C.zmq_setsockopt(s.ptr, C.int(ZMQ_LINGER), unsafe.Pointer(&linger), C.size_t(unsafe.Sizeof(linger)))
	if rc := C.zmq_close(s.ptr); rc != 0 {
		return lastError("zmq_close")
	}
	s.ptr = nil
	return nil
}

// Bind listens on the given endpoint (e.g. "tcp://127.0.0.1:0").
func (s *Sock) Bind(endpoint string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("libzmq: Bind on closed socket")
	}
	cep := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cep))
	if rc := C.zmq_bind(s.ptr, cep); rc != 0 {
		return lastError("zmq_bind " + endpoint)
	}
	return nil
}

// Connect dials the given endpoint.
func (s *Sock) Connect(endpoint string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("libzmq: Connect on closed socket")
	}
	cep := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cep))
	if rc := C.zmq_connect(s.ptr, cep); rc != 0 {
		return lastError("zmq_connect " + endpoint)
	}
	return nil
}

// Send transmits frames as a single multipart message. All but the final
// frame are sent with ZMQ_SNDMORE.
func (s *Sock) Send(frames [][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("libzmq: Send on closed socket")
	}
	if len(frames) == 0 {
		// libzmq treats this as a single empty frame; match that for
		// convenience.
		frames = [][]byte{nil}
	}
	// Pin to an OS thread for the duration of the multi-frame send to keep
	// libzmq's per-thread errno stable across cgocall boundaries. libzmq
	// sockets are thread-affine; we keep the whole logical message on one
	// thread as cheap insurance.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	for i, f := range frames {
		flags := C.int(0)
		if i < len(frames)-1 {
			flags = C.int(C.ZMQ_SNDMORE)
		}
		var ptr unsafe.Pointer
		if len(f) > 0 {
			ptr = unsafe.Pointer(&f[0])
		}
		rc := C.zmq_send(s.ptr, ptr, C.size_t(len(f)), flags)
		if rc < 0 {
			return lastError(fmt.Sprintf("zmq_send frame %d", i))
		}
	}
	return nil
}

// Recv reads a full multipart message, blocking until one is available.
// Frames accumulate in a zmq_msg_t loop until zmq_msg_more returns 0.
func (s *Sock) Recv() ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("libzmq: Recv on closed socket")
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var frames [][]byte
	for {
		var m C.zmq_msg_t
		if rc := C.zmq_msg_init(&m); rc != 0 {
			return nil, lastError("zmq_msg_init")
		}
		rc := C.zmq_msg_recv(&m, s.ptr, 0)
		if rc < 0 {
			C.zmq_msg_close(&m)
			return nil, lastError("zmq_msg_recv")
		}
		size := C.zmq_msg_size(&m)
		buf := make([]byte, int(size))
		if size > 0 {
			C.memcpy(unsafe.Pointer(&buf[0]), C.zmq_msg_data(&m), size)
		}
		more := C.zmq_msg_more(&m)
		C.zmq_msg_close(&m)
		frames = append(frames, buf)
		if more == 0 {
			return frames, nil
		}
	}
}

// SetOption configures a socket option. The value's Go type selects the
// libzmq setsockopt shape:
//
//   - int  → a C int option (LINGER, SNDHWM, …)
//   - string / []byte → a byte-array option (SUBSCRIBE, IDENTITY, …)
//
// Unsupported types return an error rather than panicking.
func (s *Sock) SetOption(opt int, val interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("libzmq: SetOption on closed socket")
	}

	switch v := val.(type) {
	case int:
		cv := C.int(v)
		if rc := C.zmq_setsockopt(s.ptr, C.int(opt), unsafe.Pointer(&cv), C.size_t(unsafe.Sizeof(cv))); rc != 0 {
			return lastError(fmt.Sprintf("zmq_setsockopt %d", opt))
		}
	case int64:
		cv := C.int64_t(v)
		if rc := C.zmq_setsockopt(s.ptr, C.int(opt), unsafe.Pointer(&cv), C.size_t(unsafe.Sizeof(cv))); rc != 0 {
			return lastError(fmt.Sprintf("zmq_setsockopt %d", opt))
		}
	case string:
		return s.setBytesLocked(opt, []byte(v))
	case []byte:
		return s.setBytesLocked(opt, v)
	default:
		return fmt.Errorf("libzmq: SetOption %d: unsupported value type %T", opt, val)
	}
	return nil
}

// GetOptionInt reads a C-int-sized option from the socket (e.g. ZMQ_RCVMORE,
// ZMQ_EVENTS). Byte-array options should use a dedicated getter (see
// LastEndpoint).
func (s *Sock) GetOptionInt(opt int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errors.New("libzmq: GetOptionInt on closed socket")
	}
	var v C.int
	sz := C.size_t(unsafe.Sizeof(v))
	if rc := C.zmq_getsockopt(s.ptr, C.int(opt), unsafe.Pointer(&v), &sz); rc != 0 {
		return 0, lastError(fmt.Sprintf("zmq_getsockopt %d", opt))
	}
	return int(v), nil
}

// Disconnect tears down a previously-established outbound connection. libzmq
// applies the active LINGER to any queued messages for that endpoint.
func (s *Sock) Disconnect(endpoint string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("libzmq: Disconnect on closed socket")
	}
	cep := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cep))
	if rc := C.zmq_disconnect(s.ptr, cep); rc != 0 {
		return lastError("zmq_disconnect " + endpoint)
	}
	return nil
}

// Unbind removes a previously-bound listener endpoint. Existing peer
// connections remain until they drop.
func (s *Sock) Unbind(endpoint string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("libzmq: Unbind on closed socket")
	}
	cep := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cep))
	if rc := C.zmq_unbind(s.ptr, cep); rc != 0 {
		return lastError("zmq_unbind " + endpoint)
	}
	return nil
}

// Monitor wires the socket's event stream to the given inproc endpoint. A
// subsequent ZMQ_PAIR socket bound to that endpoint in this Ctx receives
// ZMQ_EVENT_* frames. Passing 0 for events subscribes to all events.
func (s *Sock) Monitor(endpoint string, events int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("libzmq: Monitor on closed socket")
	}
	if events == 0 {
		events = int(C.ZMQ_EVENT_ALL)
	}
	cep := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cep))
	if rc := C.zmq_socket_monitor(s.ptr, cep, C.int(events)); rc != 0 {
		return lastError("zmq_socket_monitor " + endpoint)
	}
	return nil
}

// setBytesLocked assumes s.mu is held.
func (s *Sock) setBytesLocked(opt int, b []byte) error {
	var ptr unsafe.Pointer
	if len(b) > 0 {
		ptr = unsafe.Pointer(&b[0])
	}
	if rc := C.zmq_setsockopt(s.ptr, C.int(opt), ptr, C.size_t(len(b))); rc != 0 {
		return lastError(fmt.Sprintf("zmq_setsockopt %d (bytes)", opt))
	}
	return nil
}

// Subscribe is a convenience for SetOption(ZMQ_SUBSCRIBE, prefix). An
// empty prefix matches every message, matching libzmq's convention.
func (s *Sock) Subscribe(prefix []byte) error {
	return s.SetOption(ZMQ_SUBSCRIBE, prefix)
}

// LastEndpoint returns ZMQ_LAST_ENDPOINT as a Go string. For a bind on an
// ephemeral port this yields the resolved endpoint
// (e.g. "tcp://127.0.0.1:34567").
func (s *Sock) LastEndpoint() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", errors.New("libzmq: LastEndpoint on closed socket")
	}
	buf := make([]byte, 256)
	sz := C.size_t(len(buf))
	if rc := C.zmq_getsockopt(s.ptr, C.int(ZMQ_LAST_ENDPOINT), unsafe.Pointer(&buf[0]), &sz); rc != 0 {
		return "", lastError("zmq_getsockopt ZMQ_LAST_ENDPOINT")
	}
	// libzmq returns a null-terminated string with sz including the NUL.
	if sz > 0 && buf[sz-1] == 0 {
		sz--
	}
	return string(buf[:sz]), nil
}
