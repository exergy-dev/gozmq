package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
)

// listenerClosedErr is returned by Accept once the listener is closed. It
// matches net.ErrClosed, the terminal condition documented on
// Listener.Accept, as well as ErrEndpointInvalid.
func listenerClosedErr() error {
	return fmt.Errorf("%w: listener closed: %w", zmqerr.ErrEndpointInvalid, net.ErrClosed)
}

// aLongTimeAgo is a non-zero deadline in the past; setting it on a listener
// wakes a blocked Accept immediately.
var aLongTimeAgo = time.Unix(1, 0)

// netListener adapts a stream net.Listener (tcp, tls+tcp, ipc) to Listener.
//
// Accept blocks in the underlying Accept. Close unblocks it by closing the
// socket; ctx cancellation unblocks it by setting a past deadline on the
// listener (both *net.TCPListener and *net.UnixListener support that).
type netListener struct {
	l        net.Listener
	endpoint string
	// wrap adapts an accepted conn to the scheme's Conn. It runs on the
	// accept path, so it must not block (in particular: no TLS handshake).
	wrap func(net.Conn) Conn

	// sem admits one Accept at a time. The cancellation deadline lives on
	// the shared listener, so concurrent Accepts with different ctxs would
	// otherwise wake (and could wedge) each other.
	sem       chan struct{}
	closeOnce sync.Once
}

func newNetListener(l net.Listener, endpoint string, wrap func(net.Conn) Conn) *netListener {
	return &netListener{l: l, endpoint: endpoint, wrap: wrap, sem: make(chan struct{}, 1)}
}

func (l *netListener) Endpoint() string { return l.endpoint }

func (l *netListener) Accept(ctx context.Context) (Conn, error) {
	select {
	case l.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-l.sem }()

	dl, _ := l.l.(interface{ SetDeadline(time.Time) error })
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		stop := func() bool { return true }
		if dl != nil {
			// Clear any deadline a previous Accept's cancellation left behind.
			_ = dl.SetDeadline(time.Time{})
			stop = context.AfterFunc(ctx, func() { _ = dl.SetDeadline(aLongTimeAgo) })
		}
		c, err := l.l.Accept()
		stop()
		switch {
		case err == nil:
			return l.wrap(c), nil
		case errors.Is(err, net.ErrClosed):
			return nil, listenerClosedErr()
		case errors.Is(err, os.ErrDeadlineExceeded):
			// Our ctx fired (the loop head returns its error), or a late
			// AfterFunc from an earlier Accept's ctx did; retry.
			continue
		default:
			return nil, err
		}
	}
}

// Close closes the socket, waking any blocked Accept with a net.ErrClosed
// error. Idempotent: later calls return nil.
func (l *netListener) Close() error {
	var err error
	l.closeOnce.Do(func() { err = l.l.Close() })
	return err
}

var _ Listener = (*netListener)(nil)
