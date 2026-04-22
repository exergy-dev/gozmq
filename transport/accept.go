package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/rkm/gozmq/internal/zmqerr"
)

type acceptResult struct {
	c   net.Conn
	err error
}

func listenerClosedErr() error {
	return fmt.Errorf("%w: listener closed", zmqerr.ErrEndpointInvalid)
}

func acceptWithDeadline(
	ctx context.Context,
	l net.Listener,
	closeCh <-chan struct{},
	closed func() bool,
	pollInterval time.Duration,
	setDeadline func(time.Time) error,
	onAccept func(net.Conn) (net.Conn, error),
) (net.Conn, error) {
	if closed() {
		return nil, listenerClosedErr()
	}

	resCh := make(chan acceptResult, 1)
	go func() {
		for {
			if setDeadline != nil {
				_ = setDeadline(time.Now().Add(pollInterval))
			}
			c, err := l.Accept()
			if err != nil {
				if closed() {
					resCh <- acceptResult{err: listenerClosedErr()}
					return
				}
				if isTimeout(err) {
					select {
					case <-ctx.Done():
						resCh <- acceptResult{err: ctx.Err()}
						return
					case <-closeCh:
						resCh <- acceptResult{err: listenerClosedErr()}
						return
					default:
						continue
					}
				}
				resCh <- acceptResult{err: err}
				return
			}
			if setDeadline != nil {
				_ = setDeadline(time.Time{})
			}
			if onAccept != nil {
				wrapped, err := onAccept(c)
				if err != nil {
					_ = c.Close()
					resCh <- acceptResult{err: err}
					return
				}
				c = wrapped
			}
			resCh <- acceptResult{c: c}
			return
		}
	}()

	select {
	case r := <-resCh:
		return r.c, r.err
	case <-ctx.Done():
		go closeAcceptedWhenReady(resCh)
		return nil, ctx.Err()
	case <-closeCh:
		go closeAcceptedWhenReady(resCh)
		return nil, listenerClosedErr()
	}
}

func closeAcceptedWhenReady(resCh <-chan acceptResult) {
	r := <-resCh
	if r.c != nil {
		_ = r.c.Close()
	}
}

// isTimeout reports whether err is a net.Error timeout. Used by the Accept
// poll loop to distinguish "no peer yet" from real errors.
func isTimeout(err error) bool {
	var ne net.Error
	if errors.As(err, &ne) {
		return ne.Timeout()
	}
	return false
}
