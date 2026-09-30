package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/pipeiface"
)

// TestInproc_OfferAfterCloseFails: an offer made after Close must fail. When
// both the buffered send and closeCh are ready, select picks at random, so
// the offer used to land in a channel nobody would ever drain about half the
// time, and the dialer believed it was connected.
func TestInproc_OfferAfterCloseFails(t *testing.T) {
	for i := 0; i < 200; i++ {
		tr := NewInprocTransport(0)
		l, err := tr.Listen(context.Background(), "inproc://offer-after-close")
		require.NoError(t, err)
		acc := tr.(pipeiface.DirectPipeDialer).LookupDirectAcceptor("inproc://offer-after-close")
		require.NotNil(t, acc)
		require.NoError(t, l.Close())

		dialer, binder := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
		err = acc.OfferDirectPipe(&pipeiface.DirectPipeOffer{BindPipe: binder})
		_ = dialer.Close()
		require.ErrorIs(t, err, net.ErrClosed, "iter %d: offer on a closed listener succeeded", i)
	}
}

// TestInproc_DialRacingCloseNeverStrands races Dial against Close. A Dial
// that reports success must hand back a conn whose peer is either accepted
// or closed; it must never sit in the accept queue after Close has drained
// it, which would leave the dialer's Read blocked forever.
func TestInproc_DialRacingCloseNeverStrands(t *testing.T) {
	for i := 0; i < 500; i++ {
		tr := NewInprocTransport(0)
		ep := fmt.Sprintf("inproc://dial-close-race-%d", i)
		l, err := tr.Listen(context.Background(), ep)
		require.NoError(t, err)
		// Take the listener directly so Dial cannot miss it in the name map
		// and fall into the connect-before-bind queue.
		il := l.(*inprocListener)

		var wg sync.WaitGroup
		wg.Add(1)
		start := make(chan struct{})
		go func() {
			defer wg.Done()
			<-start
			_ = l.Close()
		}()
		close(start)
		c, err := il.dialIn(context.Background(), strings.TrimPrefix(ep, "inproc://"))
		wg.Wait()
		if err != nil {
			continue
		}
		_ = c.SetDeadline(time.Now().Add(time.Second))
		_, err = c.Read(make([]byte, 1))
		_ = c.Close()
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatalf("iter %d: Dial succeeded but its conn was stranded in a closed listener's queue", i)
		}
	}
}

// TestInproc_DialHonorsCtxWhenBacklogFull: with the accept queue full and no
// one accepting, Dial must give up when its ctx does instead of blocking
// until the listener closes.
func TestInproc_DialHonorsCtxWhenBacklogFull(t *testing.T) {
	tr := NewInprocTransport(0)
	l, err := tr.Listen(context.Background(), "inproc://backlog-full")
	require.NoError(t, err)
	defer l.Close()

	var conns []Conn
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()
	for i := 0; i < cap(l.(*inprocListener).acceptCh); i++ {
		c, err := tr.Dial(context.Background(), "inproc://backlog-full")
		require.NoError(t, err)
		conns = append(conns, c)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		c, err := tr.Dial(ctx, "inproc://backlog-full")
		if c != nil {
			_ = c.Close()
		}
		errCh <- err
	}()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(2 * time.Second):
		_ = l.Close() // unblock the goroutine before failing
		<-errCh
		t.Fatal("Dial ignored ctx while the accept queue was full")
	}
}
