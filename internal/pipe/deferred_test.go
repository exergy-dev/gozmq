package pipe

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/msg"
)

func TestDeferred_VisibleBeforeFlush(t *testing.T) {
	t.Parallel()
	a, b := NewPair(10, 10, nil, nil)
	defer a.Close()
	defer b.Close()
	require.True(t, a.TrySendDeferred(msg.FromBytes([]byte("1"))))
	require.True(t, a.TrySendDeferred(msg.FromBytes([]byte("2"))))
	require.True(t, b.Pending())
	require.Equal(t, 2, b.Len())
	m, ok := b.TryRecv()
	require.True(t, ok)
	require.Equal(t, "1", string(m.Bytes()))
	a.Flush()
	m, ok = b.TryRecv()
	require.True(t, ok)
	require.Equal(t, "2", string(m.Bytes()))
}

func TestDeferred_FlushWakesParkedRecv(t *testing.T) {
	t.Parallel()
	a, b := NewPair(10, 10, nil, nil)
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := testutil.Async(func() error {
		_, err := b.RecvOr(ctx, nil)
		return err
	})
	for b.recv.rsleepers.Load() == 0 { // wait until parked
		time.Sleep(time.Millisecond)
	}
	require.True(t, a.TrySendDeferred(msg.FromBytes([]byte("x"))))
	testutil.RequireNoRecv(t, got, 50*time.Millisecond, "receiver woke before Flush")
	a.Flush()
	require.NoError(t, testutil.RequireRecv(t, got, 5*time.Second, "Flush did not wake the receiver"))
}

func TestDeferred_FlushNothingPendingIsNoop(t *testing.T) {
	t.Parallel()
	a, b := NewPair(10, 10, nil, nil)
	defer a.Close()
	defer b.Close()
	gate := notifyGate(b)
	require.False(t, b.MarkIdle())
	a.Flush() // nothing owed: must not notify
	requireNotArmed(t, gate, "Flush with nothing pending")
	a.Flush()
	require.False(t, b.Pending())
}

func TestDeferred_FlushRunsIdleNotify(t *testing.T) {
	t.Parallel()
	a, b := NewPair(10, 10, nil, nil)
	defer a.Close()
	defer b.Close()
	gate := notifyGate(b)
	_, ok := b.RecvOrIdle()
	require.False(t, ok)
	require.True(t, a.TrySendDeferred(msg.FromBytes([]byte("x"))))
	requireNotArmed(t, gate, "before Flush")
	a.Flush()
	requireArmed(t, gate, "after Flush")
	a.Flush()
	requireNotArmed(t, gate, "second Flush")
}

func TestDeferred_HWMFull(t *testing.T) {
	t.Parallel()
	a, b := NewPair(2, 2, nil, nil)
	defer a.Close()
	defer b.Close()
	require.True(t, a.TrySendDeferred(msg.FromBytes([]byte("1"))))
	require.True(t, a.TrySendDeferred(msg.FromBytes([]byte("2"))))
	require.False(t, a.TrySendDeferred(msg.FromBytes([]byte("3"))))
	a.Flush()
	_, ok := b.TryRecv()
	require.True(t, ok)
	require.True(t, a.TrySendDeferred(msg.FromBytes([]byte("3"))))
	a.Flush()
}

func TestDeferred_CloseWhileFlushOwed(t *testing.T) {
	t.Parallel()
	a, b := NewPair(10, 10, nil, nil)
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type res struct {
		m   *msg.Msg
		err error
	}
	got := make(chan res, 2)
	go func() {
		for range 2 {
			m, err := b.RecvOr(ctx, nil)
			got <- res{m, err}
		}
	}()
	for b.recv.rsleepers.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	require.True(t, a.TrySendDeferred(msg.FromBytes([]byte("x"))))
	require.NoError(t, a.Close()) // wakes the parked receiver without Flush
	r := <-got
	require.NoError(t, r.err, "queued message survives close")
	r = <-got
	require.ErrorIs(t, r.err, io.EOF)
	require.False(t, a.TrySendDeferred(msg.FromBytes([]byte("y"))))
	a.Flush() // must not panic or block
}
