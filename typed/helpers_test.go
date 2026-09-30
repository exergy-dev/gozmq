package typed_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
)

// syncSub waits for sub's subscription to topic to reach pub without a
// fixed sleep, mirroring the root package's helper of the same name. It
// publishes sequence-numbered single-frame probes on topic until one is
// delivered, then drains every later probe; the pipe is FIFO, so nothing
// of the probe stream is left queued on return. Call it after Connect and
// before publishing (or starting a dispatch loop on sub).
func syncSub(t *testing.T, pub, sub *gozmq.Socket, topic string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	probe := func(i int) string { return fmt.Sprintf("%s\x00typed-sync-probe\x00%d", topic, i) }
	last, got := -1, ""
	for got == "" {
		last++
		if err := pub.SendMsg(ctx, gozmq.NewMsgFromBytes([]byte(probe(last)))); err != nil {
			t.Fatalf("syncSub probe send %d: %v", last, err)
		}
		tryCtx, tryCancel := context.WithTimeout(ctx, 25*time.Millisecond)
		m, err := sub.RecvMsg(tryCtx)
		tryCancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				continue
			}
			t.Fatalf("syncSub probe recv: %v", err)
		}
		got = string(m.Bytes())
		m.Release()
	}
	for want := probe(last); got != want; {
		m, err := sub.RecvMsg(ctx)
		if err != nil {
			t.Fatalf("syncSub drain recv: %v", err)
		}
		got = string(m.Bytes())
		m.Release()
	}
}
