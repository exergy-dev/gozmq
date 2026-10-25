package gozmq_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/typed"
)

// Stress tests for the receive buffer pool at its default (on): large
// frames are lent and recycled on Release, so every consumer inside the
// library is exercised with a sender that keeps sending while receivers
// release, and each payload is verified byte for byte against a
// deterministic per-message pattern, so a recycled-and-overwritten buffer
// is detected. Run with -race.

// stressShapes are the frame layouts under test (sizes in bytes). The
// first byte of the first frame is 'T' (so PUB/SUB prefix filters match)
// followed by the message number; see stressMsg.
var stressShapes = []struct {
	name  string
	sizes []int
}{
	{"8KiB", []int{8 << 10}},
	{"64KiB", []int{64 << 10}},
	{"multipart", []int{32, 64 << 10, 200}},
}

const stressN = 300

// stressFrames builds message i with the given frame sizes: frame 0 starts
// with 'T' and i (big endian), every other byte is a function of (i, frame,
// offset).
func stressFrames(i int, sizes []int) [][]byte {
	frames := make([][]byte, len(sizes))
	for k, size := range sizes {
		b := make([]byte, size)
		for j := range b {
			b[j] = byte(i*131 + k*17 + j*7 + j>>8)
		}
		frames[k] = b
	}
	frames[0][0] = 'T'
	binary.BigEndian.PutUint32(frames[0][1:5], uint32(i))
	return frames
}

// stressSeq extracts the message number from frames (which must be the
// message's own frames, without any envelope).
func stressSeq(frames [][]byte) (int, bool) {
	if len(frames) == 0 || len(frames[0]) < 5 || frames[0][0] != 'T' {
		return 0, false
	}
	return int(binary.BigEndian.Uint32(frames[0][1:5])), true
}

// stressCheck verifies frames against the pattern for their embedded
// message number and returns it. Frames beyond the shape's count at the
// front (an envelope) are the caller's to strip.
func stressCheck(frames [][]byte, sizes []int) (int, error) {
	if len(frames) != len(sizes) {
		return 0, fmt.Errorf("got %d frames, want %d", len(frames), len(sizes))
	}
	i, ok := stressSeq(frames)
	if !ok {
		return 0, fmt.Errorf("frame 0 has no sequence header")
	}
	for k, want := range stressFrames(i, sizes) {
		if !bytes.Equal(want, frames[k]) {
			return i, fmt.Errorf("message %d frame %d corrupted", i, k)
		}
	}
	return i, nil
}

var stressEndpoint atomic.Int64

// stressBind binds s on a fresh endpoint of the given transport.
func stressBind(t *testing.T, s *gozmq.Socket, transport string) string {
	t.Helper()
	ep := "tcp://127.0.0.1:0"
	if transport == "inproc" {
		ep = fmt.Sprintf("inproc://stress-%d", stressEndpoint.Add(1))
	}
	require.NoError(t, s.Bind(ep))
	return s.LastEndpoint()
}

// stressSockets creates sockets of the given types in one context with the
// default pooling (no WithRecvBufferPool option) and generous HWMs.
type stressEnv struct {
	t    *testing.T
	zctx *gozmq.Context
}

func newStressEnv(t *testing.T) *stressEnv {
	t.Helper()
	verifyNoLeaks(t)
	zctx := gozmq.NewContext()
	t.Cleanup(func() { _ = zctx.Close() })
	return &stressEnv{t: t, zctx: zctx}
}

func (e *stressEnv) socket(typ gozmq.SocketType, opts ...gozmq.Option) *gozmq.Socket {
	e.t.Helper()
	opts = append([]gozmq.Option{
		gozmq.WithLinger(0),
		gozmq.WithSendHWM(10 * stressN),
		gozmq.WithRecvHWM(10 * stressN),
	}, opts...)
	s, err := e.zctx.NewSocket(typ, opts...)
	require.NoError(e.t, err)
	e.t.Cleanup(func() { _ = s.Close() })
	return s
}

// forStress runs fn for every transport x shape.
func forStress(t *testing.T, fn func(t *testing.T, transport string, sizes []int)) {
	for _, transport := range []string{"tcp", "inproc"} {
		for _, shape := range stressShapes {
			t.Run(transport+"/"+shape.name, func(t *testing.T) {
				fn(t, transport, shape.sizes)
			})
		}
	}
}

func stressCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// stressRecvVerify receives n messages from s, checking sequence order and
// bytes, releasing each.
func stressRecvVerify(ctx context.Context, s *gozmq.Socket, sizes []int, n int) error {
	for want := 0; want < n; want++ {
		m, err := s.RecvMsg(ctx)
		if err != nil {
			return fmt.Errorf("recv %d: %w", want, err)
		}
		i, err := stressCheck(m.Frames(), sizes)
		m.Release()
		if err != nil {
			return err
		}
		if i != want {
			return fmt.Errorf("got message %d, want %d", i, want)
		}
	}
	return nil
}

func TestStress_PushPull(t *testing.T) {
	forStress(t, func(t *testing.T, transport string, sizes []int) {
		env := newStressEnv(t)
		ctx := stressCtx(t)
		pull := env.socket(gozmq.Pull)
		push := env.socket(gozmq.Push)
		require.NoError(t, push.Connect(stressBind(t, pull, transport)))

		go func() {
			for i := 0; i < stressN; i++ {
				if push.SendMsg(ctx, gozmq.NewMsgFromFrames(stressFrames(i, sizes)...)) != nil {
					return
				}
			}
		}()
		require.NoError(t, stressRecvVerify(ctx, pull, sizes, stressN))
	})
}

func TestStress_ReqRepEcho(t *testing.T) {
	forStress(t, func(t *testing.T, transport string, sizes []int) {
		env := newStressEnv(t)
		ctx := stressCtx(t)
		rep := env.socket(gozmq.Rep)
		req := env.socket(gozmq.Req)
		require.NoError(t, req.Connect(stressBind(t, rep, transport)))

		repErr := make(chan error, 1)
		go func() {
			for i := 0; i < stressN; i++ {
				m, err := rep.RecvMsg(ctx)
				if err != nil {
					repErr <- err
					return
				}
				if _, err := stressCheck(m.Frames(), sizes); err != nil {
					m.Release()
					repErr <- fmt.Errorf("rep: %w", err)
					return
				}
				// Echo the very message received: SendMsg holds its own
				// reference, so releasing straight away is correct.
				err = rep.SendMsg(ctx, m)
				m.Release()
				if err != nil {
					repErr <- err
					return
				}
			}
			repErr <- nil
		}()
		for i := 0; i < stressN; i++ {
			require.NoError(t, req.SendMsg(ctx, gozmq.NewMsgFromFrames(stressFrames(i, sizes)...)))
			m, err := req.RecvMsg(ctx)
			require.NoError(t, err)
			got, err := stressCheck(m.Frames(), sizes)
			m.Release()
			require.NoError(t, err)
			require.Equal(t, i, got)
		}
		require.NoError(t, <-repErr)
	})
}

func TestStress_RouterDealer(t *testing.T) {
	forStress(t, func(t *testing.T, transport string, sizes []int) {
		env := newStressEnv(t)
		ctx := stressCtx(t)
		router := env.socket(gozmq.Router)
		dealer := env.socket(gozmq.Dealer, gozmq.WithIdentity([]byte("stress-dealer")))
		require.NoError(t, dealer.Connect(stressBind(t, router, transport)))

		go func() {
			for i := 0; i < stressN; i++ {
				if dealer.SendMsg(ctx, gozmq.NewMsgFromFrames(stressFrames(i, sizes)...)) != nil {
					return
				}
			}
		}()
		routerErr := make(chan error, 1)
		go func() {
			for i := 0; i < stressN; i++ {
				m, err := router.RecvMsg(ctx)
				if err != nil {
					routerErr <- err
					return
				}
				frames := m.Frames()
				if len(frames) < 1 || string(frames[0]) != "stress-dealer" {
					m.Release()
					routerErr <- fmt.Errorf("bad envelope %q", frames)
					return
				}
				if _, err := stressCheck(frames[1:], sizes); err != nil {
					m.Release()
					routerErr <- fmt.Errorf("router: %w", err)
					return
				}
				// Reply by routing the received message back as is.
				err = router.SendMsg(ctx, m)
				m.Release()
				if err != nil {
					routerErr <- err
					return
				}
			}
			routerErr <- nil
		}()
		require.NoError(t, stressRecvVerify(ctx, dealer, sizes, stressN))
		require.NoError(t, <-routerErr)
	})
}

func TestStress_PubSub(t *testing.T) {
	forStress(t, func(t *testing.T, transport string, sizes []int) {
		env := newStressEnv(t)
		ctx := stressCtx(t)
		pub := env.socket(gozmq.Pub)
		hit := env.socket(gozmq.Sub)
		miss := env.socket(gozmq.Sub)
		ep := stressBind(t, pub, transport)
		require.NoError(t, hit.Subscribe([]byte("T")))
		require.NoError(t, miss.Subscribe([]byte("X")))
		require.NoError(t, hit.Connect(ep))
		require.NoError(t, miss.Connect(ep))

		// Probe until the matching subscription has propagated, then drain
		// the probes (single-frame, easily told from the test messages).
		probe := []byte("Tprobe")
		for got := false; !got; {
			require.NoError(t, pub.SendBytes(ctx, probe))
			pctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			m, err := hit.RecvMsg(pctx)
			cancel()
			got = err == nil
			if m != nil {
				m.Release()
			}
		}
		time.Sleep(100 * time.Millisecond)
		for {
			pctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			m, err := hit.RecvMsg(pctx)
			cancel()
			if err != nil {
				break
			}
			m.Release()
		}

		go func() {
			for i := 0; i < stressN; i++ {
				if pub.SendMsg(ctx, gozmq.NewMsgFromFrames(stressFrames(i, sizes)...)) != nil {
					return
				}
			}
		}()
		require.NoError(t, stressRecvVerify(ctx, hit, sizes, stressN))

		mctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		m, err := miss.RecvMsg(mctx)
		if err == nil {
			m.Release()
		}
		require.Error(t, err, "subscriber whose filter misses received a message")
	})
}

func TestStress_ProxyWithCapture(t *testing.T) {
	forStress(t, func(t *testing.T, transport string, sizes []int) {
		env := newStressEnv(t)
		ctx, cancel := context.WithCancel(stressCtx(t))
		defer cancel()

		frontend := env.socket(gozmq.Router)
		backend := env.socket(gozmq.Dealer)
		capPull := env.socket(gozmq.Pull)
		capture := env.socket(gozmq.Push)
		worker := env.socket(gozmq.Rep)
		client := env.socket(gozmq.Dealer)
		require.NoError(t, capture.Connect(stressBind(t, capPull, transport)))
		require.NoError(t, worker.Connect(stressBind(t, backend, transport)))
		require.NoError(t, client.Connect(stressBind(t, frontend, transport)))

		var wg sync.WaitGroup
		wg.Add(3)
		go func() { // the proxy
			defer wg.Done()
			_ = gozmq.Proxy(ctx, frontend, backend, capture)
		}()
		go func() { // REP worker echoing every request
			defer wg.Done()
			for {
				m, err := worker.RecvMsg(ctx)
				if err != nil {
					return
				}
				err = worker.SendMsg(ctx, m)
				m.Release()
				if err != nil {
					return
				}
			}
		}()
		var captured, bad atomic.Int64
		go func() { // capture sink: whatever it sees must be intact
			defer wg.Done()
			for {
				m, err := capPull.RecvMsg(ctx)
				if err != nil {
					return
				}
				frames := m.Frames()
				// [envelope..., empty delimiter, shape frames...]
				if len(frames) >= len(sizes) {
					if _, err := stressCheck(frames[len(frames)-len(sizes):], sizes); err != nil {
						bad.Add(1)
					} else {
						captured.Add(1)
					}
				}
				m.Release()
			}
		}()

		sendErr := make(chan error, 1)
		go func() {
			for i := 0; i < stressN; i++ {
				frames := append([][]byte{nil}, stressFrames(i, sizes)...)
				if err := client.SendMsg(ctx, gozmq.NewMsgFromFrames(frames...)); err != nil {
					sendErr <- err
					return
				}
			}
			sendErr <- nil
		}()
		for want := 0; want < stressN; want++ {
			m, err := client.RecvMsg(ctx)
			require.NoError(t, err)
			frames := m.Frames()
			require.Len(t, frames, len(sizes)+1)
			require.Empty(t, frames[0])
			i, err := stressCheck(frames[1:], sizes)
			m.Release()
			require.NoError(t, err)
			require.Equal(t, want, i)
		}
		require.NoError(t, <-sendErr)

		// Let the capture sink drain, then stop everything.
		require.Eventually(t, func() bool { return captured.Load() > 0 }, 10*time.Second, 10*time.Millisecond)
		cancel()
		wg.Wait()
		require.Zero(t, bad.Load(), "captured messages were corrupted")
	})
}

// TestStress_RecvBytes64KiB: RecvBytes copies before its release, so the
// results stay intact however many later receives recycle the buffers.
func TestStress_RecvBytes64KiB(t *testing.T) {
	env := newStressEnv(t)
	ctx := stressCtx(t)
	pull := env.socket(gozmq.Pull)
	push := env.socket(gozmq.Push)
	require.NoError(t, push.Connect(stressBind(t, pull, "tcp")))
	sizes := []int{64 << 10}

	go func() {
		for i := 0; i < stressN; i++ {
			if push.SendMsg(ctx, gozmq.NewMsgFromFrames(stressFrames(i, sizes)...)) != nil {
				return
			}
		}
	}()
	got := make([][]byte, stressN)
	for i := range got {
		b, err := pull.RecvBytes(ctx)
		require.NoError(t, err)
		got[i] = b
	}
	for i, b := range got {
		_, err := stressCheck([][]byte{b}, sizes)
		require.NoErrorf(t, err, "RecvBytes result %d changed after later receives", i)
	}
}

// TestStress_TypedRoundTrip64KiB: typed Sender/Receiver and
// Publisher/Subscriber decode 64 KiB values and release the message; the
// decoded values must not alias the recycled frame.
func TestStress_TypedRoundTrip64KiB(t *testing.T) {
	type blob struct {
		N    int    `json:"n"`
		Data []byte `json:"data"`
	}
	mk := func(i int) blob { return blob{N: i, Data: stressFrames(i, []int{64 << 10})[0]} }

	for _, transport := range []string{"tcp", "inproc"} {
		t.Run(transport+"/pushpull", func(t *testing.T) {
			env := newStressEnv(t)
			ctx := stressCtx(t)
			pull := env.socket(gozmq.Pull)
			push := env.socket(gozmq.Push)
			require.NoError(t, push.Connect(stressBind(t, pull, transport)))
			snd := typed.NewSender[blob](push, typed.JSONCodec{})
			rcv := typed.NewReceiver[blob](pull, typed.JSONCodec{})

			go func() {
				for i := 0; i < 100; i++ {
					if snd.Send(ctx, mk(i)) != nil {
						return
					}
				}
			}()
			got := make([]blob, 100)
			for i := range got {
				v, err := rcv.Recv(ctx)
				require.NoError(t, err)
				got[i] = v
			}
			for i, v := range got { // checked only after all later receives
				require.Equalf(t, mk(i), v, "value %d", i)
			}
		})
		t.Run(transport+"/pubsub", func(t *testing.T) {
			env := newStressEnv(t)
			ctx := stressCtx(t)
			pub := env.socket(gozmq.Pub)
			sub := env.socket(gozmq.Sub)
			ep := stressBind(t, pub, transport)
			require.NoError(t, sub.Subscribe([]byte("t")))
			require.NoError(t, sub.Connect(ep))
			p := typed.NewPublisher[blob](pub, typed.JSONCodec{})
			s := typed.NewSubscriber[blob](sub, typed.JSONCodec{})

			// Probe until the subscription is live; probes have N == -1.
			for live := false; !live; {
				require.NoError(t, p.Send(ctx, "t", blob{N: -1}))
				pctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
				_, err := s.Recv(pctx)
				cancel()
				live = err == nil
			}
			time.Sleep(100 * time.Millisecond)

			go func() {
				for i := 0; i < 100; i++ {
					if p.Send(ctx, "t", mk(i)) != nil {
						return
					}
				}
			}()
			var got []blob
			for len(got) < 100 {
				v, err := s.Recv(ctx)
				require.NoError(t, err)
				if v.Value.N >= 0 { // a late probe
					got = append(got, v.Value)
				}
			}
			for i, v := range got {
				require.Equalf(t, mk(i), v, "value %d", i)
			}
		})
	}
}

// TestStress_RetainedFrameSurvivesGCDefault: at the default (pool on) a
// frame slice kept after its *Msg is dropped without Release must stay
// intact across forced collections and further receives (no finalizer
// recycles it under the slice).
func TestStress_RetainedFrameSurvivesGCDefault(t *testing.T) {
	env := newStressEnv(t)
	ctx := stressCtx(t)
	pull := env.socket(gozmq.Pull)
	push := env.socket(gozmq.Push)
	require.NoError(t, push.Connect(stressBind(t, pull, "tcp")))
	sizes := []int{64 << 10}

	var kept [][]byte
	for i := 0; i < 40; i++ {
		require.NoError(t, push.SendMsg(ctx, gozmq.NewMsgFromFrames(stressFrames(i, sizes)...)))
		m, err := pull.RecvMsg(ctx)
		require.NoError(t, err)
		require.True(t, m.Lent(), "64 KiB frame not lent at the default")
		kept = append(kept, m.Bytes()) // the *Msg is dropped, not released
		m = nil
		runtime.GC()
		runtime.GC()
		for j := 0; j < 3; j++ {
			require.NoError(t, push.SendMsg(ctx, gozmq.NewMsgFromFrames(stressFrames(1000+j, sizes)...)))
			m, err := pull.RecvMsg(ctx)
			require.NoError(t, err)
			m.Release()
		}
	}
	for i, f := range kept {
		got, err := stressCheck([][]byte{f}, sizes)
		require.NoErrorf(t, err, "retained frame %d changed", i)
		require.Equal(t, i, got)
	}
}
