package typed

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/rkm/gozmq"
)

// ErrNoTopicMatch is fed to the TopicMux's error handler when a received
// [topic, payload] message carries a topic that matches no registered
// prefix. Consumers can errors.Is against it in a WithErrorHandler
// callback.
var ErrNoTopicMatch = errors.New("typed: no handler registered for topic")

// ErrTopicMuxRunning is returned by Register and Unregister while
// TopicMux.Run is executing. Registration is allowed before the first
// Run and again after a Run returns.
var ErrTopicMuxRunning = errors.New("typed: topic mux is running")

// ErrTopicAlreadyRegistered is returned by Register when topic is
// already bound to a handler.
var ErrTopicAlreadyRegistered = errors.New("typed: topic already registered")

// TopicMux dispatches incoming messages on a SUB/XSUB socket to typed
// handlers registered by topic prefix. Topic matching uses prefix
// semantics mirroring ZMQ SUB filtering: a handler registered for
// "orders." matches "orders.created", "orders.cancelled", etc. When
// multiple prefixes match, the longest one wins.
//
// Registration is frozen while Run executes: Register or Unregister
// calls made mid-Run return ErrTopicMuxRunning. Once Run returns,
// registration is available again ahead of a subsequent Run.
type TopicMux struct {
	sock    *gozmq.Socket
	codec   Codec
	entries []muxEntry
	started atomic.Bool
}

type muxEntry struct {
	topic  string
	decode func([]byte) (any, error)
	invoke func(ctx context.Context, topic string, v any) error
}

// NewTopicMux returns a TopicMux bound to sock. sock should be a SUB or
// XSUB socket; Register subscribes topic prefixes on it.
func NewTopicMux(sock *gozmq.Socket, codec Codec) *TopicMux {
	return &TopicMux{sock: sock, codec: codec}
}

// Register binds topic (a prefix) to handler and subscribes sock to
// that prefix. Returns ErrTopicMuxRunning while Run is executing, or
// ErrTopicAlreadyRegistered if topic is already bound.
func Register[T any](
	r *TopicMux,
	topic string,
	handler func(context.Context, Message[T]) error,
) error {
	if r.started.Load() {
		return ErrTopicMuxRunning
	}
	if slices.ContainsFunc(r.entries, func(e muxEntry) bool { return e.topic == topic }) {
		return fmt.Errorf("%w: %q", ErrTopicAlreadyRegistered, topic)
	}

	entry := muxEntry{
		topic: topic,
		decode: func(b []byte) (any, error) {
			var v T
			if err := r.codec.Unmarshal(b, &v); err != nil {
				return nil, err
			}
			return v, nil
		},
		invoke: func(ctx context.Context, topic string, v any) error {
			return handler(ctx, Message[T]{Topic: topic, Value: v.(T)})
		},
	}

	if err := r.sock.Subscribe([]byte(topic)); err != nil {
		return err
	}

	// Insert preserving descending-length order so Run's linear scan
	// naturally picks the longest match first.
	if i := slices.IndexFunc(r.entries, func(e muxEntry) bool { return len(topic) > len(e.topic) }); i >= 0 {
		r.entries = slices.Insert(r.entries, i, entry)
	} else {
		r.entries = append(r.entries, entry)
	}
	return nil
}

// Unregister removes the handler for topic and unsubscribes sock from
// that prefix. Returns ErrTopicMuxRunning while Run is executing. Unknown
// topics are a no-op.
func (r *TopicMux) Unregister(topic string) error {
	if r.started.Load() {
		return ErrTopicMuxRunning
	}
	if i := slices.IndexFunc(r.entries, func(e muxEntry) bool { return e.topic == topic }); i >= 0 {
		r.entries = slices.Delete(r.entries, i, i+1)
		return r.sock.Unsubscribe([]byte(topic))
	}
	return nil
}

// Run blocks and dispatches messages received on sock to the
// registered handlers. Shape / decode / handler errors are passed to
// the configured WithErrorHandler, with classification via ErrDecode,
// ErrHandler, and ErrNoTopicMatch. Socket-level errors are always
// fatal; ctx cancellation returns ctx.Err().
//
// Calling Run a second time on the same TopicMux is supported only if
// the previous Run returned; concurrent Run calls are not supported.
func (r *TopicMux) Run(ctx context.Context, opts ...Option) error {
	r.started.Store(true)
	defer r.started.Store(false)

	// muxDispatch carries a decoded message through handleLoop: the
	// matched entry plus the decoded value, ready to invoke.
	type muxDispatch struct {
		topic string
		entry muxEntry
		value any
	}
	return handleLoop(ctx, r.sock, func(m *gozmq.Msg) (muxDispatch, error) {
		var d muxDispatch
		frames := m.Frames()
		if len(frames) != 2 {
			return d, fmt.Errorf("%w: expected [topic, payload], got %d frames: %w", ErrDecode, len(frames), ErrFrameCount)
		}
		d.topic = string(frames[0])
		entry, ok := r.match(d.topic)
		if !ok {
			return d, fmt.Errorf("%w: %q", ErrNoTopicMatch, d.topic)
		}
		v, err := entry.decode(frames[1])
		if err != nil {
			return d, fmt.Errorf("%w: topic %q: %w", ErrDecode, d.topic, err)
		}
		d.entry, d.value = entry, v
		return d, nil
	}, func(ctx context.Context, d muxDispatch) error {
		if err := d.entry.invoke(ctx, d.topic, d.value); err != nil {
			return fmt.Errorf("topic %q: %w", d.topic, err)
		}
		return nil
	}, apply(opts))
}

func (r *TopicMux) match(topic string) (muxEntry, bool) {
	for _, e := range r.entries {
		if strings.HasPrefix(topic, e.topic) {
			return e, true
		}
	}
	return muxEntry{}, false
}
