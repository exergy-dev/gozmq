package typed

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/exergy-dev/gozmq"
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
	wrapper

	// mu orders registration against Run's snapshot of entries: a
	// Register that saw started == false finishes before Run reads the
	// table, and one that starts after Run's snapshot sees started.
	mu      sync.Mutex
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
	return &TopicMux{wrapper: wrapper{sock, codec}}
}

// Register binds topic (a prefix) to handler and subscribes sock to
// that prefix. Returns ErrTopicMuxRunning while Run is executing, or
// ErrTopicAlreadyRegistered if topic is already bound.
func Register[T any](
	r *TopicMux,
	topic string,
	handler func(context.Context, Message[T]) error,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started.Load() {
		return ErrTopicMuxRunning
	}
	if slices.ContainsFunc(r.entries, func(e muxEntry) bool { return e.topic == topic }) {
		return fmt.Errorf("%w: %q", ErrTopicAlreadyRegistered, topic)
	}

	entry := muxEntry{
		topic: topic,
		decode: func(b []byte) (any, error) {
			v, err := unmarshal[T](r.codec, b)
			return v, err
		},
		invoke: func(ctx context.Context, topic string, v any) error {
			// v came from the decode closure directly above, which
			// always produces a T.
			return handler(ctx, Message[T]{Topic: topic, Value: v.(T)}) //nolint:errcheck // invariant, see above
		},
	}

	if err := r.sock.Subscribe([]byte(topic)); err != nil {
		return err
	}

	// Insert preserving descending-length order so Run's linear scan
	// naturally picks the longest match first.
	i := slices.IndexFunc(r.entries, func(e muxEntry) bool { return len(topic) > len(e.topic) })
	if i < 0 {
		i = len(r.entries)
	}
	r.entries = slices.Insert(r.entries, i, entry)
	return nil
}

// Unregister removes the handler for topic and unsubscribes sock from
// that prefix. Returns ErrTopicMuxRunning while Run is executing. Unknown
// topics are a no-op.
func (r *TopicMux) Unregister(topic string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
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
// Calling Run a second time on the same TopicMux is supported once the
// previous Run returned; a Run started while another is executing returns
// ErrTopicMuxRunning.
func (r *TopicMux) Run(ctx context.Context, opts ...Option) error {
	if !r.started.CompareAndSwap(false, true) {
		return ErrTopicMuxRunning
	}
	defer r.started.Store(false)
	// Registration is frozen from here until Run returns, so a snapshot
	// taken under mu stays current without locking per message.
	r.mu.Lock()
	entries := r.entries
	r.mu.Unlock()

	// muxDispatch carries a decoded message through handleLoop: the
	// matched entry plus the decoded value, ready to invoke.
	type muxDispatch struct {
		topic string
		entry muxEntry
		value any
	}
	return handleLoop(ctx, r.sock, func(m *gozmq.Msg) (muxDispatch, error) {
		var d muxDispatch
		topic, payload, err := topicPayload(m)
		if err != nil {
			return d, err
		}
		d.topic = string(topic)
		entry, ok := match(entries, d.topic)
		if !ok {
			return d, fmt.Errorf("%w: %q", ErrNoTopicMatch, d.topic)
		}
		v, err := entry.decode(payload)
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

// match returns the longest registered prefix of topic; entries is kept
// sorted by descending prefix length, so the first hit is the longest.
func match(entries []muxEntry, topic string) (muxEntry, bool) {
	for _, e := range entries {
		if strings.HasPrefix(topic, e.topic) {
			return e, true
		}
	}
	return muxEntry{}, false
}
