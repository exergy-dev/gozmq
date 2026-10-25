package gozmq

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/exergy-dev/gozmq/security"
	"github.com/exergy-dev/gozmq/security/curve"
)

// Option configures a Socket at construction time; construct values via the
// With* helpers. Options that are inapplicable to
// the socket type are rejected with an error at Socket construction rather
// than silently ignored: pattern-specific options (WithRouterMandatory,
// WithXPubVerbose, WithSubscribe) on the wrong pattern, and ZMTP-layer
// options (security, heartbeat, identity, handshake timeout, ZAP domain)
// on STREAM sockets, which bypass ZMTP entirely.
type Option func(*socketConfig) error

type socketConfig struct {
	identity             []byte
	sendHWM              int
	recvHWM              int
	linger               time.Duration
	reconnectInterval    time.Duration
	reconnectIntervalMax time.Duration
	heartbeatInterval    time.Duration
	heartbeatTimeout     time.Duration
	maxMessageSize       int64
	routerMandatory      bool
	xpubVerbose          bool
	recvBufferPool       *bool // nil = default (on, except STREAM)
	subscriptions        [][]byte
	handshakeTimeout     time.Duration
	zapDomain            string

	// Set when the option was applied, so STREAM can reject explicit use.
	heartbeatSet        bool
	handshakeTimeoutSet bool

	// mech is the security mechanism set by the last mechanism option;
	// mechConflict records a second, different one for validateSocketConfig.
	mech         *mechSpec
	mechConflict error

	monitor     chan<- Event
	monitorMask EventType

	// logger sinks internal diagnostic events (handshake failures,
	// reconnect attempts, heartbeat timeouts). nil lets the library
	// pick slog.Default; set via WithLogger for reproducible test
	// logs and structured context.
	logger *slog.Logger
}

// mechSpec is a configured security mechanism: the option's name (for
// errors) and how to build one mechanism per session. Each option checks its
// own arguments, so a spec that is set can always be built.
type mechSpec struct {
	name  string
	build func(role security.Role, meta map[string]string) (security.Mech, error)
}

// setMech records spec as the socket's mechanism. Applying a different
// mechanism option as well is a conflict that validateSocketConfig reports;
// repeating the same option replaces it.
func (c *socketConfig) setMech(spec *mechSpec) {
	if c.mech != nil && c.mech.name != spec.name && c.mechConflict == nil {
		c.mechConflict = fmt.Errorf("gozmq: %s and %s are mutually exclusive", c.mech.name, spec.name)
	}
	c.mech = spec
}

func newSocketConfig() *socketConfig {
	return &socketConfig{
		sendHWM:              1000,
		recvHWM:              1000,
		linger:               time.Second,
		reconnectInterval:    100 * time.Millisecond,
		reconnectIntervalMax: 30 * time.Second,
		heartbeatInterval:    30 * time.Second,
		heartbeatTimeout:     90 * time.Second,
		maxMessageSize:       64 << 20,
		handshakeTimeout:     30 * time.Second,
	}
}

// WithIdentity sets the ROUTER/DEALER identity advertised during the ZMTP
// handshake. The identity is the peer-visible name by which a ROUTER
// addresses this DEALER (or by which a ROUTER peer appears in the leading
// frame of a routed message). Identity bytes must be either empty (the
// default: a ROUTER peer then synthesizes a 5-byte identity with a zero
// leading byte) or 1..255 bytes long with a non-zero leading byte; longer
// or zero-prefixed identities are rejected by NewSocket.
// The slice is copied defensively.
func WithIdentity(id []byte) Option {
	return func(c *socketConfig) error {
		c.identity = bytes.Clone(id)
		return nil
	}
}

// WithSendHWM sets the outbound high-water mark — the maximum number of
// messages queued per peer pipe before backpressure engages. Default is
// 1000, maximum [MaxHWM]. Zero disables the limit (unbounded growth; not
// recommended for untrusted peers). The queue grows with the actual depth
// in fixed-size chunks, so a large HWM costs nothing until messages are
// queued. When the HWM is reached, Send behavior depends on the
// socket pattern: blocking sockets (PAIR, PUSH, DEALER, REQ, REP, ROUTER)
// wait for a Recv on the peer to drain queue space, while PUB/XPUB drop
// the message for the full peer and continue the fan-out (the drop is
// counted in [Socket.Metrics]).
func WithSendHWM(n int) Option {
	return func(c *socketConfig) error { c.sendHWM = n; return nil }
}

// WithRecvHWM sets the inbound high-water mark — the maximum number of
// messages buffered per peer pipe before the reader goroutine stops
// accepting frames from the wire and exerts TCP backpressure on the peer.
// Default is 1000, maximum [MaxHWM]. Zero disables the limit. As with
// [WithSendHWM], storage grows with the actual queue depth, not the HWM.
//
// On inproc, where one pipe joins the two sockets, each direction holds the
// sender's send HWM plus the receiver's recv HWM, and is unlimited if
// either is zero (as in libzmq).
func WithRecvHWM(n int) Option {
	return func(c *socketConfig) error { c.recvHWM = n; return nil }
}

// WithLinger sets the grace period Socket.Close waits for outbound pipes
// to flush queued messages to the wire. Default is 1 second. Zero
// discards any queued messages immediately on Close. A negative duration
// blocks Close indefinitely until every peer has drained, matching
// libzmq's infinite-linger behavior — this can deadlock Close if a peer
// never drains or reconnects, so pair it with [Socket.CloseCtx], whose
// context bounds the wait.
func WithLinger(d time.Duration) Option {
	return func(c *socketConfig) error { c.linger = d; return nil }
}

// WithReconnectInterval sets the base delay between reconnection attempts
// on a client-side socket after a dial failure or post-handshake
// disconnect. Default is 100ms; zero selects the default rather than an
// immediate retry. The actual delay grows exponentially and is jittered by
// 20% before each attempt, capped by [WithReconnectIntervalMax].
func WithReconnectInterval(d time.Duration) Option {
	return func(c *socketConfig) error { c.reconnectInterval = d; return nil }
}

// WithReconnectIntervalMax caps the reconnection backoff. Default is 30
// seconds. Zero removes the cap: backoff doubles without bound, so after
// a long outage a socket may wait arbitrarily long between attempts.
// NOTE this diverges from libzmq, where ZMQ_RECONNECT_IVL_MAX=0 (its
// default) means "no exponential backoff, constant interval".
func WithReconnectIntervalMax(d time.Duration) Option {
	return func(c *socketConfig) error { c.reconnectIntervalMax = d; return nil }
}

// WithHeartbeat configures the ZMTP 3.1 heartbeat. interval is how often
// PING commands are sent on an otherwise-idle session; timeout is the
// duration without any inbound frame (data or PONG) after which the peer
// is considered dead and the session is torn down. Defaults: 30s interval,
// 90s timeout. A timeout of 0 means 3× interval; an interval of 0
// disables heartbeats. Heartbeats are disabled automatically when the peer
// advertises ZMTP 3.0 in the greeting. Heartbeating is the only reliable
// way to detect a half-open TCP connection (e.g. a peer killed without a
// FIN, or a NAT that dropped the flow) — disabling it is discouraged on
// long-lived connections.
func WithHeartbeat(interval, timeout time.Duration) Option {
	return func(c *socketConfig) error {
		c.heartbeatInterval = interval
		c.heartbeatTimeout = timeout
		c.heartbeatSet = true
		return nil
	}
}

// WithMaxMessageSize sets the per-message byte limit: the total length of
// all frames of one logical message. Default is 64 MiB. (libzmq's
// ZMQ_MAXMSGSIZE is per frame; gozmq bounds the whole message on both
// sides so that a sender's check and a receiver's check agree.) It is
// enforced in both directions, with different surfacing:
//
//   - Send side: SendMsg/SendBytes sum the frames up front and return a
//     synchronous error wrapping [ErrMalformedFrame] on overflow; the
//     session is NOT torn down.
//   - Recv side: a peer whose message exceeds the limit has its session
//     torn down. The read loop detects it asynchronously of the
//     application's call path, so it surfaces as an EventDisconnected
//     (with no reason) and a log line through [WithLogger], not as a
//     Recv error.
//
// For CURVE the limit applies to the plaintext; the session allows for the
// mechanism's per-frame overhead on the wire. Memory for an inbound frame is
// allocated as its bytes arrive, so a peer that announces a huge frame and
// stalls costs little; the cap bounds what a completed message may occupy.
//
// Pass 0 to disable the limit — NOT recommended for untrusted peers.
func WithMaxMessageSize(n int64) Option {
	return func(c *socketConfig) error { c.maxMessageSize = n; return nil }
}

// WithRecvBufferPool controls whether inbound frames larger than 4 KiB are
// received into buffers drawn from a shared, size-tiered pool instead of a
// fresh allocation per frame, which saves the allocation and garbage
// collection cost that dominates large-frame throughput.
//
// The default is ON for every socket type except STREAM (where it is
// silently off). Pass WithRecvBufferPool(false) to opt out and receive
// plain, garbage-collected memory. Explicit WithRecvBufferPool(true) on a
// STREAM socket is an error.
//
// With the pool, received messages carrying pooled frames are lent (see
// [Msg.Lent]):
//
//   - The receiver holds one reference and must call [Msg.Release] when
//     done with the message; [Msg.Retain] adds a reference, each needing
//     its own Release.
//   - The frames — including any slice taken from Frames, Bytes or
//     SingleFrame — are valid only until the last reference is released.
//     Their buffers then go back to the pool and are overwritten by later
//     receives, so copy anything that must outlive the message.
//   - Forwarding is safe: [Socket.SendMsg] holds its own reference until
//     the message is written, so RecvMsg, SendMsg, Release is correct.
//     Build a forwarded message with [Msg.Derive], never by wrapping the
//     frames in NewMsgFromBytes or NewMsgFromFrames.
//   - Forgetting Release is safe: there is no finalizer, so the buffer is
//     simply collected by the garbage collector instead of being reused.
//   - Releasing more times than retained panics.
//
// Code that calls Release and then keeps using the frames must copy them
// first or opt out with WithRecvBufferPool(false).
//
// Smaller frames, and frames on CURVE or custom-mechanism sessions (whose
// decoding produces a fresh plaintext anyway), are always ordinary memory,
// for which Release does nothing. The option affects receiving only, and
// is not applicable to STREAM sockets.
func WithRecvBufferPool(enabled bool) Option {
	return func(c *socketConfig) error { c.recvBufferPool = &enabled; return nil }
}

// WithRouterMandatory controls ROUTER.Send behaviour for unroutable
// destinations (leading identity frame not matching any connected peer).
// Default is false: unroutable messages are dropped silently, matching
// libzmq. When true, Send returns [ErrRouteUnknown] instead, which is the
// safer default for request/response services that need to observe
// delivery failures.
func WithRouterMandatory(enabled bool) Option {
	return func(c *socketConfig) error { c.routerMandatory = enabled; return nil }
}

// WithXPubVerbose controls subscription deduplication on XPUB. Default is
// false: XPUB surfaces a SUBSCRIBE or CANCEL to the application only when
// it is the first/last reference to that prefix across all connected SUB
// peers (the refcounted view). When true, every SUBSCRIBE/CANCEL from any
// peer is forwarded to the application as a discrete message — useful for
// applications that track subscriber churn or implement their own
// authorization on subscriptions.
func WithXPubVerbose(enabled bool) Option {
	return func(c *socketConfig) error { c.xpubVerbose = enabled; return nil }
}

// WithSubscribe adds a subscription prefix to a SUB (or XSUB) socket. May
// be passed multiple times to subscribe to disjoint prefixes. An empty
// prefix subscribes to every message. Subscriptions are refcounted:
// calling Subscribe or passing WithSubscribe for the same prefix twice
// requires two matching Unsubscribe calls before the prefix is actually
// removed from the filter and a CANCEL is sent upstream to connected
// publishers. This enables independent components on the same socket to
// manage their own subscriptions without interfering.
func WithSubscribe(prefix []byte) Option {
	return func(c *socketConfig) error {
		c.subscriptions = append(c.subscriptions, bytes.Clone(prefix))
		return nil
	}
}

// WithPlainAuth makes the socket the PLAIN server and installs its credential
// validator. The callback is invoked for each handshake with the credentials
// the client sent; returning true allows the session, false rejects it with
// [ErrAuthFailed]. When the Context has a [WithZAPHandler], that handler
// decides instead. As with libzmq's ZMQ_PLAIN_SERVER, the PLAIN role is
// independent of Bind/Connect: the socket is the server on every connection.
// It cannot be combined with [WithPlainClient] or the CURVE options.
//
// The callback runs on the connection's handshake goroutine with no timeout
// and no panic recovery: keep it quick, never call Close on the socket from
// it, and compare secrets in constant time.
//
// PLAIN credentials traverse the wire in cleartext — only use this mechanism
// on a trusted link (ipc, loopback) or wrapped in tls+tcp://. For untrusted
// networks prefer [WithCurveServer] / [WithCurveClient]. As in libzmq, inproc
// runs no mechanism: Bind or Connect on inproc:// fails with
// [ErrEndpointInvalid] for a socket with any PLAIN or CURVE option.
func WithPlainAuth(fn func(user, pass string) bool) Option {
	return func(c *socketConfig) error {
		if fn == nil {
			return fmt.Errorf("gozmq: WithPlainAuth requires a non-nil authenticate function")
		}
		c.setMech(&mechSpec{"WithPlainAuth", func(_ security.Role, meta map[string]string) (security.Mech, error) {
			return security.NewPlainServer(fn, meta), nil
		}})
		return nil
	}
}

// WithPlainClient makes the socket the PLAIN client, presenting (username,
// password) to the server during the ZMTP handshake. The PLAIN role is
// independent of Bind/Connect: the socket is the client on every connection,
// including those it accepts. It cannot be combined with [WithPlainAuth] or
// the CURVE options. Credentials are sent in cleartext; wrap in tls+tcp:// or
// switch to CURVE for any network you do not trust. Like every mechanism,
// it rules out inproc:// (see [WithPlainAuth]).
func WithPlainClient(username, password string) Option {
	return func(c *socketConfig) error {
		if len(username) > 255 || len(password) > 255 {
			return fmt.Errorf("gozmq: WithPlainClient username and password must each be at most 255 bytes")
		}
		c.setMech(&mechSpec{"WithPlainClient", func(_ security.Role, meta map[string]string) (security.Mech, error) {
			return security.NewPlainClient(username, password, meta), nil
		}})
		return nil
	}
}

// WithZAPDomain sets the RFC 27 ZAP domain presented to the Context's
// [WithZAPHandler] when it authorizes this socket's peers. PLAIN and CURVE
// servers consult the handler whatever the domain; NULL sockets consult it,
// for the connections they accept, only when a domain is set, as in libzmq,
// so a handler installed for PLAIN or CURVE does not start vetting every NULL
// socket. The default is the empty domain.
func WithZAPDomain(domain string) Option {
	return func(c *socketConfig) error {
		c.zapDomain = domain
		return nil
	}
}

// WithCurveServer configures the socket as the server side of a CURVE
// (CurveZMQ) handshake using secretKey as its long-term private key.
// secretKey must be 32 raw bytes (decode from Z85 via the
// security/curve package if you store keys in the standard textual
// format). CURVE provides forward-secret AEAD encryption; the client
// authenticates the server by its pinned public key, and the handshake
// proves that the client holds the secret half of the public key it
// presents. Which client keys are *allowed* is not decided here: without a
// [WithZAPHandler] on the Context every client that completes the
// handshake is accepted. Install a ZAP handler that checks the client's
// public key (the ZAP request's credential) to authorize clients.
// inproc runs no mechanism, so a CURVE socket cannot Bind or Connect on
// inproc:// (the call fails with [ErrEndpointInvalid]).
func WithCurveServer(secretKey []byte) Option {
	return func(c *socketConfig) error {
		if len(secretKey) != 32 {
			return fmt.Errorf("gozmq: WithCurveServer requires a 32-byte secret key, got %d bytes", len(secretKey))
		}
		key := curve.Key(secretKey)
		c.setMech(&mechSpec{"WithCurveServer", func(_ security.Role, meta map[string]string) (security.Mech, error) {
			return curve.NewCurveServer(key, meta)
		}})
		return nil
	}
}

// WithCurveClient configures the socket as the client side of a CURVE
// handshake. serverKey is the expected server long-term public key
// (clients authenticate the server by this pinned key);
// publicKey/secretKey is this client's long-term keypair, which the
// server uses to authenticate the client. All three arguments must be
// 32 raw bytes each; decode from Z85 first if stored textually. The
// resulting channel is forward-secret (ephemeral session keys) and
// mutually authenticated. As with [WithCurveServer], inproc:// is ruled out.
func WithCurveClient(serverKey, publicKey, secretKey []byte) Option {
	return func(c *socketConfig) error {
		if len(serverKey) != 32 || len(publicKey) != 32 || len(secretKey) != 32 {
			return fmt.Errorf("gozmq: WithCurveClient requires three 32-byte keys, got server=%d public=%d secret=%d bytes",
				len(serverKey), len(publicKey), len(secretKey))
		}
		server, public, secret := curve.Key(serverKey), curve.Key(publicKey), curve.Key(secretKey)
		c.setMech(&mechSpec{"WithCurveClient", func(_ security.Role, meta map[string]string) (security.Mech, error) {
			return curve.NewCurveClient(server, public, secret, meta)
		}})
		return nil
	}
}

// WithSecurityMech installs a caller-supplied ZMTP security mechanism,
// bypassing the built-in NULL / PLAIN / CURVE selection entirely. It is
// mutually exclusive with [WithPlainAuth], [WithPlainClient],
// [WithCurveServer], and [WithCurveClient].
//
// Reach for this only when the shipped mechanisms genuinely do not fit —
// a private protocol variant, or a fault-injecting test double. A
// [security.Mech] sits directly on the wire, so a mistake in it is a
// security bug rather than a correctness bug, and a mechanism gozmq does
// not ship will not interoperate with libzmq or any other ZMTP peer. If
// what you actually need is control over *which* peers are allowed rather
// than how the handshake works, use [WithZAPHandler] instead and keep the
// vetted cryptography.
//
// newMech is called once per session, with the session's role and with the
// local metadata gozmq advertises in READY (Socket-Type, and Identity when
// [WithIdentity] is set). The mechanism must include those properties in
// its own Metadata, and report the peer's through PeerMetadata once the
// handshake is done; the simplest way is to pass the map straight through to
// [security.NewMechBase], embed the result, and call its MergePeerMetadata.
// Returning an error fails the session — gozmq never falls back to a weaker
// mechanism.
//
// Each inbound handshake frame is capped at 64 KiB, whatever
// [WithMaxMessageSize] says, so an unauthenticated peer cannot make the
// socket allocate more. inproc runs no handshake, so a socket with a
// custom mechanism cannot Bind or Connect on inproc:// (the call fails
// with [ErrEndpointInvalid]).
func WithSecurityMech(newMech func(role security.Role, meta map[string]string) (security.Mech, error)) Option {
	return func(c *socketConfig) error {
		if newMech == nil {
			return fmt.Errorf("gozmq: WithSecurityMech requires a non-nil constructor")
		}
		c.setMech(&mechSpec{"WithSecurityMech", func(role security.Role, meta map[string]string) (security.Mech, error) {
			m, err := newMech(role, meta)
			if err == nil && m == nil {
				err = errors.New("constructor returned a nil mechanism")
			}
			return m, err
		}})
		return nil
	}
}

// WithHandshakeTimeout caps the total duration of the ZMTP handshake,
// covering the greeting exchange plus the security mechanism handshake
// (NULL, PLAIN, or CURVE) and the READY metadata exchange; on tls+tcp it
// also bounds the TLS handshake, which runs lazily inside it. Default is 30
// seconds; 0 disables the limit. On timeout the session is torn down with
// [ErrHandshakeTimeout] and, on a client socket, the reconnector retries
// with the configured backoff.
func WithHandshakeTimeout(d time.Duration) Option {
	return func(c *socketConfig) error {
		c.handshakeTimeout = d
		c.handshakeTimeoutSet = true
		return nil
	}
}

// WithMonitor registers a channel to receive lifecycle [Event]s whose
// Type bit is set in mask. Use [EventAll] to receive every event kind.
// The channel must be buffered: events are delivered non-blocking, and
// when the channel is full the event is dropped rather than blocking
// the socket's hot path. A buffer of 64-256 events is recommended;
// consumers that lag materially are responsible for their own drops.
// Never close the channel: events are emitted from session goroutines and
// one may still be in flight after [Socket.Monitor] (nil) or Close
// returns. Stop reading and let it be garbage collected instead.
func WithMonitor(ch chan<- Event, mask EventType) Option {
	return func(c *socketConfig) error {
		c.monitor = ch
		c.monitorMask = mask
		return nil
	}
}

// WithLogger sets the [*slog.Logger] used for internal diagnostics
// emitted by this socket and its child sessions — handshake failures,
// reconnect attempts, heartbeat loss, and teardown anomalies. Default is
// [slog.Default]. Passing nil resets to the process default.
func WithLogger(l *slog.Logger) Option {
	return func(c *socketConfig) error { c.logger = l; return nil }
}

func nonNeg[T int | int64 | time.Duration](name string, v T) error {
	if v < 0 {
		return fmt.Errorf("gozmq: %s requires n >= 0", name)
	}
	return nil
}

// MaxHWM is the largest value WithSendHWM and WithRecvHWM accept. Queues
// grow with their actual depth (8 bytes per queued message plus the messages
// themselves), so the cap is a sanity bound on a typo rather than an upfront
// reservation. Use 0 for an unbounded queue.
const MaxHWM = 1 << 20

func hwmInRange(name string, v int) error {
	if err := nonNeg(name, v); err != nil {
		return err
	}
	if v > MaxHWM {
		return fmt.Errorf("gozmq: %s(%d) exceeds MaxHWM (%d); use 0 for an unbounded queue", name, v, MaxHWM)
	}
	return nil
}

func validateSocketConfig(t SocketType, cfg *socketConfig) error {
	if err := cmp.Or(
		hwmInRange("WithSendHWM", cfg.sendHWM),
		hwmInRange("WithRecvHWM", cfg.recvHWM),
		nonNeg("WithMaxMessageSize", cfg.maxMessageSize),
		nonNeg("WithHandshakeTimeout", cfg.handshakeTimeout),
		nonNeg("WithReconnectInterval", cfg.reconnectInterval),
		nonNeg("WithReconnectIntervalMax", cfg.reconnectIntervalMax),
		nonNeg("WithHeartbeat interval", cfg.heartbeatInterval),
		nonNeg("WithHeartbeat timeout", cfg.heartbeatTimeout),
	); err != nil {
		return err
	}
	if len(cfg.identity) > 0 {
		if len(cfg.identity) > 255 {
			return fmt.Errorf("gozmq: WithIdentity requires 1..255 bytes")
		}
		if cfg.identity[0] == 0 {
			return fmt.Errorf("gozmq: WithIdentity requires a non-zero leading byte")
		}
	}
	// At most one mechanism per socket: otherwise newMechanism would silently
	// pick one. The options check their own arguments, so a mechanism that
	// is set can always be built and never falls back to NULL.
	if cfg.mechConflict != nil {
		return cfg.mechConflict
	}
	if cfg.routerMandatory && t != Router {
		return fmt.Errorf("gozmq: WithRouterMandatory only valid on ROUTER sockets")
	}
	if cfg.xpubVerbose && t != XPub {
		return fmt.Errorf("gozmq: WithXPubVerbose only valid on XPUB sockets")
	}
	if len(cfg.subscriptions) > 0 && t != Sub && t != XSub {
		return fmt.Errorf("gozmq: WithSubscribe only valid on SUB/XSUB sockets")
	}
	if t == Stream {
		// STREAM bypasses ZMTP entirely: no handshake, no security, no
		// heartbeats, no identity exchange. Silently ignoring these
		// options would be a security hazard (e.g. WithPlainAuth on a
		// raw-TCP listener that authenticates nobody).
		switch {
		case cfg.mech != nil:
			return fmt.Errorf("gozmq: security options (PLAIN, CURVE, WithSecurityMech) are not applicable to STREAM sockets (no ZMTP handshake)")
		case len(cfg.identity) > 0:
			return fmt.Errorf("gozmq: WithIdentity is not applicable to STREAM sockets (no ZMTP handshake)")
		case cfg.heartbeatSet:
			return fmt.Errorf("gozmq: WithHeartbeat is not applicable to STREAM sockets (no ZMTP heartbeats)")
		case cfg.handshakeTimeoutSet:
			return fmt.Errorf("gozmq: WithHandshakeTimeout is not applicable to STREAM sockets (no ZMTP handshake)")
		case cfg.zapDomain != "":
			return fmt.Errorf("gozmq: WithZAPDomain is not applicable to STREAM sockets (no ZMTP handshake)")
		case cfg.recvBufferPool != nil && *cfg.recvBufferPool:
			return fmt.Errorf("gozmq: WithRecvBufferPool is not applicable to STREAM sockets (no ZMTP framing)")
		}
	}
	return nil
}
