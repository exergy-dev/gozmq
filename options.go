package gozmq

import (
	"fmt"
	"log/slog"
	"time"
)

// Option configures a Socket at construction time. The option type is opaque;
// construct values via the With* helpers. Applying an option that is
// inapplicable to the socket type returns an error at Socket construction
// rather than silently ignoring it.
type Option interface {
	apply(*socketConfig) error
}

type socketOptionMeta interface {
	optionName() string
}

// socketConfig is the concrete aggregate that every Option mutates. Defaults
// are set in newSocketConfig.
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
	subscriptions        [][]byte
	handshakeTimeout     time.Duration
	zapDomain            string

	plainAuth       func(user, pass string) bool
	plainClientUser string
	plainClientPass string
	plainClient     bool

	curveServer    bool
	curveServerKey []byte
	curvePublicKey []byte
	curveSecretKey []byte

	monitor     chan<- Event
	monitorMask EventType

	// logger sinks internal diagnostic events (handshake failures,
	// reconnect attempts, heartbeat timeouts). nil lets the library
	// pick slog.Default; set via WithLogger for reproducible test
	// logs and structured context.
	logger *slog.Logger
}

func newSocketConfig() *socketConfig {
	return &socketConfig{
		sendHWM:              1000,
		recvHWM:              1000,
		linger:               time.Second,
		reconnectInterval:    100 * time.Millisecond,
		reconnectIntervalMax: 0,
		heartbeatInterval:    30 * time.Second,
		heartbeatTimeout:     90 * time.Second,
		maxMessageSize:       64 << 20,
		handshakeTimeout:     30 * time.Second,
	}
}

type optionFunc struct {
	name    string
	applyFn func(*socketConfig) error
}

func (f optionFunc) apply(c *socketConfig) error { return f.applyFn(c) }
func (f optionFunc) optionName() string          { return f.name }

func newOption(name string, fn func(*socketConfig) error) Option {
	return optionFunc{name: name, applyFn: fn}
}

// WithIdentity sets the ROUTER/DEALER identity advertised during the ZMTP
// handshake. The identity is the peer-visible name by which a ROUTER
// addresses this DEALER (or by which a ROUTER peer appears in the leading
// frame of a routed message). Identity bytes must be either empty (library
// assigns a random identity, default) or 1..255 bytes long with a non-zero
// leading byte; longer or zero-prefixed identities are rejected by ZMTP.
// The slice is copied defensively.
func WithIdentity(id []byte) Option {
	return newOption("WithIdentity", func(c *socketConfig) error {
		c.identity = append([]byte(nil), id...)
		return nil
	})
}

// WithSendHWM sets the outbound high-water mark — the maximum number of
// messages queued per peer pipe before backpressure engages. Default is
// 1000. Zero disables the limit (unbounded growth; not recommended for
// untrusted peers). When the HWM is reached, Send behavior depends on the
// socket pattern: blocking sockets (PAIR, PUSH, DEALER, REQ, REP, ROUTER
// without mandatory) wait for a Recv on the peer to drain queue space,
// while drop-on-overflow patterns (PUB, and ROUTER with mandatory plus an
// unknown route) may return [ErrHWM] or silently drop per pattern policy.
func WithSendHWM(n int) Option {
	return newOption("WithSendHWM", func(c *socketConfig) error { c.sendHWM = n; return nil })
}

// WithRecvHWM sets the inbound high-water mark — the maximum number of
// messages buffered per peer pipe before the reader goroutine stops
// accepting frames from the wire and exerts TCP backpressure on the peer.
// Default is 1000. Zero disables the limit.
func WithRecvHWM(n int) Option {
	return newOption("WithRecvHWM", func(c *socketConfig) error { c.recvHWM = n; return nil })
}

// WithLinger sets the grace period Socket.Close waits for outbound pipes
// to flush queued messages to the wire. Default is 1 second. Zero
// discards any queued messages immediately on Close. A negative duration
// blocks Close indefinitely until every peer has drained, matching
// libzmq's infinite-linger behavior — this can deadlock Close if a peer
// never drains or reconnects, so pair it with a parallel shutdown
// watchdog if you need it.
func WithLinger(d time.Duration) Option {
	return newOption("WithLinger", func(c *socketConfig) error { c.linger = d; return nil })
}

// WithReconnectInterval sets the base delay between reconnection attempts
// on a client-side socket after a dial failure or post-handshake
// disconnect. Default is 100ms. The actual delay grows exponentially and
// is jittered by 20% before each attempt, capped by
// [WithReconnectIntervalMax].
func WithReconnectInterval(d time.Duration) Option {
	return newOption("WithReconnectInterval", func(c *socketConfig) error { c.reconnectInterval = d; return nil })
}

// WithReconnectIntervalMax caps the reconnection backoff. Default is 0,
// meaning no cap (backoff grows without bound). Setting this to e.g.
// 30 * time.Second bounds the longest wait while preserving fast recovery
// on transient failures.
func WithReconnectIntervalMax(d time.Duration) Option {
	return newOption("WithReconnectIntervalMax", func(c *socketConfig) error { c.reconnectIntervalMax = d; return nil })
}

// WithHeartbeat configures the ZMTP 3.1 heartbeat. interval is how often
// PING commands are sent on an otherwise-idle session; timeout is the
// duration without any inbound frame (data or PONG) after which the peer
// is considered dead and the session is torn down. Defaults: 30s interval,
// 90s timeout. Heartbeats are disabled automatically when the peer
// advertises ZMTP 3.0 in the greeting. Heartbeating is the only reliable
// way to detect a half-open TCP connection (e.g. a peer killed without a
// FIN, or a NAT that dropped the flow) — disabling it is discouraged on
// long-lived connections.
func WithHeartbeat(interval, timeout time.Duration) Option {
	return newOption("WithHeartbeat", func(c *socketConfig) error {
		c.heartbeatInterval = interval
		c.heartbeatTimeout = timeout
		return nil
	})
}

// WithMaxMessageSize sets the per-message byte limit. Default is 64 MiB.
// A peer that sends a frame exceeding this limit has its session torn
// down with [ErrMalformedFrame]; the violation surfaces via a monitor
// EventDisconnected (with Reason carrying the error) rather than as a
// synchronous Send/Recv error, because the read loop detects it
// asynchronously of the application's call path. Pass 0 to disable the
// limit — opt-in to unlimited size, NOT recommended for untrusted peers
// because a hostile frame header can force an arbitrarily large
// allocation.
func WithMaxMessageSize(n int64) Option {
	return newOption("WithMaxMessageSize", func(c *socketConfig) error { c.maxMessageSize = n; return nil })
}

// WithRouterMandatory controls ROUTER.Send behaviour for unroutable
// destinations (leading identity frame not matching any connected peer).
// Default is false: unroutable messages are dropped silently, matching
// libzmq. When true, Send returns [ErrRouteUnknown] instead, which is the
// safer default for request/response services that need to observe
// delivery failures.
func WithRouterMandatory(enabled bool) Option {
	return newOption("WithRouterMandatory", func(c *socketConfig) error { c.routerMandatory = enabled; return nil })
}

// WithXPubVerbose controls subscription deduplication on XPUB. Default is
// false: XPUB surfaces a SUBSCRIBE or CANCEL to the application only when
// it is the first/last reference to that prefix across all connected SUB
// peers (the refcounted view). When true, every SUBSCRIBE/CANCEL from any
// peer is forwarded to the application as a discrete message — useful for
// applications that track subscriber churn or implement their own
// authorization on subscriptions.
func WithXPubVerbose(enabled bool) Option {
	return newOption("WithXPubVerbose", func(c *socketConfig) error { c.xpubVerbose = enabled; return nil })
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
	return newOption("WithSubscribe", func(c *socketConfig) error {
		c.subscriptions = append(c.subscriptions, append([]byte(nil), prefix...))
		return nil
	})
}

// WithPlainAuth installs the server-side PLAIN credential validator. The
// callback is invoked for each inbound handshake with the credentials the
// client sent; returning true allows the session, false rejects it with
// [ErrAuthFailed]. PLAIN credentials traverse the wire in cleartext —
// only use this mechanism on a trusted link (inproc, ipc, loopback) or
// wrapped in tls+tcp://. For untrusted networks prefer [WithCurveServer]
// / [WithCurveClient].
func WithPlainAuth(fn func(user, pass string) bool) Option {
	return newOption("WithPlainAuth", func(c *socketConfig) error { c.plainAuth = fn; return nil })
}

// WithPlainClient configures the socket to present PLAIN credentials
// (username, password) to the server during the ZMTP handshake. Credentials
// are sent in cleartext; wrap in tls+tcp:// or switch to CURVE for any
// network you do not trust.
func WithPlainClient(username, password string) Option {
	return newOption("WithPlainClient", func(c *socketConfig) error {
		c.plainClient = true
		c.plainClientUser = username
		c.plainClientPass = password
		return nil
	})
}

// WithZapDomain sets the RFC 27 ZAP domain presented to the context-level
// ZAP handler for server-side PLAIN or CURVE authorization. The default is
// the empty domain, matching libzmq.
func WithZapDomain(domain string) Option {
	return newOption("WithZapDomain", func(c *socketConfig) error {
		c.zapDomain = domain
		return nil
	})
}

// WithCurveServer configures the socket as the server side of a CURVE
// (CurveZMQ) handshake using secretKey as its long-term private key.
// secretKey must be 32 raw bytes (decode from Z85 via the
// security/curve package if you store keys in the standard textual
// format). CURVE provides forward-secret AEAD encryption and mutual
// authentication against each connecting client's long-term public key.
// Clients must present a matching public key via [WithCurveClient].
func WithCurveServer(secretKey []byte) Option {
	return newOption("WithCurveServer", func(c *socketConfig) error {
		c.curveServer = true
		c.curveSecretKey = append([]byte(nil), secretKey...)
		return nil
	})
}

// WithCurveClient configures the socket as the client side of a CURVE
// handshake. serverKey is the expected server long-term public key
// (clients authenticate the server by this pinned key);
// publicKey/secretKey is this client's long-term keypair, which the
// server uses to authenticate the client. All three arguments must be
// 32 raw bytes each; decode from Z85 first if stored textually. The
// resulting channel is forward-secret (ephemeral session keys) and
// mutually authenticated.
func WithCurveClient(serverKey, publicKey, secretKey []byte) Option {
	return newOption("WithCurveClient", func(c *socketConfig) error {
		c.curveServer = false
		c.curveServerKey = append([]byte(nil), serverKey...)
		c.curvePublicKey = append([]byte(nil), publicKey...)
		c.curveSecretKey = append([]byte(nil), secretKey...)
		return nil
	})
}

// WithHandshakeTimeout caps the total duration of the ZMTP handshake,
// covering the greeting exchange plus the security mechanism handshake
// (NULL, PLAIN, or CURVE) and the READY metadata exchange. Default is 30
// seconds. On timeout the session is torn down with [ErrHandshakeTimeout]
// and, on a client socket, the reconnector retries with the configured
// backoff.
func WithHandshakeTimeout(d time.Duration) Option {
	return newOption("WithHandshakeTimeout", func(c *socketConfig) error { c.handshakeTimeout = d; return nil })
}

// WithMonitor registers a channel to receive lifecycle [Event]s whose
// Type bit is set in mask. Use [EventAll] to receive every event kind.
// The channel must be buffered: events are delivered non-blocking, and
// when the channel is full the event is dropped rather than blocking
// the socket's hot path. A buffer of 64-256 events is recommended;
// consumers that lag materially are responsible for their own drops.
func WithMonitor(ch chan<- Event, mask EventType) Option {
	return newOption("WithMonitor", func(c *socketConfig) error {
		c.monitor = ch
		c.monitorMask = mask
		return nil
	})
}

// WithLogger sets the [*slog.Logger] used for internal diagnostics
// emitted by this socket and its child sessions — handshake failures,
// reconnect attempts, heartbeat loss, and teardown anomalies. Default is
// [slog.Default]. Passing nil resets to the process default. This option
// is optional; it exists primarily so tests can capture structured log
// output without mutating the process-wide default logger.
func WithLogger(l *slog.Logger) Option {
	return newOption("WithLogger", func(c *socketConfig) error { c.logger = l; return nil })
}

func validateSocketConfig(t SocketType, cfg *socketConfig) error {
	if cfg == nil {
		return nil
	}
	if cfg.sendHWM < 0 {
		return fmt.Errorf("gozmq: WithSendHWM requires n >= 0")
	}
	if cfg.recvHWM < 0 {
		return fmt.Errorf("gozmq: WithRecvHWM requires n >= 0")
	}
	if cfg.maxMessageSize < 0 {
		return fmt.Errorf("gozmq: WithMaxMessageSize requires n >= 0")
	}
	if cfg.handshakeTimeout < 0 {
		return fmt.Errorf("gozmq: WithHandshakeTimeout requires d >= 0")
	}
	if cfg.reconnectInterval < 0 {
		return fmt.Errorf("gozmq: WithReconnectInterval requires d >= 0")
	}
	if cfg.reconnectIntervalMax < 0 {
		return fmt.Errorf("gozmq: WithReconnectIntervalMax requires d >= 0")
	}
	if cfg.heartbeatInterval < 0 || cfg.heartbeatTimeout < 0 {
		return fmt.Errorf("gozmq: WithHeartbeat requires non-negative durations")
	}
	if len(cfg.identity) > 0 {
		if len(cfg.identity) > 255 {
			return fmt.Errorf("gozmq: WithIdentity requires 1..255 bytes")
		}
		if cfg.identity[0] == 0 {
			return fmt.Errorf("gozmq: WithIdentity requires a non-zero leading byte")
		}
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
	if len(cfg.curveSecretKey) > 0 && len(cfg.curveSecretKey) != 32 {
		return fmt.Errorf("gozmq: CURVE secret keys must be 32 bytes")
	}
	if len(cfg.curvePublicKey) > 0 && len(cfg.curvePublicKey) != 32 {
		return fmt.Errorf("gozmq: CURVE public keys must be 32 bytes")
	}
	if len(cfg.curveServerKey) > 0 && len(cfg.curveServerKey) != 32 {
		return fmt.Errorf("gozmq: CURVE server keys must be 32 bytes")
	}
	if cfg.curveServer {
		if len(cfg.curveSecretKey) != 32 {
			return fmt.Errorf("gozmq: WithCurveServer requires a 32-byte secret key")
		}
	} else if len(cfg.curveSecretKey) > 0 || len(cfg.curvePublicKey) > 0 || len(cfg.curveServerKey) > 0 {
		if len(cfg.curveSecretKey) != 32 || len(cfg.curvePublicKey) != 32 || len(cfg.curveServerKey) != 32 {
			return fmt.Errorf("gozmq: WithCurveClient requires 32-byte server, public, and secret keys")
		}
	}
	return nil
}
