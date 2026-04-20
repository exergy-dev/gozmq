package gozmq

import (
	"io"
	"testing"
	"time"
)

func TestNewSocketRejectsInvalidOptionCombinations(t *testing.T) {
	ctx := NewContext()
	defer ctx.Close()

	cases := []struct {
		name string
		typ  SocketType
		opt  Option
	}{
		{name: "subscribe-on-pair", typ: Pair, opt: WithSubscribe([]byte("topic"))},
		{name: "router-mandatory-on-dealer", typ: Dealer, opt: WithRouterMandatory(true)},
		{name: "xpub-verbose-on-sub", typ: Sub, opt: WithXPubVerbose(true)},
		{name: "identity-leading-zero", typ: Dealer, opt: WithIdentity([]byte{0, 1})},
		{name: "curve-server-short-key", typ: Router, opt: WithCurveServer([]byte("short"))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ctx.NewSocket(tc.typ, tc.opt); err == nil {
				t.Fatalf("NewSocket(%v, %T) unexpectedly succeeded", tc.typ, tc.opt)
			}
		})
	}
}

func TestSetOptionRejectedOnLiveSocket(t *testing.T) {
	ctx := NewContext()
	defer ctx.Close()

	s, err := ctx.NewSocket(Pair)
	if err != nil {
		t.Fatalf("NewSocket: %v", err)
	}
	defer s.Close()

	if err := s.SetOption(WithLinger(2 * time.Second)); err == nil {
		t.Fatal("SetOption unexpectedly succeeded")
	}
}

func TestApplyConnSocketOptions(t *testing.T) {
	s := &Socket{cfg: newSocketConfig()}
	s.cfg.tcpKeepalive = false
	s.cfg.tcpNoDelay = true

	conn := &fakeTunedConn{}
	if err := s.applyConnSocketOptions(conn); err != nil {
		t.Fatalf("applyConnSocketOptions: %v", err)
	}
	if !conn.keepAliveCalled {
		t.Fatal("SetTCPKeepAlive was not called")
	}
	if conn.keepAliveEnabled {
		t.Fatal("SetTCPKeepAlive called with enabled=true, want false")
	}
	if conn.keepAlivePeriod != 30*time.Second {
		t.Fatalf("SetTCPKeepAlive period=%v, want 30s", conn.keepAlivePeriod)
	}
	if !conn.noDelayCalled {
		t.Fatal("SetTCPNoDelay was not called")
	}
	if !conn.noDelayEnabled {
		t.Fatal("SetTCPNoDelay called with enabled=false, want true")
	}
}

type fakeTunedConn struct {
	keepAliveCalled  bool
	keepAliveEnabled bool
	keepAlivePeriod  time.Duration
	noDelayCalled    bool
	noDelayEnabled   bool
}

func (c *fakeTunedConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *fakeTunedConn) Write([]byte) (int, error)        { return 0, nil }
func (c *fakeTunedConn) Close() error                     { return nil }
func (c *fakeTunedConn) LocalEndpoint() string            { return "" }
func (c *fakeTunedConn) RemoteEndpoint() string           { return "" }
func (c *fakeTunedConn) SetDeadline(time.Time) error      { return nil }
func (c *fakeTunedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakeTunedConn) SetWriteDeadline(time.Time) error { return nil }

func (c *fakeTunedConn) SetTCPKeepAlive(enabled bool, period time.Duration) error {
	c.keepAliveCalled = true
	c.keepAliveEnabled = enabled
	c.keepAlivePeriod = period
	return nil
}

func (c *fakeTunedConn) SetTCPNoDelay(enabled bool) error {
	c.noDelayCalled = true
	c.noDelayEnabled = enabled
	return nil
}
