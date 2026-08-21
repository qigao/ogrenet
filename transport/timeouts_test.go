package transport

import (
	"errors"
	"testing"
	"time"
)

func TestTimeoutDefaults(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	got := e.cfg.timeouts
	if got.Connect != 10*time.Second || got.Handshake != 10*time.Second || got.Write != 10*time.Second {
		t.Fatalf("bounded defaults = %+v", got)
	}
	if got.ReadIdle != 0 || got.ConnectionIdle != 0 || got.MaxLifetime != 0 {
		t.Fatalf("idle/lifetime defaults = %+v", got)
	}
}

func TestTimeoutValidationRejectsNegative(t *testing.T) {
	cases := []Timeouts{
		{Connect: -time.Second},
		{Handshake: -time.Second},
		{Write: -time.Second},
		{ReadIdle: -time.Second},
		{ConnectionIdle: -time.Second},
		{MaxLifetime: -time.Second},
	}
	for _, tt := range cases {
		if _, err := New(WithTimeouts(tt)); !errors.Is(err, ErrInvalidTimeout) {
			t.Fatalf("New(%+v) = %v, want ErrInvalidTimeout", tt, err)
		}
	}
}

func TestTimeoutZeroSemantics(t *testing.T) {
	e, err := New(WithTimeouts(Timeouts{}))
	if err != nil {
		t.Fatal(err)
	}
	got := e.cfg.timeouts
	if got.Connect != 10*time.Second || got.Handshake != 10*time.Second || got.Write != 10*time.Second {
		t.Fatalf("bounded zero semantics = %+v", got)
	}
	if got.ReadIdle != 0 || got.ConnectionIdle != 0 || got.MaxLifetime != 0 {
		t.Fatalf("idle zero semantics = %+v", got)
	}
}

func TestTimeoutOptionOrderIndependentTLSOverride(t *testing.T) {
	first, err := New(
		WithTLSHandshakeTimeout(5*time.Second),
		WithTimeouts(Timeouts{Handshake: 20 * time.Second}),
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(
		WithTimeouts(Timeouts{Handshake: 20 * time.Second}),
		WithTLSHandshakeTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := first.cfg.effectiveTLSHandshakeTimeout(); got != 5*time.Second {
		t.Fatalf("first TLS handshake timeout = %v, want 5s", got)
	}
	if got := second.cfg.effectiveTLSHandshakeTimeout(); got != 5*time.Second {
		t.Fatalf("second TLS handshake timeout = %v, want 5s", got)
	}
}

func TestTimeoutOptionOrderIndependentWebSocketOverride(t *testing.T) {
	ws := WebSocketConfig{
		HandshakeTimeout: 7 * time.Second,
		WriteTimeout:     8 * time.Second,
		PingInterval:     30 * time.Second,
		PongTimeout:      9 * time.Second,
	}
	base := Timeouts{Handshake: 20 * time.Second, Write: 21 * time.Second}
	first, err := New(WithWebSocketConfig(ws), WithTimeouts(base))
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(WithTimeouts(base), WithWebSocketConfig(ws))
	if err != nil {
		t.Fatal(err)
	}
	for name, e := range map[string]*Engine{"first": first, "second": second} {
		if got := e.cfg.effectiveWSHandshakeTimeout(); got != 7*time.Second {
			t.Fatalf("%s WS handshake timeout = %v, want 7s", name, got)
		}
		if got := e.cfg.effectiveWSWriteTimeout(); got != 8*time.Second {
			t.Fatalf("%s WS write timeout = %v, want 8s", name, got)
		}
	}
}

func TestTimeoutBasePolicyUsedWithoutProtocolOverride(t *testing.T) {
	e, err := New(WithTimeouts(Timeouts{Handshake: 3 * time.Second, Write: 4 * time.Second}))
	if err != nil {
		t.Fatal(err)
	}
	if got := e.cfg.effectiveTLSHandshakeTimeout(); got != 3*time.Second {
		t.Fatalf("TLS handshake timeout = %v, want 3s", got)
	}
	if got := e.cfg.effectiveWSHandshakeTimeout(); got != 3*time.Second {
		t.Fatalf("WS handshake timeout = %v, want 3s", got)
	}
	if got := e.cfg.effectiveWSWriteTimeout(); got != 4*time.Second {
		t.Fatalf("WS write timeout = %v, want 4s", got)
	}
}

func TestTimeoutErrorContract(t *testing.T) {
	cause := errors.New("root")
	err := &TimeoutError{Kind: TimeoutWrite, Cause: cause}
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("errors.Is(%v, ErrTimeout) = false", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("errors.Is(%v, cause) = false", err)
	}
	var te *TimeoutError
	if !errors.As(err, &te) {
		t.Fatalf("errors.As(%v, *TimeoutError) = false", err)
	}
	if te.Kind != TimeoutWrite || !te.Timeout() || te.Temporary() {
		t.Fatalf("timeout contract = %#v", te)
	}
}

func TestTimeoutErrorWithoutCauseStillMatchesSentinel(t *testing.T) {
	err := &TimeoutError{Kind: TimeoutConnectionIdle}
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("errors.Is(%v, ErrTimeout) = false", err)
	}
	if err.Unwrap() != nil {
		t.Fatalf("Unwrap() = %v, want nil", err.Unwrap())
	}
}

func TestWebSocketCloseTimeoutDefaultAndValidation(t *testing.T) {
	cfg := defaultConfig()
	if cfg.ws.CloseTimeout != defaultWSCloseTimeout {
		t.Fatalf("CloseTimeout = %v, want %v", cfg.ws.CloseTimeout, defaultWSCloseTimeout)
	}
	ws := cfg.ws
	ws.CloseTimeout = -time.Second
	if err := WithWebSocketConfig(ws)(&cfg); !errors.Is(err, ErrInvalidWebSocketConfig) {
		t.Fatalf("negative CloseTimeout error = %v", err)
	}
	ws.CloseTimeout = 0
	if err := WithWebSocketConfig(ws)(&cfg); err != nil {
		t.Fatalf("zero CloseTimeout error = %v", err)
	}
	if cfg.ws.CloseTimeout != defaultWSCloseTimeout {
		t.Fatalf("normalized CloseTimeout = %v", cfg.ws.CloseTimeout)
	}
}

func TestTimeoutCloseKind(t *testing.T) {
	err := &TimeoutError{Kind: TimeoutClose}
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("TimeoutClose does not match ErrTimeout: %v", err)
	}
	if got := err.Kind.String(); got != "close" {
		t.Fatalf("TimeoutClose string = %q", got)
	}
}
