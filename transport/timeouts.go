package transport

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	defaultConnectTimeout   = 10 * time.Second
	defaultHandshakeTimeout = 10 * time.Second
	defaultWriteTimeout     = 10 * time.Second
)

// Timeouts configures Engine-wide timeout policy. Zero-valued Connect,
// Handshake, and Write fields use bounded production defaults. Zero-valued
// ReadIdle, ConnectionIdle, and MaxLifetime fields disable those policies.
type Timeouts struct {
	Connect        time.Duration
	Handshake      time.Duration
	Write          time.Duration
	ReadIdle       time.Duration
	ConnectionIdle time.Duration
	MaxLifetime    time.Duration
}

func normalizeTimeouts(t Timeouts) (Timeouts, error) {
	for _, d := range []time.Duration{t.Connect, t.Handshake, t.Write, t.ReadIdle, t.ConnectionIdle, t.MaxLifetime} {
		if d < 0 {
			return Timeouts{}, ErrInvalidTimeout
		}
	}
	if t.Connect == 0 {
		t.Connect = defaultConnectTimeout
	}
	if t.Handshake == 0 {
		t.Handshake = defaultHandshakeTimeout
	}
	if t.Write == 0 {
		t.Write = defaultWriteTimeout
	}
	return t, nil
}

// WithTimeouts configures the Engine-wide timeout policy.
func WithTimeouts(timeouts Timeouts) Option {
	return func(c *config) error {
		normalized, err := normalizeTimeouts(timeouts)
		if err != nil {
			return err
		}
		c.timeouts = normalized
		return nil
	}
}

var ErrTimeout = errors.New("transport: operation timed out")

// TimeoutKind identifies the runtime timeout domain.
type TimeoutKind uint8

const (
	TimeoutConnect TimeoutKind = iota + 1
	TimeoutHandshake
	TimeoutWrite
	TimeoutReadIdle
	TimeoutConnectionIdle
	TimeoutMaxLifetime
	TimeoutClose
)

func (k TimeoutKind) String() string {
	switch k {
	case TimeoutConnect:
		return "connect"
	case TimeoutHandshake:
		return "handshake"
	case TimeoutWrite:
		return "write"
	case TimeoutReadIdle:
		return "read-idle"
	case TimeoutConnectionIdle:
		return "connection-idle"
	case TimeoutMaxLifetime:
		return "max-lifetime"
	case TimeoutClose:
		return "close"
	default:
		return "unknown"
	}
}

// TimeoutError reports an Engine-enforced timeout while preserving any
// underlying OS, TLS, WebSocket, or context error as Cause.
type TimeoutError struct {
	Kind  TimeoutKind
	Cause error
}

func (e *TimeoutError) Error() string {
	if e == nil {
		return ErrTimeout.Error()
	}
	if e.Cause == nil {
		return fmt.Sprintf("transport: %s timeout", e.Kind)
	}
	return fmt.Sprintf("transport: %s timeout: %v", e.Kind, e.Cause)
}

func (e *TimeoutError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *TimeoutError) Is(target error) bool { return target == ErrTimeout }
func (e *TimeoutError) Timeout() bool        { return true }
func (e *TimeoutError) Temporary() bool      { return false }

func boundedOperationContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}

func mapOperationTimeout(parent, operation context.Context, kind TimeoutKind, err error) error {
	if err == nil {
		return nil
	}
	if cause := context.Cause(parent); cause != nil {
		return cause
	}
	if errors.Is(context.Cause(operation), context.DeadlineExceeded) {
		return &TimeoutError{Kind: kind, Cause: err}
	}
	return err
}

func (c config) effectiveTLSHandshakeTimeout() time.Duration {
	if c.timeoutOverrides.tlsHandshake {
		return c.tlsHandshakeTimeout
	}
	return c.timeouts.Handshake
}

func (c config) effectiveWSHandshakeTimeout() time.Duration {
	if c.timeoutOverrides.wsHandshake {
		return c.ws.HandshakeTimeout
	}
	return c.timeouts.Handshake
}

func (c config) effectiveWSWriteTimeout() time.Duration {
	if c.timeoutOverrides.wsWrite {
		return c.ws.WriteTimeout
	}
	return c.timeouts.Write
}

type timeoutOverrides struct {
	tlsHandshake bool
	wsHandshake  bool
	wsWrite      bool
}
