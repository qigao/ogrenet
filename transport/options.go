package transport

import (
	"crypto/tls"
	"fmt"
	"math"
	"time"

	"github.com/qigao/ogrenet/secure"
	"github.com/qigao/ogrenet/wire"
)

const (
	defaultWriteQueue          = 256
	defaultMaxQueuedBytes      = 64 << 20
	defaultReadBuffer          = 64 << 10
	defaultMaxBufferedRead     = 32 << 20
	defaultMaxMessageBytes     = 16 << 20
	defaultMaxDatagramBytes    = 65507
	defaultTLSHandshakeTimeout = 10 * time.Second
	defaultWSHandshakeTimeout  = 10 * time.Second
	defaultWSWriteTimeout      = 10 * time.Second
	defaultWSPingInterval      = 30 * time.Second
	defaultWSPongTimeout       = 10 * time.Second
)

// FramerFactory creates per-session framing state for TCP and TLS. A new
// framer is created for every accepted or dialed stream session. WS/WSS use
// native WebSocket message boundaries and never invoke this factory.
type FramerFactory func() wire.Framer

// CipherFactory creates one message cipher instance per session. Use it for
// ciphers with mutable per-session state. The factory may be called
// concurrently.
type CipherFactory func() (secure.Cipher, error)

// TCPConfig configures portable TCP sockets before they enter TCP/TLS/WS/WSS
// protocol handling.
type TCPConfig struct {
	NoDelay          bool
	KeepAlive        bool
	KeepAlivePeriod  time.Duration
	ReadBufferBytes  int
	WriteBufferBytes int
}

// WebSocketConfig controls WS/WSS handshake and liveness behavior. Compression
// is intentionally disabled by the transport; enablement can be added later as
// an explicit protocol decision rather than an implicit negotiation fallback.
type WebSocketConfig struct {
	OriginPatterns   []string
	Subprotocols     []string
	HandshakeTimeout time.Duration
	WriteTimeout     time.Duration
	PingInterval     time.Duration
	PongTimeout      time.Duration
}

type config struct {
	cipher              secure.Cipher
	cipherFactory       CipherFactory
	framerFactory       FramerFactory
	clientTLS           *tls.Config
	serverTLS           *tls.Config
	tlsHandshakeTimeout time.Duration
	tcp                 TCPConfig
	ws                  WebSocketConfig
	writeQueue          int
	maxQueuedBytes      int
	readBuffer          int
	maxBufferedRead     int
	maxMessageBytes     int
	maxDatagramBytes    int
}

func defaultConfig() config {
	return config{
		tlsHandshakeTimeout: defaultTLSHandshakeTimeout,
		tcp: TCPConfig{
			NoDelay:         true,
			KeepAlive:       true,
			KeepAlivePeriod: 30 * time.Second,
		},
		ws: WebSocketConfig{
			HandshakeTimeout: defaultWSHandshakeTimeout,
			WriteTimeout:     defaultWSWriteTimeout,
			PingInterval:     defaultWSPingInterval,
			PongTimeout:      defaultWSPongTimeout,
		},
		writeQueue:      defaultWriteQueue,
		maxQueuedBytes:  defaultMaxQueuedBytes,
		readBuffer:      defaultReadBuffer,
		maxBufferedRead: defaultMaxBufferedRead,
		maxMessageBytes: defaultMaxMessageBytes,
		maxDatagramBytes: defaultMaxDatagramBytes,
	}
}

// Option configures Engine.
type Option func(*config) error

// WithCipher configures one message cipher shared by all sessions. The cipher
// must be safe for concurrent use. Use WithCipherFactory for mutable session
// state. This is application/message encryption and is independent of TLS.
func WithCipher(cipher secure.Cipher) Option {
	return func(c *config) error {
		c.cipher = cipher
		c.cipherFactory = nil
		return nil
	}
}

// WithCipherFactory creates one message cipher per session. The last
// WithCipher or WithCipherFactory option wins.
func WithCipherFactory(factory CipherFactory) Option {
	return func(c *config) error {
		if factory == nil {
			return ErrNilCipherFactory
		}
		c.cipher = nil
		c.cipherFactory = factory
		return nil
	}
}

// WithFramerFactory replaces the default wire.Codec for TCP and TLS only.
func WithFramerFactory(factory FramerFactory) Option {
	return func(c *config) error {
		if factory == nil {
			return ErrNilFramer
		}
		c.framerFactory = factory
		return nil
	}
}

// WithTLSClientConfig configures TLS/WSS clients. The value is cloned and the
// transport enforces TLS 1.3 as the minimum version.
func WithTLSClientConfig(cfg *tls.Config) Option {
	return func(c *config) error {
		if cfg == nil {
			return ErrTLSConfigRequired
		}
		c.clientTLS = cfg.Clone()
		return nil
	}
}

// WithTLSServerConfig configures TLS/WSS listeners. A certificate or
// GetCertificate callback is required and TLS 1.3 is the minimum version.
func WithTLSServerConfig(cfg *tls.Config) Option {
	return func(c *config) error {
		if cfg == nil {
			return ErrTLSConfigRequired
		}
		c.serverTLS = cfg.Clone()
		return nil
	}
}

func WithTLSHandshakeTimeout(timeout time.Duration) Option {
	return func(c *config) error {
		if timeout <= 0 {
			return ErrInvalidTimeout
		}
		c.tlsHandshakeTimeout = timeout
		return nil
	}
}

func WithTCPConfig(cfg TCPConfig) Option {
	return func(c *config) error {
		if cfg.KeepAlive && cfg.KeepAlivePeriod <= 0 {
			return ErrInvalidTimeout
		}
		if cfg.ReadBufferBytes < 0 || cfg.WriteBufferBytes < 0 {
			return ErrInvalidBuffer
		}
		c.tcp = cfg
		return nil
	}
}

func WithWebSocketConfig(cfg WebSocketConfig) Option {
	return func(c *config) error {
		if cfg.HandshakeTimeout <= 0 || cfg.WriteTimeout <= 0 || cfg.PongTimeout <= 0 || cfg.PingInterval < 0 {
			return ErrInvalidWebSocketConfig
		}
		cfg.OriginPatterns = append([]string(nil), cfg.OriginPatterns...)
		cfg.Subprotocols = append([]string(nil), cfg.Subprotocols...)
		c.ws = cfg
		return nil
	}
}

// WithWriteQueue sets the maximum number of accepted sends waiting behind the
// in-flight write. One additional internal slot covers the in-flight write.
func WithWriteQueue(size int) Option {
	return func(c *config) error {
		maxInt := int(^uint(0) >> 1)
		if size <= 0 || size == maxInt {
			return ErrInvalidQueueSize
		}
		c.writeQueue = size
		return nil
	}
}

// WithMaxQueuedBytes bounds encoded bytes retained by one session or packet
// socket, including its in-flight write.
func WithMaxQueuedBytes(size int) Option {
	return func(c *config) error {
		if size <= 0 {
			return ErrInvalidQueuedBytes
		}
		c.maxQueuedBytes = size
		return nil
	}
}

func WithReadBuffer(size int) Option {
	return func(c *config) error {
		if size <= 0 {
			return ErrInvalidBuffer
		}
		c.readBuffer = size
		return nil
	}
}

func WithMaxBufferedRead(size int) Option {
	return func(c *config) error {
		if size <= 0 {
			return ErrInvalidBuffer
		}
		c.maxBufferedRead = size
		return nil
	}
}

// WithMaxMessageBytes bounds plaintext application messages and stream wire
// payloads. WebSocket wire messages are allowed a small encryption/base64
// overhead but decrypted plaintext is always checked against this limit.
func WithMaxMessageBytes(size int) Option {
	return func(c *config) error {
		if size <= 0 || uint64(size) > math.MaxUint32 {
			return ErrInvalidMessageSize
		}
		c.maxMessageBytes = size
		return nil
	}
}

func WithMaxDatagramBytes(size int) Option {
	return func(c *config) error {
		if size <= 0 || size > 65507 {
			return ErrInvalidDatagramSize
		}
		c.maxDatagramBytes = size
		return nil
	}
}

func (c config) newCipher() (secure.Cipher, error) {
	cipher := c.cipher
	if c.cipherFactory == nil {
		return cipher, nil
	}
	var err error
	cipher, err = c.cipherFactory()
	if err != nil {
		return nil, fmt.Errorf("transport: create cipher: %w", err)
	}
	if cipher == nil {
		return nil, ErrNilCipher
	}
	return cipher, nil
}

func (c config) newFramer() (wire.Framer, error) {
	if c.framerFactory != nil {
		framer := c.framerFactory()
		if framer == nil {
			return nil, ErrNilFramer
		}
		return framer, nil
	}
	cipher, err := c.newCipher()
	if err != nil {
		return nil, err
	}
	codec := wire.New(cipher)
	codec.MaxPayload = uint32(c.maxMessageBytes)
	return codec, nil
}
