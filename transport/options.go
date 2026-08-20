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

type FramerFactory func() wire.Framer
type CipherFactory func() (secure.Cipher, error)

type TCPConfig struct {
	NoDelay          bool
	KeepAlive        bool
	KeepAlivePeriod  time.Duration
	ReadBufferBytes  int
	WriteBufferBytes int
}

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
		writeQueue:       defaultWriteQueue,
		maxQueuedBytes:   defaultMaxQueuedBytes,
		readBuffer:       defaultReadBuffer,
		maxBufferedRead:  defaultMaxBufferedRead,
		maxMessageBytes:  defaultMaxMessageBytes,
		maxDatagramBytes: defaultMaxDatagramBytes,
	}
}

type Option func(*config) error

func WithCipher(cipher secure.Cipher) Option {
	return func(c *config) error {
		if c.framerFactory != nil {
			return ErrConflictingCodecOptions
		}
		c.cipher = cipher
		c.cipherFactory = nil
		return nil
	}
}

func WithCipherFactory(factory CipherFactory) Option {
	return func(c *config) error {
		if factory == nil {
			return ErrNilCipherFactory
		}
		if c.framerFactory != nil {
			return ErrConflictingCodecOptions
		}
		c.cipher = nil
		c.cipherFactory = factory
		return nil
	}
}

// WithFramerFactory replaces the default wire.Codec for TCP and TLS only.
// Message ciphers are composed by the default codec, so custom framers and
// WithCipher/WithCipherFactory are intentionally mutually exclusive.
func WithFramerFactory(factory FramerFactory) Option {
	return func(c *config) error {
		if factory == nil {
			return ErrNilFramer
		}
		if c.cipher != nil || c.cipherFactory != nil {
			return ErrConflictingCodecOptions
		}
		c.framerFactory = factory
		return nil
	}
}

func WithTLSClientConfig(cfg *tls.Config) Option {
	return func(c *config) error {
		if cfg == nil {
			return ErrTLSConfigRequired
		}
		c.clientTLS = cfg.Clone()
		return nil
	}
}

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
