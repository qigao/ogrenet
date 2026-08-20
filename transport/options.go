package transport

import (
	"fmt"

	"github.com/qigao/ogrenet/secure"
	"github.com/qigao/ogrenet/wire"
)

const (
	defaultWriteQueue      = 256
	defaultMaxQueuedBytes  = 64 << 20
	defaultReadBuffer      = 64 << 10
	defaultMaxBufferedRead = 32 << 20
)

// FramerFactory creates per-connection framing state. A new framer is created
// for every accepted or dialed connection so custom stateful framers do not
// leak protocol state between peers. The factory may be called concurrently.
type FramerFactory func() wire.Framer

// CipherFactory creates one cipher instance for each accepted or dialed
// connection. Use this for ciphers that carry mutable per-session state. The
// factory may be called concurrently.
type CipherFactory func() (secure.Cipher, error)

type config struct {
	cipher          secure.Cipher
	cipherFactory   CipherFactory
	framerFactory   FramerFactory
	writeQueue      int
	maxQueuedBytes  int
	readBuffer      int
	maxBufferedRead int
}

func defaultConfig() config {
	return config{
		writeQueue:      defaultWriteQueue,
		maxQueuedBytes:  defaultMaxQueuedBytes,
		readBuffer:      defaultReadBuffer,
		maxBufferedRead: defaultMaxBufferedRead,
	}
}

// Option configures Engine.
type Option func(*config) error

// WithCipher configures one cipher instance shared by all default wire.Codecs.
// The cipher must therefore be safe for concurrent use across connections. Use
// WithCipherFactory for ciphers with mutable per-connection state. It has no
// effect when WithFramerFactory supplies a custom framer.
func WithCipher(cipher secure.Cipher) Option {
	return func(c *config) error {
		c.cipher = cipher
		c.cipherFactory = nil
		return nil
	}
}

// WithCipherFactory configures a factory that creates one cipher per
// connection for the default wire.Codec. The last WithCipher or
// WithCipherFactory option wins. It has no effect when WithFramerFactory
// supplies a custom framer.
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

// WithFramerFactory replaces the default wire.Codec with a per-connection
// framer factory.
func WithFramerFactory(factory FramerFactory) Option {
	return func(c *config) error {
		if factory == nil {
			return ErrNilFramer
		}
		c.framerFactory = factory
		return nil
	}
}

// WithWriteQueue sets the maximum number of encoded frames waiting for the
// connection writer. TrySend returns ErrWouldBlock when this queue is full.
func WithWriteQueue(size int) Option {
	return func(c *config) error {
		if size <= 0 {
			return ErrInvalidQueueSize
		}
		c.writeQueue = size
		return nil
	}
}

// WithMaxQueuedBytes bounds the total encoded frame bytes retained by one
// connection's writer queue and in-flight write. Send waits for this budget;
// TrySend returns ErrWouldBlock when insufficient byte budget is available.
func WithMaxQueuedBytes(size int) Option {
	return func(c *config) error {
		if size <= 0 {
			return ErrInvalidQueuedBytes
		}
		c.maxQueuedBytes = size
		return nil
	}
}

// WithReadBuffer sets the temporary socket read buffer size.
func WithReadBuffer(size int) Option {
	return func(c *config) error {
		if size <= 0 {
			return ErrInvalidBuffer
		}
		c.readBuffer = size
		return nil
	}
}

// WithMaxBufferedRead limits bytes retained while waiting for a complete frame.
// It protects custom framers from allowing unbounded stream accumulation.
func WithMaxBufferedRead(size int) Option {
	return func(c *config) error {
		if size <= 0 {
			return ErrInvalidBuffer
		}
		c.maxBufferedRead = size
		return nil
	}
}

func (c config) newFramer() (wire.Framer, error) {
	if c.framerFactory != nil {
		framer := c.framerFactory()
		if framer == nil {
			return nil, ErrNilFramer
		}
		return framer, nil
	}

	cipher := c.cipher
	if c.cipherFactory != nil {
		var err error
		cipher, err = c.cipherFactory()
		if err != nil {
			return nil, fmt.Errorf("transport: create cipher: %w", err)
		}
		if cipher == nil {
			return nil, ErrNilCipher
		}
	}
	return wire.New(cipher), nil
}
