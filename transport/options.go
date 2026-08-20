package transport

import (
	"github.com/qigao/ogrenet/secure"
	"github.com/qigao/ogrenet/wire"
)

const (
	defaultWriteQueue      = 256
	defaultReadBuffer      = 64 << 10
	defaultMaxBufferedRead = 32 << 20
)

// FramerFactory creates per-connection framing state. A new framer is created
// for every accepted or dialed connection so custom stateful framers do not
// leak protocol state between peers.
type FramerFactory func() wire.Framer

type config struct {
	cipher          secure.Cipher
	framerFactory   FramerFactory
	writeQueue      int
	readBuffer      int
	maxBufferedRead int
}

func defaultConfig() config {
	return config{
		writeQueue:      defaultWriteQueue,
		readBuffer:      defaultReadBuffer,
		maxBufferedRead: defaultMaxBufferedRead,
	}
}

// Option configures Engine.
type Option func(*config) error

// WithCipher configures the cipher used by the default wire.Codec. It has no
// effect when WithFramerFactory supplies a custom framer.
func WithCipher(cipher secure.Cipher) Option {
	return func(c *config) error {
		c.cipher = cipher
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
	return wire.New(c.cipher), nil
}
