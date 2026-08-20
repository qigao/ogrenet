package transport

import "errors"

var (
	ErrClosed                 = errors.New("transport: engine or connection is closed")
	ErrWouldBlock             = errors.New("transport: send queue is full")
	ErrFrameExceedsQueueBudget = errors.New("transport: encoded frame exceeds queued-byte budget")
	ErrNilContext             = errors.New("transport: nil context")
	ErrNilFramer              = errors.New("transport: framer factory returned nil")
	ErrInvalidFramer          = errors.New("transport: framer returned an invalid consumed length")
	ErrReadBufferFull         = errors.New("transport: buffered unread data exceeded the configured limit")
	ErrInvalidQueueSize       = errors.New("transport: write queue size must be positive")
	ErrInvalidBuffer          = errors.New("transport: buffer sizes must be positive")
	ErrInvalidQueuedBytes     = errors.New("transport: queued-byte limit must be positive")
	ErrUnsupportedNet         = errors.New("transport: unsupported non-stream network")
)
