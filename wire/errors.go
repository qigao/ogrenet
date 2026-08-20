package wire

import "errors"

var (
	ErrNeedMore           = errors.New("wire: need more bytes")
	ErrBadMagic           = errors.New("wire: invalid magic")
	ErrUnsupportedVersion = errors.New("wire: unsupported version")
	ErrUnsupportedFlags   = errors.New("wire: unsupported flags")
	ErrFrameTooLarge      = errors.New("wire: frame payload exceeds configured maximum")
	ErrMissingCipher      = errors.New("wire: encrypted frame requires a cipher")
	ErrAlgorithmMismatch  = errors.New("wire: encrypted frame algorithm does not match configured cipher")
	ErrTrailingData       = errors.New("wire: trailing bytes after frame")
)
