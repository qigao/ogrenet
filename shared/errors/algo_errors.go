package errors

import "github.com/pkg/errors"

var (
	ErrNoHost             = errors.New("no host")
	ErrAlgoNotImplemented = errors.New("algorithm not supported")
	ErrNoHostsAdded       = errors.New("no hosts added")
)
