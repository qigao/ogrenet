package transport

import (
	"context"
	"testing"

	"github.com/qigao/ogrenet"
)

func TestPublicGracefulSessionInterfaces(t *testing.T) {
	var stream ogrenet.Session = (*conn)(nil)
	var half ogrenet.HalfCloseSession = (*conn)(nil)
	var ws ogrenet.Session = (*wsSession)(nil)
	_ = stream
	_ = half
	_ = ws

	var _ interface {
		Shutdown(context.Context) error
	} = stream
}
