//go:build !linux

package transport

import (
	"errors"
	"testing"
)

func TestNewEpollUnsupportedPlatformDoesNotApplyOptions(t *testing.T) {
	applied := false
	opt := func(*config) error {
		applied = true
		return nil
	}
	_, err := NewEpoll(EpollConfig{}, opt)
	if !errors.Is(err, ErrBackendUnsupported) {
		t.Fatalf("err=%v", err)
	}
	if applied {
		t.Fatal("option applied on unsupported platform")
	}
}
