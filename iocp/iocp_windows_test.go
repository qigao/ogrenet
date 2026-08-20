//go:build windows

package iocp

import (
	"errors"
	"testing"
	"time"
)

func TestPostAndGet(t *testing.T) {
	p, err := Open(0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	want := Completion{Bytes: 42, Key: 7}
	if err := p.Post(want); err != nil {
		t.Fatal(err)
	}
	got, err := p.Get(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got.Bytes != want.Bytes || got.Key != want.Key || got.Overlapped != nil {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestGetTimeout(t *testing.T) {
	p, err := Open(0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	if _, err := p.Get(0); !errors.Is(err, ErrTimeout) {
		t.Fatalf("got error %v, want ErrTimeout", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	p, err := Open(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Get(0); !errors.Is(err, ErrClosed) {
		t.Fatalf("got error %v, want ErrClosed", err)
	}
}

func TestReservedKey(t *testing.T) {
	p, err := Open(0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	if err := p.Post(Completion{Key: reservedKey}); !errors.Is(err, ErrReservedKey) {
		t.Fatalf("got error %v, want ErrReservedKey", err)
	}
}
