package secure

import (
	"bytes"
	"testing"
)

func TestAESGCMRoundTripAndTamper(t *testing.T) {
	c, err := NewAESGCM([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	plain := []byte("hello authenticated world")
	sealed, err := c.Seal(nil, plain)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(sealed, plain) {
		t.Fatal("ciphertext equals plaintext")
	}
	opened, err := c.Open(nil, sealed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(opened, plain) {
		t.Fatalf("got %q, want %q", opened, plain)
	}
	sealed[len(sealed)-1] ^= 1
	if _, err := c.Open(nil, sealed); err == nil {
		t.Fatal("tampered ciphertext authenticated successfully")
	}
}

func TestRSAOAEPWrapRoundTrip(t *testing.T) {
	wrapper, err := GenerateRSAOAEP(2048)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("0123456789abcdef0123456789abcdef")
	wrapped, err := wrapper.Wrap(key)
	if err != nil {
		t.Fatal(err)
	}
	unwrapped, err := wrapper.Unwrap(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(unwrapped, key) {
		t.Fatalf("got %x, want %x", unwrapped, key)
	}
}
