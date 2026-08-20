//go:build gmssl && cgo

package gmssl

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestSM3KnownVector(t *testing.T) {
	got := NewSM3().Sum(nil, []byte("abc"))
	want, err := hex.DecodeString("66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %x, want %x", got, want)
	}
}

func TestSM4GCMRoundTripAndTamper(t *testing.T) {
	c, err := NewSM4GCM([]byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	plain := []byte("gmssl authenticated payload")
	sealed, err := c.Seal(nil, plain)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := c.Open(nil, sealed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(opened, plain) {
		t.Fatalf("got %x, want %x", opened, plain)
	}
	sealed[len(sealed)-1] ^= 1
	if _, err := c.Open(nil, sealed); err == nil {
		t.Fatal("tampered SM4-GCM ciphertext authenticated successfully")
	}
}

func TestSM2KeyWrapperRoundTrip(t *testing.T) {
	publicKey, privateKey, err := GenerateSM2Key()
	if err != nil {
		t.Fatal(err)
	}
	wrapper, err := NewSM2KeyWrapper(publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("0123456789abcdef")
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

func TestLegacySM2C1C2C3RoundTrip(t *testing.T) {
	publicKey, privateKey, err := GenerateSM2Key()
	if err != nil {
		t.Fatal(err)
	}
	cipher, err := NewLegacySM2C1C2C3(privateKey, publicKey)
	if err != nil {
		t.Fatal(err)
	}
	plain := []byte("legacy sm2")
	sealed, err := cipher.Seal(nil, plain)
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) == 0 || sealed[0] != 0x04 {
		t.Fatalf("legacy ciphertext does not start with uncompressed C1: %x", sealed)
	}
	opened, err := cipher.Open(nil, sealed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(opened, plain) {
		t.Fatalf("got %x, want %x", opened, plain)
	}
}

func TestLegacySM4CBCRoundTrip(t *testing.T) {
	cipher := NewLegacySM4CBC([]byte("short-key"), []byte("short-iv"))
	plain := []byte("legacy sm4 cbc")
	sealed, err := cipher.Seal(nil, plain)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := cipher.Open(nil, sealed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(opened, plain) {
		t.Fatalf("got %x, want %x", opened, plain)
	}
}

func TestLegacySM3Method(t *testing.T) {
	legacy := LegacySM3Method{}
	sealed, err := legacy.Seal(nil, []byte("abc"))
	if err != nil {
		t.Fatal(err)
	}
	opened, err := legacy.Open(nil, sealed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(opened, sealed) {
		t.Fatal("legacy SM3 Open must return the digest unchanged")
	}
}
