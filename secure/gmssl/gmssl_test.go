//go:build gmssl && cgo

package gmssl

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/qigao/ogrenet/secure"
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

func TestSM4GCMAssociatedData(t *testing.T) {
	c, err := NewSM4GCM([]byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	var authenticated secure.AuthenticatedCipher = c
	sealed, err := authenticated.SealAAD(nil, []byte("payload"), []byte("header-a"))
	if err != nil {
		t.Fatal(err)
	}
	opened, err := authenticated.OpenAAD(nil, sealed, []byte("header-a"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(opened, []byte("payload")) {
		t.Fatalf("got %q", opened)
	}
	if _, err := authenticated.OpenAAD(nil, sealed, []byte("header-b")); err == nil {
		t.Fatal("ciphertext authenticated with different associated data")
	}
}

func TestSM4GCMEmptyRoundTrip(t *testing.T) {
	c, err := NewSM4GCM([]byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := c.Seal(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) != sm4NonceSize+sm4TagSize {
		t.Fatalf("got encrypted length %d, want %d", len(sealed), sm4NonceSize+sm4TagSize)
	}
	opened, err := c.Open(nil, sealed)
	if err != nil {
		t.Fatal(err)
	}
	if len(opened) != 0 {
		t.Fatalf("got %x, want empty plaintext", opened)
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
