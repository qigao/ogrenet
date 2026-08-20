package legacy

import (
	"bytes"
	"testing"
)

func TestCipherKeyRoundTrip(t *testing.T) {
	key, err := NewCipherKey()
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != 16 {
		t.Fatalf("got key length %d, want 16", len(key))
	}

	plain := []byte("legacy cipher key payload")
	sealed, err := key.Encode(plain)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := key.Decode(sealed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(opened, plain) {
		t.Fatalf("got %x, want %x", opened, plain)
	}
}

func TestCipherKeyEmptyRoundTrip(t *testing.T) {
	key, err := NewCipherKey()
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := key.Encode(nil)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := key.Decode(sealed)
	if err != nil {
		t.Fatal(err)
	}
	if len(opened) != 0 {
		t.Fatalf("got %x, want empty plaintext", opened)
	}
}
