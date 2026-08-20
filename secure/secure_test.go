package secure

import (
	"bytes"
	"crypto/rsa"
	"errors"
	"math/big"
	"testing"
)

func TestAlgorithmWireIDs(t *testing.T) {
	want := map[Algorithm]uint16{
		AlgNone:            0x0000,
		AlgAESGCM:          0x0001,
		AlgSM4GCM:          0x0002,
		AlgSM3Digest:       0x0003,
		AlgRSAOAEP:         0x0004,
		AlgSM2:             0x0005,
		AlgLegacyAES128CFB: 0x1001,
		AlgLegacyAES192CFB: 0x1002,
		AlgLegacyAES256CFB: 0x1003,
	}
	for algorithm, id := range want {
		if got := uint16(algorithm); got != id {
			t.Fatalf("algorithm %v wire ID = %#04x, want %#04x", algorithm, got, id)
		}
	}
}

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

func TestAESGCMAssociatedData(t *testing.T) {
	base, err := NewAESGCM([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	c, ok := base.(AuthenticatedCipher)
	if !ok {
		t.Fatal("AES-GCM does not implement AuthenticatedCipher")
	}
	plain := []byte("payload")
	sealed, err := c.SealAAD(nil, plain, []byte("header-a"))
	if err != nil {
		t.Fatal(err)
	}
	opened, err := c.OpenAAD(nil, sealed, []byte("header-a"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(opened, plain) {
		t.Fatalf("got %q, want %q", opened, plain)
	}
	if _, err := c.OpenAAD(nil, sealed, []byte("header-b")); err == nil {
		t.Fatal("ciphertext authenticated with different associated data")
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

func TestRSAOAEPRejectsWeakGeneratedKey(t *testing.T) {
	if _, err := GenerateRSAOAEP(1024); !errors.Is(err, ErrRSAKeyTooSmall) {
		t.Fatalf("GenerateRSAOAEP(1024) = %v, want ErrRSAKeyTooSmall", err)
	}
}

func TestRSAOAEPRejectsWeakImportedKeys(t *testing.T) {
	weakN := new(big.Int).Lsh(big.NewInt(1), 1023)
	weakPublic := &rsa.PublicKey{N: weakN, E: 65537}
	weakPrivate := &rsa.PrivateKey{
		PublicKey: *weakPublic,
		D:         big.NewInt(1),
		Primes:    []*big.Int{big.NewInt(3), big.NewInt(5)},
	}
	wrapper := NewRSAOAEP(weakPublic, weakPrivate)
	if _, err := wrapper.Wrap([]byte("key")); !errors.Is(err, ErrRSAKeyTooSmall) {
		t.Fatalf("weak Wrap = %v, want ErrRSAKeyTooSmall", err)
	}
	if _, err := wrapper.Unwrap([]byte("ciphertext")); !errors.Is(err, ErrRSAKeyTooSmall) {
		t.Fatalf("weak Unwrap = %v, want ErrRSAKeyTooSmall", err)
	}
}

func TestRSAOAEPRejectsInvalidImportedKeys(t *testing.T) {
	wrapper := NewRSAOAEP(&rsa.PublicKey{E: 65537}, &rsa.PrivateKey{})
	if _, err := wrapper.Wrap([]byte("key")); !errors.Is(err, ErrInvalidRSAKey) {
		t.Fatalf("invalid Wrap = %v, want ErrInvalidRSAKey", err)
	}
	if _, err := wrapper.Unwrap([]byte("ciphertext")); !errors.Is(err, ErrInvalidRSAKey) {
		t.Fatalf("invalid Unwrap = %v, want ErrInvalidRSAKey", err)
	}
}
