package legacy

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5" // #nosec G501 -- retained only for pre-v2 wire/key compatibility.
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"github.com/qigao/ogrenet/secure"
)

var ErrLegacyCiphertextTooShort = errors.New("secure/legacy: ciphertext is shorter than GCM nonce")

// CipherKey preserves the pre-v2 ogrenet AES-GCM key API while also satisfying
// secure.Cipher so it can be used by the unified wire/transport stack. New code
// should use secure.NewAESGCM with an independently generated key instead.
type CipherKey []byte

// CryptCipherKey is the legacy name for an asymmetrically wrapped CipherKey.
type CryptCipherKey []byte

// NewCipherKey preserves the old derivation exactly: generate 10 random bytes,
// hash them with SHA-256, then use MD5(SHA-256(random)) as the 16-byte AES key.
// Deprecated: this exists only for compatibility with pre-v2 peers.
func NewCipherKey() (CipherKey, error) {
	seed := make([]byte, 10)
	if _, err := io.ReadFull(rand.Reader, seed); err != nil {
		return nil, fmt.Errorf("secure/legacy: generate cipher key seed: %w", err)
	}
	shaSum := sha256.Sum256(seed)
	md5Sum := md5.Sum(shaSum[:]) // #nosec G401 -- exact legacy derivation is required.
	key := make(CipherKey, len(md5Sum))
	copy(key, md5Sum[:])
	return key, nil
}

func (CipherKey) Algorithm() secure.Algorithm { return secure.AlgLegacyCipherKeyAESGCM }

// Seal adapts the legacy nonce-prefixed AES-GCM transform to secure.Cipher.
func (key CipherKey) Seal(dst, plaintext []byte) ([]byte, error) {
	encoded, err := key.Encode(plaintext)
	if err != nil {
		return nil, err
	}
	return append(dst, encoded...), nil
}

// Open adapts the legacy nonce-prefixed AES-GCM transform to secure.Cipher.
func (key CipherKey) Open(dst, ciphertext []byte) ([]byte, error) {
	decoded, err := key.Decode(ciphertext)
	if err != nil {
		return nil, err
	}
	return append(dst, decoded...), nil
}

// Encode preserves the old AES-GCM wire layout: nonce || ciphertext || tag.
func (key CipherKey) Encode(plaintext []byte) ([]byte, error) {
	aead, err := legacyGCM(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("secure/legacy: generate nonce: %w", err)
	}
	return aead.Seal(nonce, nonce, plaintext, nil), nil
}

// Decode opens a legacy nonce-prefixed AES-GCM ciphertext.
func (key CipherKey) Decode(ciphertext []byte) ([]byte, error) {
	aead, err := legacyGCM(key)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < aead.NonceSize() {
		return nil, ErrLegacyCiphertextTooShort
	}
	nonce := ciphertext[:aead.NonceSize()]
	plaintext, err := aead.Open(nil, nonce, ciphertext[aead.NonceSize():], nil)
	if err != nil {
		return nil, fmt.Errorf("secure/legacy: authenticate ciphertext: %w", err)
	}
	return plaintext, nil
}

func legacyGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("secure/legacy: AES-GCM: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("secure/legacy: AES-GCM: %w", err)
	}
	return aead, nil
}

var _ secure.Cipher = CipherKey(nil)
