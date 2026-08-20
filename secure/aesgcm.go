package secure

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

var ErrCiphertextTooShort = errors.New("secure: ciphertext is shorter than nonce")

type aesGCM struct {
	aead cipher.AEAD
}

// NewAESGCM creates an authenticated AES-GCM message cipher. key must be 16,
// 24, or 32 bytes. The wire value produced by Seal is nonce || ciphertext || tag.
func NewAESGCM(key []byte) (Cipher, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("secure: aes-gcm: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("secure: aes-gcm: %w", err)
	}
	return &aesGCM{aead: aead}, nil
}

func (*aesGCM) Algorithm() Algorithm { return AlgAESGCM }

func (c *aesGCM) Seal(dst, plaintext []byte) ([]byte, error) {
	start := len(dst)
	dst = append(dst, make([]byte, c.aead.NonceSize())...)
	nonce := dst[start:]
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return dst[:start], fmt.Errorf("secure: generate nonce: %w", err)
	}
	return c.aead.Seal(dst, nonce, plaintext, nil), nil
}

func (c *aesGCM) Open(dst, ciphertext []byte) ([]byte, error) {
	nonceSize := c.aead.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, ErrCiphertextTooShort
	}
	plaintext, err := c.aead.Open(dst, ciphertext[:nonceSize], ciphertext[nonceSize:], nil)
	if err != nil {
		return nil, fmt.Errorf("secure: authenticate ciphertext: %w", err)
	}
	return plaintext, nil
}
