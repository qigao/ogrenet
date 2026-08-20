package legacy

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"

	"github.com/qigao/ogrenet/secure"
)

type aesCFB struct {
	algorithm secure.Algorithm
	block     cipher.Block
	iv        []byte
}

func NewAES128CFB(key, iv []byte) (secure.Cipher, error) {
	return newAESCFB(secure.AlgLegacyAES128CFB, key, iv, 16)
}

func NewAES192CFB(key, iv []byte) (secure.Cipher, error) {
	return newAESCFB(secure.AlgLegacyAES192CFB, key, iv, 24)
}

func NewAES256CFB(key, iv []byte) (secure.Cipher, error) {
	return newAESCFB(secure.AlgLegacyAES256CFB, key, iv, 32)
}

func newAESCFB(algorithm secure.Algorithm, key, iv []byte, keySize int) (secure.Cipher, error) {
	block, err := aes.NewCipher(normalizeFixed(key, keySize))
	if err != nil {
		return nil, fmt.Errorf("secure/legacy: AES-CFB: %w", err)
	}
	return &aesCFB{
		algorithm: algorithm,
		block:     block,
		iv:        normalizeFixed(iv, aes.BlockSize),
	}, nil
}

func (c *aesCFB) Algorithm() secure.Algorithm { return c.algorithm }

func (c *aesCFB) Seal(dst, plaintext []byte) ([]byte, error) {
	start := len(dst)
	dst = append(dst, make([]byte, len(plaintext))...)
	cipher.NewCFBEncrypter(c.block, c.iv).XORKeyStream(dst[start:], plaintext)
	return dst, nil
}

func (c *aesCFB) Open(dst, ciphertext []byte) ([]byte, error) {
	start := len(dst)
	dst = append(dst, make([]byte, len(ciphertext))...)
	cipher.NewCFBDecrypter(c.block, c.iv).XORKeyStream(dst[start:], ciphertext)
	return dst, nil
}
