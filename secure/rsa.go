package secure

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha512"
	"errors"
	"fmt"
)

var (
	ErrMissingPublicKey  = errors.New("secure: public key is required")
	ErrMissingPrivateKey = errors.New("secure: private key is required")
)

// RSAOAEP wraps session keys using RSA-OAEP with SHA-512, preserving the
// primitive used by the pre-v2 ogrenet implementation while giving it the
// correct key-wrapping role.
type RSAOAEP struct {
	public  *rsa.PublicKey
	private *rsa.PrivateKey
}

func NewRSAOAEP(public *rsa.PublicKey, private *rsa.PrivateKey) *RSAOAEP {
	return &RSAOAEP{public: public, private: private}
}

func GenerateRSAOAEP(bits int) (*RSAOAEP, error) {
	private, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, fmt.Errorf("secure: generate rsa key: %w", err)
	}
	return NewRSAOAEP(&private.PublicKey, private), nil
}

func (*RSAOAEP) Algorithm() Algorithm { return AlgRSAOAEP }

func (r *RSAOAEP) Wrap(key []byte) ([]byte, error) {
	if r == nil || r.public == nil {
		return nil, ErrMissingPublicKey
	}
	wrapped, err := rsa.EncryptOAEP(sha512.New(), rand.Reader, r.public, key, nil)
	if err != nil {
		return nil, fmt.Errorf("secure: rsa-oaep wrap: %w", err)
	}
	return wrapped, nil
}

func (r *RSAOAEP) Unwrap(wrapped []byte) ([]byte, error) {
	if r == nil || r.private == nil {
		return nil, ErrMissingPrivateKey
	}
	key, err := rsa.DecryptOAEP(sha512.New(), rand.Reader, r.private, wrapped, nil)
	if err != nil {
		return nil, fmt.Errorf("secure: rsa-oaep unwrap: %w", err)
	}
	return key, nil
}
