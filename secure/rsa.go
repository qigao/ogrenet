package secure

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha512"
	"errors"
	"fmt"
)

const MinRSAKeyBits = 2048

var (
	ErrMissingPublicKey  = errors.New("secure: public key is required")
	ErrMissingPrivateKey = errors.New("secure: private key is required")
	ErrInvalidRSAKey     = errors.New("secure: invalid RSA key")
	ErrRSAKeyTooSmall    = errors.New("secure: RSA key size must be at least 2048 bits")
)

// RSAOAEP wraps session keys using RSA-OAEP with SHA-512, preserving the
// primitive used by the pre-v2 ogrenet implementation while giving it the
// correct key-wrapping role. Keys smaller than MinRSAKeyBits are rejected.
type RSAOAEP struct {
	public  *rsa.PublicKey
	private *rsa.PrivateKey
}

func NewRSAOAEP(public *rsa.PublicKey, private *rsa.PrivateKey) *RSAOAEP {
	return &RSAOAEP{public: public, private: private}
}

func GenerateRSAOAEP(bits int) (*RSAOAEP, error) {
	if bits < MinRSAKeyBits {
		return nil, ErrRSAKeyTooSmall
	}
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
	if err := validateRSAPublicKey(r.public); err != nil {
		return nil, err
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
	if err := validateRSAPrivateKey(r.private); err != nil {
		return nil, err
	}
	key, err := rsa.DecryptOAEP(sha512.New(), rand.Reader, r.private, wrapped, nil)
	if err != nil {
		return nil, fmt.Errorf("secure: rsa-oaep unwrap: %w", err)
	}
	return key, nil
}

func validateRSAPublicKey(key *rsa.PublicKey) error {
	if key == nil || key.N == nil || key.N.Sign() <= 0 || key.E < 3 || key.E%2 == 0 {
		return ErrInvalidRSAKey
	}
	if key.N.BitLen() < MinRSAKeyBits {
		return ErrRSAKeyTooSmall
	}
	return nil
}

func validateRSAPrivateKey(key *rsa.PrivateKey) error {
	if key == nil {
		return ErrInvalidRSAKey
	}
	if err := validateRSAPublicKey(&key.PublicKey); err != nil {
		return err
	}
	if key.D == nil || key.D.Sign() <= 0 || len(key.Primes) < 2 {
		return ErrInvalidRSAKey
	}
	return nil
}
