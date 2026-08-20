package secure

// Algorithm is a stable wire identifier for a security primitive.
type Algorithm uint16

const (
	AlgNone Algorithm = iota
	AlgAESGCM
	AlgSM4GCM
	AlgSM3Digest
	AlgRSAOAEP
	AlgSM2

	AlgLegacyAES128CFB Algorithm = 0x1001
	AlgLegacyAES192CFB Algorithm = 0x1002
	AlgLegacyAES256CFB Algorithm = 0x1003
	AlgLegacySM2C1C2C3 Algorithm = 0x1004
	AlgLegacySM3Method Algorithm = 0x1005
	AlgLegacySM4CBC    Algorithm = 0x1006
)

// Cipher is a reversible message transform. Seal and Open append their output
// to dst, matching the allocation-control convention used by cipher.AEAD.
type Cipher interface {
	Algorithm() Algorithm
	Seal(dst, plaintext []byte) ([]byte, error)
	Open(dst, ciphertext []byte) ([]byte, error)
}

// Digest is a one-way integrity primitive and is intentionally separate from
// Cipher so hashes such as SM3 cannot be mistaken for reversible encryption.
type Digest interface {
	Algorithm() Algorithm
	Sum(dst, data []byte) []byte
	Verify(data, digest []byte) bool
}

// KeyWrapper is an asymmetric primitive intended for wrapping session keys,
// not bulk application payloads.
type KeyWrapper interface {
	Algorithm() Algorithm
	Wrap(key []byte) ([]byte, error)
	Unwrap(wrapped []byte) ([]byte, error)
}

// Profile collects optional message-security primitives used by higher layers.
type Profile struct {
	Cipher     Cipher
	Digest     Digest
	KeyWrapper KeyWrapper
}
