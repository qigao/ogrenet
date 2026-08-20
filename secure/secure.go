package secure

// Algorithm is a stable wire identifier for a security primitive. Values are
// assigned explicitly because changing an existing numeric ID is a wire-format
// compatibility break.
type Algorithm uint16

const (
	AlgNone      Algorithm = 0x0000
	AlgAESGCM    Algorithm = 0x0001
	AlgSM4GCM    Algorithm = 0x0002
	AlgSM3Digest Algorithm = 0x0003
	AlgRSAOAEP   Algorithm = 0x0004
	AlgSM2       Algorithm = 0x0005

	// 0x1000-0x10ff is reserved for pre-v2 non-GM compatibility transforms.
	AlgLegacyAES128CFB Algorithm = 0x1001
	AlgLegacyAES192CFB Algorithm = 0x1002
	AlgLegacyAES256CFB Algorithm = 0x1003
)

// Cipher is a reversible message transform. Seal and Open append their output
// to dst, matching the allocation-control convention used by cipher.AEAD.
type Cipher interface {
	Algorithm() Algorithm
	Seal(dst, plaintext []byte) ([]byte, error)
	Open(dst, ciphertext []byte) ([]byte, error)
}

// AuthenticatedCipher is implemented by ciphers that can authenticate
// associated metadata in addition to the encrypted payload. The default wire
// codec uses this to authenticate its semantic header fields.
type AuthenticatedCipher interface {
	Cipher
	SealAAD(dst, plaintext, aad []byte) ([]byte, error)
	OpenAAD(dst, ciphertext, aad []byte) ([]byte, error)
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
