//go:build gmssl && cgo

package gmssl

/*
#cgo LDFLAGS: -lgmssl
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <gmssl/rand.h>
#include <gmssl/sm2.h>
#include <gmssl/sm3.h>
#include <gmssl/sm4.h>

static int ogre_rand_bytes(uint8_t *out, size_t outlen) {
	if (outlen == 0) return 1;
	return rand_bytes(out, outlen);
}

static int ogre_sm3_sum(const uint8_t *in, size_t inlen, uint8_t out[32]) {
	SM3_CTX ctx;
	sm3_init(&ctx);
	if (inlen > 0) sm3_update(&ctx, in, inlen);
	sm3_finish(&ctx, out);
	return 1;
}

static int ogre_sm4_gcm_encrypt(const uint8_t key_bytes[16], const uint8_t *iv, size_t ivlen,
	const uint8_t *in, size_t inlen, uint8_t *out, uint8_t tag[16]) {
	SM4_KEY key;
	sm4_set_encrypt_key(&key, key_bytes);
	return sm4_gcm_encrypt(&key, iv, ivlen, NULL, 0, in, inlen, out, 16, tag);
}

static int ogre_sm4_gcm_decrypt(const uint8_t key_bytes[16], const uint8_t *iv, size_t ivlen,
	const uint8_t *in, size_t inlen, const uint8_t tag[16], uint8_t *out) {
	SM4_KEY key;
	sm4_set_encrypt_key(&key, key_bytes);
	return sm4_gcm_decrypt(&key, iv, ivlen, NULL, 0, in, inlen, tag, 16, out);
}

static int ogre_sm4_cbc_encrypt(const uint8_t key[16], const uint8_t iv[16],
	const uint8_t *in, size_t inlen, uint8_t *out, size_t *outlen) {
	SM4_CBC_CTX ctx;
	size_t n1 = 0, n2 = 0;
	if (sm4_cbc_encrypt_init(&ctx, key, iv) != 1) return -1;
	if (inlen > 0 && sm4_cbc_encrypt_update(&ctx, in, inlen, out, &n1) != 1) return -1;
	if (sm4_cbc_encrypt_finish(&ctx, out + n1, &n2) != 1) return -1;
	*outlen = n1 + n2;
	return 1;
}

static int ogre_sm4_cbc_decrypt(const uint8_t key[16], const uint8_t iv[16],
	const uint8_t *in, size_t inlen, uint8_t *out, size_t *outlen) {
	SM4_CBC_CTX ctx;
	size_t n1 = 0, n2 = 0;
	if (sm4_cbc_decrypt_init(&ctx, key, iv) != 1) return -1;
	if (inlen > 0 && sm4_cbc_decrypt_update(&ctx, in, inlen, out, &n1) != 1) return -1;
	if (sm4_cbc_decrypt_finish(&ctx, out + n1, &n2) != 1) return -1;
	*outlen = n1 + n2;
	return 1;
}

static int ogre_sm2_key_from_private(SM2_KEY *key, const uint8_t private_key[32]) {
	sm2_z256_t d;
	sm2_z256_from_bytes(d, private_key);
	return sm2_key_set_private_key(key, d);
}

static int ogre_sm2_key_from_public(SM2_KEY *key, const uint8_t *public_key, size_t public_key_len) {
	SM2_Z256_POINT point;
	int ret;
	if (public_key_len == 64) {
		ret = sm2_z256_point_from_bytes(&point, public_key);
	} else {
		ret = sm2_z256_point_from_octets(&point, public_key, public_key_len);
	}
	if (ret != 1) return ret;
	return sm2_key_set_public_key(key, &point);
}

static int ogre_sm2_generate(uint8_t private_key[32], uint8_t public_key[64]) {
	SM2_KEY key;
	if (sm2_key_generate(&key) != 1) return -1;
	sm2_z256_to_bytes(key.private_key, private_key);
	return sm2_z256_point_to_bytes(&key.public_key, public_key);
}

static int ogre_sm2_encrypt_der(const SM2_KEY *key, const uint8_t *in, size_t inlen,
	uint8_t *out, size_t *outlen) {
	return sm2_encrypt(key, in, inlen, out, outlen);
}

static int ogre_sm2_decrypt_der(const SM2_KEY *key, const uint8_t *in, size_t inlen,
	uint8_t *out, size_t *outlen) {
	return sm2_decrypt(key, in, inlen, out, outlen);
}

static int ogre_sm2_encrypt_c1c2c3(const SM2_KEY *key, const uint8_t *in, size_t inlen,
	uint8_t *out, size_t *outlen) {
	SM2_CIPHERTEXT c;
	if (inlen == 0 || inlen > SM2_MAX_PLAINTEXT_SIZE) return -1;
	if (sm2_do_encrypt(key, in, inlen, &c) != 1) return -1;
	out[0] = 0x04;
	memcpy(out + 1, c.point.x, 32);
	memcpy(out + 33, c.point.y, 32);
	memcpy(out + 65, c.ciphertext, c.ciphertext_size);
	memcpy(out + 65 + c.ciphertext_size, c.hash, 32);
	*outlen = 97 + c.ciphertext_size;
	return 1;
}

static int ogre_sm2_decrypt_c1c2c3(const SM2_KEY *key, const uint8_t *in, size_t inlen,
	uint8_t *out, size_t *outlen) {
	SM2_CIPHERTEXT c;
	size_t clen;
	if (inlen < 98 || in[0] != 0x04) return -1;
	clen = inlen - 97;
	if (clen == 0 || clen > SM2_MAX_PLAINTEXT_SIZE) return -1;
	memset(&c, 0, sizeof(c));
	memcpy(c.point.x, in + 1, 32);
	memcpy(c.point.y, in + 33, 32);
	c.ciphertext_size = (uint8_t)clen;
	memcpy(c.ciphertext, in + 65, clen);
	memcpy(c.hash, in + 65 + clen, 32);
	return sm2_do_decrypt(key, &c, out, outlen);
}
*/
import "C"

import (
	"crypto/subtle"
	"errors"
	"unsafe"

	"github.com/qigao/ogrenet/secure"
)

const (
	sm4KeySize   = 16
	sm4NonceSize = 12
	sm4TagSize   = 16
	sm2MaxPlain  = 255
	sm2MaxCipher = 366
)

var (
	ErrGmSSL              = errors.New("secure/gmssl: GmSSL operation failed")
	ErrInvalidKeySize     = errors.New("secure/gmssl: invalid key size")
	ErrInvalidSM2Key      = errors.New("secure/gmssl: invalid SM2 key")
	ErrMessageTooLarge    = errors.New("secure/gmssl: SM2 plaintext exceeds 255 bytes")
	ErrCiphertextTooShort = errors.New("secure/gmssl: ciphertext too short")
)

func bytePtr(b []byte) *C.uint8_t {
	if len(b) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0]))
}

// SM3 is a GmSSL-backed SM3 digest.
type SM3 struct{}

func NewSM3() secure.Digest { return SM3{} }
func (SM3) Algorithm() secure.Algorithm { return secure.AlgSM3Digest }
func (SM3) Sum(dst, data []byte) []byte {
	var out [32]byte
	C.ogre_sm3_sum(bytePtr(data), C.size_t(len(data)), (*C.uint8_t)(unsafe.Pointer(&out[0])))
	return append(dst, out[:]...)
}
func (SM3) Verify(data, digest []byte) bool {
	if len(digest) != 32 {
		return false
	}
	sum := SM3{}.Sum(nil, data)
	return subtle.ConstantTimeCompare(sum, digest) == 1
}

// SM4GCM is a GmSSL-backed authenticated SM4-GCM cipher. Seal emits
// nonce || ciphertext || tag.
type SM4GCM struct{ key [sm4KeySize]byte }

func NewSM4GCM(key []byte) (*SM4GCM, error) {
	if len(key) != sm4KeySize {
		return nil, ErrInvalidKeySize
	}
	c := &SM4GCM{}
	copy(c.key[:], key)
	return c, nil
}
func (*SM4GCM) Algorithm() secure.Algorithm { return secure.AlgSM4GCM }
func (c *SM4GCM) Seal(dst, plaintext []byte) ([]byte, error) {
	var nonce [sm4NonceSize]byte
	if C.ogre_rand_bytes((*C.uint8_t)(unsafe.Pointer(&nonce[0])), C.size_t(len(nonce))) != 1 {
		return nil, ErrGmSSL
	}
	body := make([]byte, len(plaintext)+sm4TagSize)
	// GmSSL 3.2.0 requires out != NULL even for a zero-length plaintext.
	bodyPtr := bytePtr(body)
	tagPtr := (*C.uint8_t)(unsafe.Pointer(&body[len(plaintext)]))
	if C.ogre_sm4_gcm_encrypt((*C.uint8_t)(unsafe.Pointer(&c.key[0])), (*C.uint8_t)(unsafe.Pointer(&nonce[0])), C.size_t(len(nonce)),
		bytePtr(plaintext), C.size_t(len(plaintext)), bodyPtr, tagPtr) != 1 {
		return nil, ErrGmSSL
	}
	dst = append(dst, nonce[:]...)
	return append(dst, body...), nil
}
func (c *SM4GCM) Open(dst, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < sm4NonceSize+sm4TagSize {
		return nil, ErrCiphertextTooShort
	}
	nonce := ciphertext[:sm4NonceSize]
	body := ciphertext[sm4NonceSize : len(ciphertext)-sm4TagSize]
	tag := ciphertext[len(ciphertext)-sm4TagSize:]
	// GmSSL 3.2.0 also requires out != NULL for zero-length ciphertext.
	plainBuf := make([]byte, len(body)+1)
	plain := plainBuf[:len(body)]
	if C.ogre_sm4_gcm_decrypt((*C.uint8_t)(unsafe.Pointer(&c.key[0])), bytePtr(nonce), C.size_t(len(nonce)),
		bytePtr(body), C.size_t(len(body)), bytePtr(tag), bytePtr(plainBuf)) != 1 {
		return nil, ErrGmSSL
	}
	return append(dst, plain...), nil
}

// SM2KeyWrapper wraps short session keys using GmSSL's DER-encoded SM2 ciphertext.
type SM2KeyWrapper struct {
	public     C.SM2_KEY
	private    C.SM2_KEY
	hasPublic  bool
	hasPrivate bool
}

func NewSM2KeyWrapper(publicKey, privateKey []byte) (*SM2KeyWrapper, error) {
	w := &SM2KeyWrapper{}
	if len(privateKey) > 0 {
		if len(privateKey) != 32 || C.ogre_sm2_key_from_private(&w.private, bytePtr(privateKey)) != 1 {
			return nil, ErrInvalidSM2Key
		}
		w.hasPrivate = true
		w.public = w.private
		w.hasPublic = true
	}
	if len(publicKey) > 0 {
		if (len(publicKey) != 64 && len(publicKey) != 65) || C.ogre_sm2_key_from_public(&w.public, bytePtr(publicKey), C.size_t(len(publicKey))) != 1 {
			return nil, ErrInvalidSM2Key
		}
		w.hasPublic = true
	}
	if !w.hasPublic && !w.hasPrivate {
		return nil, ErrInvalidSM2Key
	}
	return w, nil
}

func GenerateSM2Key() (publicKey, privateKey []byte, err error) {
	publicKey = make([]byte, 64)
	privateKey = make([]byte, 32)
	if C.ogre_sm2_generate(bytePtr(privateKey), bytePtr(publicKey)) != 1 {
		return nil, nil, ErrGmSSL
	}
	return publicKey, privateKey, nil
}

func (*SM2KeyWrapper) Algorithm() secure.Algorithm { return secure.AlgSM2 }
func (w *SM2KeyWrapper) Wrap(key []byte) ([]byte, error) {
	if !w.hasPublic {
		return nil, secure.ErrMissingPublicKey
	}
	if len(key) == 0 || len(key) > sm2MaxPlain {
		return nil, ErrMessageTooLarge
	}
	out := make([]byte, sm2MaxCipher)
	var outlen C.size_t
	if C.ogre_sm2_encrypt_der(&w.public, bytePtr(key), C.size_t(len(key)), bytePtr(out), &outlen) != 1 {
		return nil, ErrGmSSL
	}
	return out[:int(outlen)], nil
}
func (w *SM2KeyWrapper) Unwrap(wrapped []byte) ([]byte, error) {
	if !w.hasPrivate {
		return nil, secure.ErrMissingPrivateKey
	}
	out := make([]byte, sm2MaxPlain)
	var outlen C.size_t
	if C.ogre_sm2_decrypt_der(&w.private, bytePtr(wrapped), C.size_t(len(wrapped)), bytePtr(out), &outlen) != 1 {
		return nil, ErrGmSSL
	}
	return out[:int(outlen)], nil
}

// LegacySM2C1C2C3 preserves the old raw C1||C2||C3 SM2 ciphertext layout.
type LegacySM2C1C2C3 struct {
	public     C.SM2_KEY
	private    C.SM2_KEY
	hasPublic  bool
	hasPrivate bool
}

func NewLegacySM2C1C2C3(privateKey, publicKey []byte) (*LegacySM2C1C2C3, error) {
	c := &LegacySM2C1C2C3{}
	if len(privateKey) > 0 {
		if len(privateKey) != 32 || C.ogre_sm2_key_from_private(&c.private, bytePtr(privateKey)) != 1 {
			return nil, ErrInvalidSM2Key
		}
		c.hasPrivate = true
		c.public = c.private
		c.hasPublic = true
	}
	if len(publicKey) > 0 {
		if (len(publicKey) != 64 && len(publicKey) != 65) || C.ogre_sm2_key_from_public(&c.public, bytePtr(publicKey), C.size_t(len(publicKey))) != 1 {
			return nil, ErrInvalidSM2Key
		}
		c.hasPublic = true
	}
	if !c.hasPublic && !c.hasPrivate {
		return nil, ErrInvalidSM2Key
	}
	return c, nil
}
func (*LegacySM2C1C2C3) Algorithm() secure.Algorithm { return secure.AlgLegacySM2C1C2C3 }
func (c *LegacySM2C1C2C3) Seal(dst, plaintext []byte) ([]byte, error) {
	if len(plaintext) == 0 {
		return dst, nil
	}
	if !c.hasPublic {
		return nil, secure.ErrMissingPublicKey
	}
	if len(plaintext) > sm2MaxPlain {
		return nil, ErrMessageTooLarge
	}
	out := make([]byte, 1+64+len(plaintext)+32)
	var outlen C.size_t
	if C.ogre_sm2_encrypt_c1c2c3(&c.public, bytePtr(plaintext), C.size_t(len(plaintext)), bytePtr(out), &outlen) != 1 {
		return nil, ErrGmSSL
	}
	return append(dst, out[:int(outlen)]...), nil
}
func (c *LegacySM2C1C2C3) Open(dst, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) == 0 {
		return dst, nil
	}
	if !c.hasPrivate {
		return nil, secure.ErrMissingPrivateKey
	}
	out := make([]byte, sm2MaxPlain)
	var outlen C.size_t
	if C.ogre_sm2_decrypt_c1c2c3(&c.private, bytePtr(ciphertext), C.size_t(len(ciphertext)), bytePtr(out), &outlen) != 1 {
		return nil, ErrGmSSL
	}
	return append(dst, out[:int(outlen)]...), nil
}

// LegacySM4CBC preserves the old key/IV truncation and ASCII-space padding rules.
type LegacySM4CBC struct {
	key [16]byte
	iv  [16]byte
}

func normalizeLegacy(src []byte, dst []byte) {
	copy(dst, src)
	if len(src) < len(dst) {
		for i := len(src); i < len(dst); i++ {
			dst[i] = ' '
		}
	}
}
func NewLegacySM4CBC(key, iv []byte) *LegacySM4CBC {
	c := &LegacySM4CBC{}
	normalizeLegacy(key, c.key[:])
	normalizeLegacy(iv, c.iv[:])
	return c
}
func (*LegacySM4CBC) Algorithm() secure.Algorithm { return secure.AlgLegacySM4CBC }
func (c *LegacySM4CBC) Seal(dst, plaintext []byte) ([]byte, error) {
	if len(plaintext) == 0 {
		return dst, nil
	}
	out := make([]byte, len(plaintext)+32)
	var outlen C.size_t
	if C.ogre_sm4_cbc_encrypt((*C.uint8_t)(unsafe.Pointer(&c.key[0])), (*C.uint8_t)(unsafe.Pointer(&c.iv[0])),
		bytePtr(plaintext), C.size_t(len(plaintext)), bytePtr(out), &outlen) != 1 {
		return nil, ErrGmSSL
	}
	return append(dst, out[:int(outlen)]...), nil
}
func (c *LegacySM4CBC) Open(dst, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) == 0 {
		return dst, nil
	}
	out := make([]byte, len(ciphertext)+16)
	var outlen C.size_t
	if C.ogre_sm4_cbc_decrypt((*C.uint8_t)(unsafe.Pointer(&c.key[0])), (*C.uint8_t)(unsafe.Pointer(&c.iv[0])),
		bytePtr(ciphertext), C.size_t(len(ciphertext)), bytePtr(out), &outlen) != 1 {
		return nil, ErrGmSSL
	}
	return append(dst, out[:int(outlen)]...), nil
}

// LegacySM3Method preserves the old non-reversible behavior: Seal returns
// SM3(data), Open returns its input unchanged. New code should use NewSM3.
type LegacySM3Method struct{}
func (LegacySM3Method) Algorithm() secure.Algorithm { return secure.AlgLegacySM3Method }
func (LegacySM3Method) Seal(dst, plaintext []byte) ([]byte, error) {
	if len(plaintext) == 0 {
		return dst, nil
	}
	return SM3{}.Sum(dst, plaintext), nil
}
func (LegacySM3Method) Open(dst, ciphertext []byte) ([]byte, error) {
	return append(dst, ciphertext...), nil
}
