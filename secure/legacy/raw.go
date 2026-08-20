package legacy

import "github.com/qigao/ogrenet/secure"

// Raw is the no-op compatibility transform used by the pre-v2 raw method.
type Raw struct{}

func (Raw) Algorithm() secure.Algorithm { return secure.AlgNone }
func (Raw) Seal(dst, plaintext []byte) ([]byte, error) {
	return append(dst, plaintext...), nil
}
func (Raw) Open(dst, ciphertext []byte) ([]byte, error) {
	return append(dst, ciphertext...), nil
}
