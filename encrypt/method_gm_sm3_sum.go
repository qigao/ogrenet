package encrypt

import (
	"github.com/ZZMarquis/gm/sm3"
)

type GMSM3SUMMethod struct{}

func (m *GMSM3SUMMethod) Init(key, iv []byte) error {
	return nil
}

func (m *GMSM3SUMMethod) Encrypt(in []byte) (out []byte, err error) {
	if len(in) == 0 {
		return
	}
	hash := sm3.Sum(in)
	return hash[:], nil
}

// 不支持解码
func (m *GMSM3SUMMethod) Decrypt(dst []byte) (src []byte, err error) {
	return dst, nil
}

func (m *GMSM3SUMMethod) Method() uint8 {
	return encryptMethodGMSM3SUM
}
