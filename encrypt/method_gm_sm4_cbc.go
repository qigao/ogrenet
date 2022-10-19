package encrypt

import (
	"bytes"

	"github.com/ZZMarquis/gm/sm4"
	"github.com/ZZMarquis/gm/util"
)

type GMSM4CBCMethod struct {
	iv  []byte
	key []byte
}

func (m *GMSM4CBCMethod) Init(key, iv []byte) error {
	l := len(key)
	if l > sm4.BlockSize {
		key = key[:16]
	} else if l < sm4.BlockSize {
		key = append(key, bytes.Repeat([]byte{' '}, 16-l)...)
	}

	// 判断iv长度
	l2 := len(iv)
	if l2 > sm4.BlockSize {
		iv = iv[:sm4.BlockSize]
	} else if l2 < sm4.BlockSize {
		iv = append(iv, bytes.Repeat([]byte{' '}, sm4.BlockSize-l2)...)
	}
	m.key = key
	m.iv = iv

	return nil
}

func (m *GMSM4CBCMethod) Encrypt(in []byte) (dst []byte, err error) {
	if len(in) == 0 {
		return
	}
	cipherText, err := sm4.CBCEncrypt(m.key, m.iv, util.PKCS5Padding(in, sm4.BlockSize))
	if err != nil {
		return nil, err
	}
	return cipherText, nil
}

func (m *GMSM4CBCMethod) Decrypt(out []byte) (in []byte, err error) {
	if len(out) == 0 {
		return
	}

	plainTextWithPadding, err := sm4.CBCDecrypt(m.key, m.iv, out)
	if err != nil {
		return nil, err
	}
	return util.PKCS5UnPadding(plainTextWithPadding), nil
}

func (m *GMSM4CBCMethod) Method() uint8 {
	return encryptMethodGMSM4CBC
}
