package tls

const (
	encryptMethodRaw = iota
	encryptMethodAES128CFB
	encryptMethodAES192CFB
	encryptMethodAES256CFB
	encryptMethodGMSM2ECC
	encryptMethodGMSM3SUM
	encryptMethodGMSM4CBC
)

type MethodInterface interface {
	Init(key []byte, iv []byte) error

	Encrypt(src []byte) (dst []byte, err error)

	Decrypt(dst []byte) (src []byte, err error)

	Method() uint8
}
