package ogrenet

import (
	"time"

	"github.com/rs/zerolog/log"

	"github.com/qigao/ogrenet/tls"
)

// CipherOption   Server初始化参数
type CipherOption struct {
	EncryptMethod tls.MethodInterface // 数据加解密算法
	Timeout       time.Duration       // 连接读写超时时间
	PrivateKey    []byte              // 加解密算法私钥
	PublicKey     []byte              // 加解密算法公钥
}

type Cipher interface {
	apply(*CipherOption)
}

type funcServerOption struct {
	f func(*CipherOption)
}

func (fdo *funcServerOption) apply(do *CipherOption) {
	fdo.f(do)
}

func newFuncServerOption(f func(*CipherOption)) *funcServerOption {
	return &funcServerOption{
		f: f,
	}
}

// WithEncryptMethod 设置加解密方法
func WithEncryptMethod(encryptMethod tls.MethodInterface) Cipher {
	return newFuncServerOption(func(o *CipherOption) {
		o.EncryptMethod = encryptMethod
	})
}

// WithTimeout 设置TCP超时检查的间隔时间以及超时时间
func WithTimeout(timeout time.Duration) Cipher {
	return newFuncServerOption(func(o *CipherOption) {
		if timeout < 0 {
			log.Fatal().Msg("timeoutTicker must greater than 0")
		}
		o.Timeout = timeout
	})
}

// WithPrivateKey 设置加解密私钥
func WithPrivateKey(privateKey []byte) Cipher {
	return newFuncServerOption(func(o *CipherOption) {
		if len(privateKey) == 0 {
			log.Fatal().Msg("privateKey not be nil")
		}
		o.PrivateKey = privateKey
	})
}

// WithPublicKey 设置加解密公钥
func WithPublicKey(publicKey []byte) Cipher {
	return newFuncServerOption(func(o *CipherOption) {
		if len(publicKey) == 0 {
			log.Fatal().Msg("publicKey not be nil")
		}
		o.PublicKey = publicKey
	})
}

func GetOptions(opts ...Cipher) *CipherOption {
	options := &CipherOption{}

	for _, o := range opts {
		if o != nil {
			o.apply(options)
		}
	}
	return options
}
