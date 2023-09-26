package main

import (
	network2 "github.com/qigao/ogrenet/src/network"
	"time"
)

func main() {
	svr := network2.NewServer(":5005", new(Handle),
		// options.WithEncryptMethod(new(encrypt.AES256CFBMethod)),
		// options.WithEncryptMethodPublicKey([]byte(encrypt.MagicKey)),
		// options.WithEncryptMethodPrivateKey([]byte(encrypt.MagicKey[:16])),
		network2.WithTimeout(5*time.Second),
	)
	err := svr.RunUDP()
	if err != nil {
		panic(err)
	}
	select {}
}
