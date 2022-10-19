package main

import (
	"time"

	"github.com/qigao/ogrenet/network"
)

func main() {
	svr := network.NewServer(":5005", new(Handle),
		// options.WithEncryptMethod(new(encrypt.AES256CFBMethod)),
		// options.WithEncryptMethodPublicKey([]byte(encrypt.MagicKey)),
		// options.WithEncryptMethodPrivateKey([]byte(encrypt.MagicKey[:16])),
		network.WithTimeout(5*time.Second),
	)
	err := svr.RunUDP()
	if err != nil {
		panic(err)
	}
	select {}
}
