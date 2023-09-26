package main

import (
	"github.com/qigao/ogrenet/encrypt"
	"github.com/qigao/ogrenet/network"
	"time"
)

func main() {
	method, err := encrypt.NewMethodInstance("aes-256-cfb", encrypt.MagicKey, encrypt.MagicKey[:16])
	if err != nil {
		panic(err)
	}
	svr := network.NewServer(":5005", new(Handle),
		network.WithEncryptMethod(method),
		network.WithTimeout(5*time.Second),
	)
	if err != nil {
		panic(err)
	}
	err = svr.RunTCP()
	if err != nil {
		panic(err)
	}
}
