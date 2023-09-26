package main

import (
	encrypt2 "github.com/qigao/ogrenet/src/encrypt"
	network2 "github.com/qigao/ogrenet/src/network"
	"time"
)

func main() {
	method, err := encrypt2.NewMethodInstance("aes-256-cfb", encrypt2.MagicKey, encrypt2.MagicKey)
	if err != nil {
		panic(err)
	}
	svr := network2.NewServer(":5005", new(Handle),
		network2.WithEncryptMethod(method),
		network2.WithTimeout(5*time.Second),
	)
	if err != nil {
		panic(err)
	}
	err = svr.RunTCP()
	if err != nil {
		panic(err)
	}
}
