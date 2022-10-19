package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/qigao/ogrenet/codecs/modbus"

	"github.com/qigao/ogrenet/network"
)

func main() {
	opts := &network.Options{
		Codec:       modbus.NewEmptyModbusCodec(),
		EventHandle: &Handler{},
	}
	net := network.NewOgreNet("0.0.0.0", 8090, opts)
	net.Run()
	defer net.Close()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
	<-c
}
