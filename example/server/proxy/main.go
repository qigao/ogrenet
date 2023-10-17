package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/qigao/ogrenet/options"

	"github.com/qigao/ogrenet/network"
)

func main() {
	opts := &options.Options{}
	net := network.NewOgreNetProxy("0.0.0.0", 8090, &Handler{}, opts)
	net.Run()
	defer net.Close()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
	<-c
}
