package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/qigao/ogrenet"
)

func main() {
	opts := &ogrenet.Options{}
	proxyNet := ogrenet.NewOgreNetProxy("0.0.0.0", 8090, &Handler{}, opts)
	proxyNet.Run()
	defer proxyNet.Close()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
	<-c
}
