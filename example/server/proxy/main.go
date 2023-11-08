package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/qigao/ogrenet"
	codec "github.com/qigao/ogrenet/codecs/passthru"
)

func main() {
	opts := &ogrenet.Options{}
	codecPool := codec.NewCodecPool()
	handle := NewProxyHandler(codecPool)
	proxyNet := ogrenet.NewOgreNet("0.0.0.0", 8090, handle, opts)
	proxyNet.Run()
	defer proxyNet.Close()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
	<-c
}
