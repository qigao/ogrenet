package main

import (
	"os"
	"os/signal"

	"github.com/qigao/ogrenet"
	codec "github.com/qigao/ogrenet/codecs/passthru"
	"golang.org/x/sys/unix"
)

func main() {
	codecPool := codec.NewCodecPool()
	handle := NewProxyHandler(codecPool)
	proxyNet := ogrenet.NewOgreNet("tcp4", "0.0.0.0", 8090, handle)
	proxyNet.Run()
	defer proxyNet.Close()

	c := make(chan os.Signal, 1)
	signal.Notify(c, unix.SIGTERM, unix.SIGQUIT, unix.SIGINT)
	<-c
}
