package main

import (
	"os"
	"os/signal"

	"github.com/qigao/ogrenet"
	codec "github.com/qigao/ogrenet/codecs/passthru"
	"golang.org/x/sys/unix"
)

func main() {
	opts := &ogrenet.Options{}
	codecPool := codec.NewCodecPool()
	handle := NewProxyHandler(codecPool)
	proxyNet := ogrenet.NewOgreNet("0.0.0.0", 8090, handle, opts)
	proxyNet.Run()
	defer proxyNet.Close()

	c := make(chan os.Signal, 1)
	signal.Notify(c, unix.SIGTERM, unix.SIGQUIT, unix.SIGINT)
	<-c
}
