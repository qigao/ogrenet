package main

import (
	"os"
	"os/signal"

	ogre "github.com/qigao/ogrenet"
	"golang.org/x/sys/unix"
)

func main() {
	net := ogre.NewOgreNet("tcp", "0.0.0.0", 8090, &Handler{}, ogre.WithNumPoller(2))
	net.Run()
	defer net.Close()

	c := make(chan os.Signal, 1)
	signal.Notify(c, unix.SIGTERM, unix.SIGQUIT, unix.SIGINT)
	<-c
}
