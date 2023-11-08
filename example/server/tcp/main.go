package main

import (
	"os"
	"os/signal"
	"syscall"

	ogre "github.com/qigao/ogrenet"
)

func main() {
	opts := &ogre.Options{}
	net := ogre.NewOgreNet("0.0.0.0", 8090, &Handler{}, opts)
	net.Run()
	defer net.Close()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
	<-c
}
