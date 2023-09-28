package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/qigao/ogrenet/network"
)

func main() {
	e := network.NewOgreNet("tcp", ":8090",
		network.WithNumPoller(5), network.WithEventHandler(&Handler{}))

	if err := e.Start(); err != nil {
		panic(err)
	}

	defer e.Stop()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
	<-c
}
