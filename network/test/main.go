package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/qigao/ogrenet/network"
)

func main() {
	serve := network.NewOgreNet("0.0.0.0", 8090, &Ws{}, &network.Options{
		ReadBufferSize:    1024,
		WriteBufferSize:   1024,
		ConnectionTimeOut: 1000,
		IsCompressOn:      true,
		CompressLevel:     9,
	})
	go func() {
		serve.Run()
	}()
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		s := <-c
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			serve.Close()
			// time.Sleep(time.Second)
			return
		case syscall.SIGHUP:
		default:
			return
		}
	}
}
