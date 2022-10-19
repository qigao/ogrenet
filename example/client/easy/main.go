package main

import (
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"
)

func main() {
	conn, err := net.Dial("tcp", ":8090")
	if err != nil {
		log.Error().Err(err)
	}
	for int := 0; int < 100; int++ {
		n, err := conn.Write([]byte("hello world"))
		if err != nil {
			log.Error().Err(err)
		}

		go func() {
			b := make([]byte, 100)
			if n, err = conn.Read(b); err != nil {
				log.Error().Err(err)
			}
			log.Info().Msgf("read data: %d, %s", n, string(b))
		}()
	}
	defer conn.Close()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	<-ch
}
