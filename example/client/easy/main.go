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
	for i := 0; i < 5000; i++ {
		_, err := conn.Write(MyBody(body))
		if err != nil {
			log.Error().Err(err)
		}

		go func() {
			b := make([]byte, 256)
			if n, err := conn.Read(b); err != nil {
				log.Error().Err(err).Msgf("sent byte: %d err: %v", n, err)
			}
			log.Info().Msgf("recvd data: %d, %x", i, b)
		}()
	}
	defer conn.Close()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	<-ch
}
