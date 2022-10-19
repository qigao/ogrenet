package main

import (
	"flag"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	ip          = flag.String("ip", "127.0.0.1", "server IP")
	port        = flag.String("port", "8090", "server port")
	connections = flag.Int("conn", 5, "number of tcp connections")
)

func main() {
	flag.Parse()
	setLimit()

	addr := *ip + ":" + *port
	var err error
	var conns []net.Conn

	for i := 0; i < *connections; i++ {
		var c net.Conn

		c, err = net.DialTimeout("tcp", addr, 10*time.Second)

		if err != nil {
			log.Info().Msgf("failed to connect %d %v", i, err)
			i--
			continue
		}
		conns = append(conns, c)
		time.Sleep(time.Millisecond)
	}

	tts := time.Second
	if *connections > 100 {
		tts = time.Millisecond * 5
	}

	func() {
		for i := 0; i < len(conns); i++ {
			conn := conns[i]
			go send(conn, tts)
		}
	}()
	func() {
		for i := 0; i < len(conns); i++ {
			conn := conns[i]
			go recv(conn)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		s := <-c
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			for conn := range conns {
				conns[conn].Close()
			}
			return
		case syscall.SIGHUP:
		default:
			return
		}
	}
}

func send(conn net.Conn, tts time.Duration) {
	time.Sleep(tts)
	send := DemoMsg(body)
	n, err := conn.Write(send)
	log.Info().Msgf("send to server :%d %x %v", n, send, err)
}

func recv(conn net.Conn) {
	buf := make([]byte, 1024)
	n, err := conn.Read(buf[:])
	if err != nil {
		log.Fatal().Msgf("recvd from server err %v ", err)
	}
	data := buf[:n]
	log.Info().Msgf("rvd from server :%d %x", n, data)
	conn.Close()
}

func setLimit() {
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		log.Fatal().Err(err)
	}
	rLimit.Cur = rLimit.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		log.Fatal().Err(err)
	}
}
