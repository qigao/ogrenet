package main

import (
	"flag"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/qigao/ogrenet/codecs/passthru"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
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

	func() {
		for i := 0; i < len(conns); i++ {
			conn := conns[i]
			go register(conn)
		}
	}()
	func() {
		for i := 0; i < len(conns); i++ {
			conn := conns[i]
			go recv(conn)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, unix.SIGHUP, unix.SIGQUIT, unix.SIGTERM, unix.SIGINT)
	for {
		s := <-c
		switch s {
		case unix.SIGQUIT, unix.SIGTERM, unix.SIGINT:
			for conn := range conns {
				conns[conn].Close()
			}
			return
		case unix.SIGHUP:
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

func register(conn net.Conn) {
	time.Sleep(time.Second)
	send, err := passthru.NewRegisterCodec([4]byte{0x00, 0x00, 0x00, 0x00}).Encode()
	if err != nil {
		log.Fatal().Msgf("register err %v ", err)
	}
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
	var rLimit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rLimit); err != nil {
		log.Fatal().Err(err)
	}
	rLimit.Cur = rLimit.Max
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &rLimit); err != nil {
		log.Fatal().Err(err)
	}
}
