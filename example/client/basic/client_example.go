package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/qigao/ogrenet/tls"
	"github.com/rs/zerolog/log"
)

var (
	ip            = flag.String("ip", "127.0.0.1", "server IP")
	port          = flag.String("port", "8090", "server port")
	protocol      = flag.String("proto", "tcp", "server type tcp / udp")
	connections   = flag.Int("conn", 1, "number of tcp connections")
	caCrt         = flag.String("ca", "./certs/ca.crt", "tls ca cert")
	crt           = flag.String("crt", "./certs/client.crt", "tls client cert file")
	key           = flag.String("key", "./certs/client.key", "tls client key file")
	encryptMethod = flag.String("encryptMethod", "", "set send or recv buffer encryptMethod method")
)

func main() {
	flag.Parse()

	setLimit()

	addr := *ip + ":" + *port
	var enc tls.MethodInterface
	var err error
	var conns []net.Conn
	if *encryptMethod != "" {
		enc, err = tls.NewMethodInstance(*encryptMethod, tls.MagicKey, tls.MagicKey[:16])
		if err != nil {
			log.Fatal().Err(err).Msgf("set encryptMethod method errrr: %v", err)
		}
	}
	log.Printf("连接到 %s", addr)

	for i := 0; i < *connections; i++ {
		var c net.Conn
		if *protocol == "tls" {

			cert, err := os.ReadFile(*caCrt)
			if err != nil {
				log.Fatal().Err(err).Msgf("could not open certificate file: %v", err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(cert)

			certificate, err := tls.LoadX509KeyPair(*crt, *key)
			if err != nil {
				log.Fatal().Err(err).Msgf("could not load certificate: %v", err)
			}

			// Create a HTTPS client and supply the created CA pool and certificate
			tlsConfig := &tls.Config{
				RootCAs:            caCertPool,
				ClientCAs:          caCertPool,
				Certificates:       []tls.Certificate{certificate},
				ClientAuth:         tls.RequireAndVerifyClientCert,
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: true,
			}

			c, err = tls.Dial("tcp", addr, tlsConfig)
		} else {
			c, err = net.DialTimeout(*protocol, addr, 10*time.Second)
		}

		if err != nil {
			log.Info().Msgf("failed to connect %d %v", i, err)
			i--
			continue
		}
		conns = append(conns, c)
		time.Sleep(time.Millisecond)
	}

	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	log.Printf("完成初始化 %d 连接", len(conns))

	tts := time.Second
	if *connections > 100 {
		tts = time.Millisecond * 5
	}

	for {
		buf := make([]byte, 1024)
		for i := 0; i < len(conns); i++ {
			time.Sleep(tts)
			conn := conns[i]
			log.Printf("连接 %d 发送数据", i)
			send := []byte("hello world\r\n")
			if enc != nil {
				enSend, err := enc.Encrypt(send)
				if err != nil {
					log.Fatal().Msgf("encryptMethod encode error %v", err)
				}
				n, err := conn.Write(enSend)
				log.Info().Msgf(" send to server :%d hello world %v", n, err)
			} else {
				n, err := conn.Write(send)
				log.Info().Msgf(" send to server :%d hello world %v", n, err)
			}
			n, err := conn.Read(buf[:])
			if err != nil {
				log.Fatal().Msgf(" recv from server err %v ", err)
			}
			if *protocol != "udp" {
				var recv []byte
				if enc != nil {
					recv, err = enc.Decrypt(buf[:n])
					if err != nil {
						log.Fatal().Msgf(" recv from server byte decode error  %v", err)
					}
				} else {
					recv = buf[:n]
				}
				log.Info().Msgf(string(recv))
			}
		}
	}
	// select{}
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
