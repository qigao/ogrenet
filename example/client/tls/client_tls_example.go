package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	caCrt = "./certs/CertAuth.crt"
	crt   = "./certs/client.crt"
	key   = "./certs/client.key"
)

func main() {
	cert, err := os.ReadFile(caCrt)
	if err != nil {
		log.Fatalf("could not open certificate file: %v", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(cert)

	certificate, err := tls.LoadX509KeyPair(crt, key)
	if err != nil {
		log.Fatalf("could not load certificate: %v", err)
	}

	// Create a HTTPS client and supply the created CA pool and certificate
	tlsConfig := &tls.Config{
		RootCAs:      caCertPool,
		ClientCAs:    caCertPool,
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}

	c, err := tls.Dial("tcp", "127.0.0.1:5005", tlsConfig)
	if err != nil {
		panic(err)
	}
	for {
		fmt.Println("send hello")
		_, err := c.Write([]byte("hello"))
		if err != nil {
			fmt.Println(err)
			break
		}
		time.Sleep(time.Second)
	}
	c.Close()
	return
}
