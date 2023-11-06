package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	//svr := ogrenet.NewServe(":5005", new(Handle),
	//	//options.WithEncryptMethod(new(tls.AES256CFBMethod)),
	//	//options.WithEncryptMethodPublicKey([]byte(tls.MagicKey)),
	//	//options.WithEncryptMethodPrivateKey([]byte(tls.MagicKey[:16])),
	//	options.WithTimeout(5*time.Second),
	//)
	caCertFile, err := os.ReadFile("./certs/CertAuth.crt")
	if err != nil {
		log.Fatalf("error reading CA certificate: %v", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCertFile)

	certificate, err := tls.LoadX509KeyPair("./certs/server.crt", "./certs/server.key")
	if err != nil {
		log.Fatalf("could not load certificate: %v", err)
	}
	// Create the TLS Config with the CA pool and enable Client certificate validation
	tlsConfig := &tls.Config{
		Certificates:     []tls.Certificate{certificate},
		ClientCAs:        caCertPool,
		ClientAuth:       tls.RequireAndVerifyClientCert,
		MinVersion:       tls.VersionTLS12,
		CurvePreferences: []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		},
	}
	ln, err := net.Listen("tcp", ":5005")
	if err != nil {
		panic(err)
	}
	defer ln.Close()
	tlsListener := tls.NewListener(ln, tlsConfig)
	defer tlsListener.Close()
	for {
		conn, err := tlsListener.Accept()
		if err != nil {
			fmt.Println("new tls client connection error ", err)
		}
		go func() {
			for {
				buf := make([]byte, 1024)
				n, err := conn.Read(buf)
				if err != nil {
					fmt.Println("recv err ", err)
					conn.Close()
					return
				}
				fmt.Println("recv : ", string(buf[:n]))
			}
		}()
	}
}
