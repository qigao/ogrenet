package network

import (
	"crypto/tls"
	"net"

	"github.com/rs/zerolog/log"
)

type Server struct {
	address string
	// 服务参数
	options *Options
	// 处理消息回调接口
	handler Handler
}

func NewServer(address string, handler Handler, opts ...Option) *Server {
	SetLimit()
	return &Server{
		options: GetOptions(opts...),
		address: address,
		handler: handler,
	}
}

func (s *Server) RunUDP() error {
	log.Info().Msgf("[Server] Run %s udp server", s.address)
	udpAddr, err := net.ResolveUDPAddr("udp", s.address)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}

	newConnection(conn, s.handler, s.options, true, false).setupUDP()
	return nil
}

func (s *Server) RunTCP() error {
	log.Info().Msgf("[Server] Run tcp server %s", s.address)
	ln, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Error().Msgf("new tcp client connection error: %v ", err)
		}
		newConnection(conn, s.handler, s.options, false, false).setupTCP()
	}
}

func (s *Server) RunTLS(cfg *tls.Config) error {
	log.Info().Msgf("[Server] Run tls server: %s", s.address)

	ln, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	defer ln.Close()
	tlsListener := tls.NewListener(ln, cfg)
	defer tlsListener.Close()
	for {
		conn, err := tlsListener.Accept()
		if err != nil {
			log.Error().Msgf("new tls client connection error: %v ", err)
		}
		newConnection(conn.(*tls.Conn), s.handler, s.options, false, false).setupTLS()
	}
}
