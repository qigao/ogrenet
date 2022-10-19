package network

import (
	"net"
	"runtime"

	"github.com/qigao/ogrenet/utils"
	"github.com/rs/zerolog/log"
)

var MaxOpenFiles = 1024 * 1024 * 2

func NewServer(network, addr string, fns ...Option) *Server {
	e := new(Server)
	opts := new(Options)
	for _, opt := range fns {
		opt(opts)
	}

	e.options = opts
	e.exitCh = make(chan struct{})
	e.network = network
	e.addr = addr

	return e
}

type Server struct {
	network string
	addr    string

	exitCh chan struct{}

	listener      *Listener
	pollerManager *Manager
	conns         []Conn

	options *Options
}

func (e *Server) Start() (err error) {
	log.Info().Msg("server started")
	e.init()
	// new a listener
	ln, err := e.options.listener(e.network, e.addr)
	if err != nil {
		return err
	}

	listener := new(Listener)
	listener.engine = e
	listener.listener = ln
	listener.addr = ln.Addr()
	e.listener = listener

	// init poller manger
	if e.pollerManager, err = NewManager(e, e.options.numPoller); err != nil {
		return err
	}

	go e.acceptPolling(true)

	return nil
}

func (e *Server) init() {
	if e.options.listener == nil {
		e.options.listener = net.Listen
	}

	if e.options.numPoller <= 0 {
		e.options.numPoller = runtime.NumCPU()
	}
	if e.options.eventHandler == nil {
		e.options.eventHandler = new(eventHandler)
	}
	if e.options.byteBuffer == nil {
		e.options.byteBuffer = new(utils.BufferDefault)
	}

	e.conns = make([]Conn, MaxOpenFiles)
}

func (e *Server) Stop() error {
	close(e.exitCh)
	// listener close
	e.listener.Close()

	// conns close
	for _, conn := range e.conns {
		if conn == nil {
			continue
		}
		conn.Close()
	}

	// poller stop
	e.pollerManager.Stop()

	return nil
}

func (e *Server) GetByteBuffer() utils.ByteBuffer {
	return e.options.byteBuffer
}

func (e *Server) GetEventHandler() EventHandler {
	return e.options.eventHandler
}

func (e *Server) AddConn(conn Conn) {
	e.conns[conn.Fd()] = conn
}

func (e *Server) Remove(pd int) {
	e.conns[pd] = nil
}

func (e *Server) GetConn(pd int) Conn {
	if pd >= len(e.conns) {
		log.Fatal().Msgf("fd conn is not exist")
	}
	return e.conns[pd]
}

func (e *Server) acceptPolling(localOSThread bool) error {
	if localOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	handler := e.GetEventHandler()

	for {
		select {
		case <-e.exitCh:
			log.Info().Msg("exit polling")
			return nil
		default:
			nc, err := e.listener.Accept()
			if err != nil {
				continue
			}
			if nc == nil {
				continue
			}
			ec := nc.(*conn)
			poller := e.pollerManager.Pick(ec.Fd())
			ec.poller = poller

			// set ctx
			ec.ctx = handler.OnOpen(ec)
			if err = poller.AddRead(ec.Fd()); err != nil {
				log.Info().Msgf("poller.AddRead: %v", err)
				nc.Close()
				continue
			}
			e.conns[ec.Fd()] = ec
		}
	}
}
