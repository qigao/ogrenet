package network

import (
	"net"
	"runtime"
	"sync"

	"github.com/qigao/ogrenet/buffer"
	"golang.org/x/exp/maps"

	"github.com/rs/zerolog/log"
)

type OgreNet struct {
	network string
	addr    string

	exitCh      chan struct{}
	mutex       sync.RWMutex
	netListener *NetListener
	poller      *NetPollManager
	conns       map[int]Conn

	options *Options
}

func NewOgreNet(network, addr string, fns ...Option) *OgreNet {
	e := new(OgreNet)
	opts := new(Options)
	for _, opt := range fns {
		opt(opts)
	}

	e.options = opts
	e.exitCh = make(chan struct{})
	e.network = network
	e.addr = addr
	e.mutex = sync.RWMutex{}
	return e
}

func (n *OgreNet) Start() (err error) {
	log.Info().Msg("server started")
	n.init()
	// new a listener
	ln, err := n.options.listener(n.network, n.addr)
	if err != nil {
		return err
	}

	listener := new(NetListener)
	listener.listener = ln
	n.netListener = listener

	// init poller manger
	if n.poller, err = NewNetPollManager(n, n.options.numPoller); err != nil {
		return err
	}

	go n.acceptPolling(true)

	return nil
}

func (n *OgreNet) init() {
	if n.options.listener == nil {
		n.options.listener = net.Listen
	}

	if n.options.numPoller <= 0 {
		n.options.numPoller = runtime.NumCPU()
	}
	if n.options.eventHandler == nil {
		n.options.eventHandler = new(DefaultEventHandler)
	}
	n.conns = make(map[int]Conn)
}

func (n *OgreNet) Stop() error {
	close(n.exitCh)
	// listener close
	n.netListener.Close()

	// conns close
	n.mutex.Lock()
	for _, conn := range n.conns {
		conn.Close()
	}
	maps.Clear(n.conns)
	n.mutex.Unlock()
	// poller stop
	n.poller.Stop()

	return nil
}

func (n *OgreNet) GetEventHandler() EventHandler {
	return n.options.eventHandler
}

func (n *OgreNet) AddConn(conn Conn) {
	n.mutex.Lock()
	n.conns[conn.Fd()] = conn
	n.mutex.Unlock()
}

func (n *OgreNet) Remove(pd int) {
	n.mutex.Lock()
	delete(n.conns, pd)
	n.mutex.Unlock()
}

func (n *OgreNet) GetConn(pd int) Conn {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.conns[pd]
}

func (n *OgreNet) acceptPolling(localOSThread bool) error {
	if localOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	handler := n.GetEventHandler()

	for {
		select {
		case <-n.exitCh:
			log.Info().Msg("exit polling")
			return nil
		default:
			nc, err := n.netListener.Accept()
			if err != nil {
				continue
			}
			if nc == nil {
				continue
			}
			ec := nc.(*OgreConn)
			ec.laddr = nc.LocalAddr()
			ec.raddr = nc.RemoteAddr()
			poller := n.poller.Pick(ec.Fd())
			ec.poller = poller
			ec.wBuf = buffer.NewSpscRingBuffer(capacity)
			ec.rBuf = buffer.NewSpscRingBuffer(capacity)
			// set ctx
			ec.ctx = handler.OnOpen(ec)
			if err = poller.AddRead(ec.Fd()); err != nil {
				log.Info().Msgf("poller.AddRead: %v", err)
				nc.Close()
				continue
			}
			n.conns[ec.Fd()] = ec
		}
	}
}
