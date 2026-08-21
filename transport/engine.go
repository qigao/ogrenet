package transport

import (
	"sync"
	"sync/atomic"
)

type engineState uint8

const (
	engineRunning engineState = iota
	engineDraining
	engineAborting
	engineDone
)

// Engine is the portable production implementation of ogrenet.Engine. Native
// poller packages remain independently available below this layer.
type Engine struct {
	cfg       config
	admission *admissionController
	observer  *observerDispatcher

	mu              sync.Mutex
	state           engineState
	shutdownReason  abortReason
	shutdownErr     error
	activeOps       int
	streamListeners map[*listener]struct{}
	wsListeners     map[*wsListener]struct{}
	streams         map[*conn]struct{}
	websockets      map[*wsSession]struct{}
	packets         map[*packetConn]struct{}
	streamLeases    map[*conn]*connectionLease
	wsLeases        map[*wsSession]*connectionLease
	packetLeases    map[*packetConn]*connectionLease
	done            chan struct{}
	doneOnce        sync.Once
	nextID          atomic.Uint64
}

func New(opts ...Option) (*Engine, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	if err := cfg.limits.validate(); err != nil {
		return nil, err
	}
	return &Engine{
		cfg:             cfg,
		admission:       newAdmissionController(cfg.limits),
		observer:        newObserverDispatcher(cfg.observer, cfg.observerBuffer),
		state:           engineRunning,
		streamListeners: make(map[*listener]struct{}),
		wsListeners:     make(map[*wsListener]struct{}),
		streams:         make(map[*conn]struct{}),
		websockets:      make(map[*wsSession]struct{}),
		packets:         make(map[*packetConn]struct{}),
		streamLeases:    make(map[*conn]*connectionLease),
		wsLeases:        make(map[*wsSession]*connectionLease),
		packetLeases:    make(map[*packetConn]*connectionLease),
		done:            make(chan struct{}),
	}, nil
}
