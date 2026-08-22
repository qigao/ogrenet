//go:build linux

package transport

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/epoll"
)

type epollManagedKind uint8

const (
	epollManagedListener epollManagedKind = iota + 1
	epollManagedSession
	epollManagedPacket
)

type epollManagedResource interface {
	managedID() uint64
	managedKind() epollManagedKind
	prepareEngineDrain()
	requestEngineShutdown()
	requestEngineAbort(abortReason)
}

var (
	errNativeResourceIDExhausted = errors.New("transport: native resource id exhausted")
	errEpollDuplicateManagedID   = errors.New("transport: duplicate epoll managed resource id")
)

type epollEngine struct {
	cfg      config
	epollCfg resolvedEpollConfig

	admission *admissionController
	observer  *observerDispatcher
	callbacks *epollCallbackExecutor
	reactors  []*epollReactor

	mu             sync.Mutex
	state          engineState
	shutdownReason abortReason
	shutdownErr    error
	activeOps      int
	managed        map[uint64]epollManagedResource

	nextReactor atomic.Uint64
	nextID      atomic.Uint64

	quiescent     chan struct{}
	quiescentOnce sync.Once
	reactorWG     sync.WaitGroup
	done          chan struct{}
	doneOnce      sync.Once
}

var _ ogrenet.Engine = (*epollEngine)(nil)

func newEpollEngine(cfg config, resolved resolvedEpollConfig) (*epollEngine, error) {
	e := &epollEngine{
		cfg:       cfg,
		epollCfg:  resolved,
		admission: newAdmissionController(cfg.limits),
		observer:  newObserverDispatcher(cfg.observer, cfg.observerBuffer),
		state:     engineRunning,
		managed:   make(map[uint64]epollManagedResource),
		quiescent: make(chan struct{}),
		done:      make(chan struct{}),
	}

	for i := 0; i < resolved.pollers; i++ {
		p, err := epoll.Open()
		if err != nil {
			for _, reactor := range e.reactors {
				_ = reactor.poller.Close()
			}
			e.observer.stop()
			return nil, fmt.Errorf("transport: open epoll reactor %d: %w", i, err)
		}
		e.reactors = append(e.reactors, &epollReactor{
			index:     i,
			cfg:       resolved,
			poller:    p,
			events:    make([]epoll.Event, resolved.eventBatch),
			resources: make(map[uint64]epollEventResource),
		})
	}

	e.callbacks = newEpollCallbackExecutor(resolved.callbackWorkers, resolved.callbackQueue, e.wakeWorkerWaiters)
	for _, reactor := range e.reactors {
		reactor.onFatal = e.onReactorFatal
		reactor.workerCapacityAvailable = e.callbacks.hasCapacity
		e.reactorWG.Add(1)
		go func(r *epollReactor) {
			defer e.reactorWG.Done()
			r.run()
		}(reactor)
	}
	go e.finalize()
	return e, nil
}

func (e *epollEngine) Listen(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.Handler) (ogrenet.Listener, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.Validate(); err != nil {
		return nil, err
	}
	if !endpoint.Scheme.IsSession() {
		return nil, ErrProtocolMismatch
	}
	if endpoint.Scheme != ogrenet.SchemeTCP {
		return nil, ErrProtocolUnsupported
	}
	return e.listenNativeTCP(ctx, endpoint, h)
}

func (e *epollEngine) Dial(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.Handler) (ogrenet.Session, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.ValidateDial(); err != nil {
		return nil, err
	}
	if !endpoint.Scheme.IsSession() {
		return nil, ErrProtocolMismatch
	}
	if endpoint.Scheme != ogrenet.SchemeTCP {
		return nil, ErrProtocolUnsupported
	}
	return e.dialNativeTCP(ctx, endpoint, h, nil)
}

func (e *epollEngine) ListenPacket(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.PacketHandler) (ogrenet.PacketConn, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.Validate(); err != nil {
		return nil, err
	}
	if !endpoint.Scheme.IsPacket() {
		return nil, ErrProtocolMismatch
	}
	if endpoint.Scheme != ogrenet.SchemeUDP {
		return nil, ErrProtocolUnsupported
	}
	return e.listenNativeUDP(ctx, endpoint, h)
}

func (e *epollEngine) DialPacket(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.PacketHandler) (ogrenet.PacketConn, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.ValidateDial(); err != nil {
		return nil, err
	}
	if !endpoint.Scheme.IsPacket() {
		return nil, ErrProtocolMismatch
	}
	if endpoint.Scheme != ogrenet.SchemeUDP {
		return nil, ErrProtocolUnsupported
	}
	return e.dialNativeUDP(ctx, endpoint, h)
}

func (e *epollEngine) Stats() ogrenet.EngineStats {
	if e == nil {
		return ogrenet.EngineStats{}
	}
	return engineStatsSnapshot(e.admission, e.observer)
}

func (e *epollEngine) Done() <-chan struct{} { return e.done }

func (e *epollEngine) Shutdown(ctx context.Context) error { return e.shutdownNative(ctx) }

func (e *epollEngine) Close() error { return e.closeNative() }

func (e *epollEngine) beginOp() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != engineRunning {
		return ErrClosed
	}
	e.activeOps++
	return nil
}

func (e *epollEngine) endOp() {
	e.mu.Lock()
	if e.activeOps > 0 {
		e.activeOps--
	}
	e.maybeQuiescentLocked()
	e.mu.Unlock()
}

func (e *epollEngine) addManaged(resource epollManagedResource) error {
	if resource == nil || resource.managedID() == 0 {
		return errEpollInvalidResourceID
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != engineRunning {
		return ErrClosed
	}
	id := resource.managedID()
	if _, exists := e.managed[id]; exists {
		return errEpollDuplicateManagedID
	}
	e.managed[id] = resource
	return nil
}

func (e *epollEngine) removeManaged(id uint64) {
	e.mu.Lock()
	delete(e.managed, id)
	e.maybeQuiescentLocked()
	e.mu.Unlock()
}

func (e *epollEngine) beginOpSnapshot() (engineState, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state, e.activeOps
}

func (e *epollEngine) nextResourceID() (uint64, error) {
	for {
		current := e.nextID.Load()
		if current == math.MaxUint64 {
			return 0, errNativeResourceIDExhausted
		}
		if e.nextID.CompareAndSwap(current, current+1) {
			return current + 1, nil
		}
	}
}

func (e *epollEngine) selectReactor() *epollReactor {
	if e == nil || len(e.reactors) == 0 {
		return nil
	}
	idx := e.nextReactor.Add(1) - 1
	return e.reactors[idx%uint64(len(e.reactors))]
}

func (e *epollEngine) wakeAll() {
	for _, reactor := range e.reactors {
		reactor.wake()
	}
}

func (e *epollEngine) wakeWorkerWaiters() {
	for _, reactor := range e.reactors {
		reactor.wakeWorkerBlocked()
	}
}
