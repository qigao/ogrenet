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
	return nil, ErrProtocolUnsupported
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
	return nil, ErrProtocolUnsupported
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

func (e *epollEngine) snapshotManagedLocked() []epollManagedResource {
	out := make([]epollManagedResource, 0, len(e.managed))
	for _, resource := range e.managed {
		out = append(out, resource)
	}
	return out
}

func (e *epollEngine) maybeQuiescentLocked() {
	if e.state == engineRunning || e.activeOps != 0 || len(e.managed) != 0 || !e.admission.idle() {
		return
	}
	e.quiescentOnce.Do(func() { close(e.quiescent) })
}

func (e *epollEngine) selectReactor() *epollReactor {
	if e == nil || len(e.reactors) == 0 {
		return nil
	}
	index := e.nextReactor.Add(1) - 1
	return e.reactors[index%uint64(len(e.reactors))]
}

func (e *epollEngine) nextResourceID() (uint64, error) {
	for {
		current := e.nextID.Load()
		if current >= math.MaxUint64-1 {
			return 0, errNativeResourceIDExhausted
		}
		if e.nextID.CompareAndSwap(current, current+1) {
			return current + 1, nil
		}
	}
}

func (e *epollEngine) wakeAll() {
	if e == nil {
		return
	}
	for _, reactor := range e.reactors {
		_ = reactor.poller.Wake()
	}
}

func (e *epollEngine) wakeWorkerWaiters() {
	if e == nil {
		return
	}
	for _, reactor := range e.reactors {
		if reactor.hasWorkerBlocked.Load() {
			reactor.signalControl(epollControlWorkerCapacity)
		}
	}
}

func (e *epollEngine) onReactorFatal(err error) {
	if e == nil || err == nil {
		return
	}
	e.mu.Lock()
	if e.state == engineDone {
		e.mu.Unlock()
		return
	}
	if e.shutdownErr == nil {
		e.shutdownErr = err
	} else {
		e.shutdownErr = errors.Join(e.shutdownErr, err)
	}
	e.state = engineAborting
	if e.shutdownReason == abortNone {
		e.shutdownReason = abortFailure
	}
	managed := e.snapshotManagedLocked()
	e.maybeQuiescentLocked()
	e.mu.Unlock()

	for _, resource := range managed {
		resource.requestEngineAbort(abortFailure)
	}
	e.wakeAll()
}

func (e *epollEngine) finalize() {
	<-e.quiescent
	for _, reactor := range e.reactors {
		reactor.signalControl(epollControlStop)
	}
	e.reactorWG.Wait()
	for _, reactor := range e.reactors {
		_ = reactor.poller.Close()
	}
	e.callbacks.stopIdle()
	e.observer.stop()

	e.mu.Lock()
	e.state = engineDone
	e.mu.Unlock()
	e.doneOnce.Do(func() { close(e.done) })
}
