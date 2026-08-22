//go:build linux

package transport

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

type epollStressClientHandler struct {
	opened chan struct{}
	echoed chan ogrenet.Message
}

func (h *epollStressClientHandler) OnOpen(ogrenet.Session) {
	close(h.opened)
}

func (h *epollStressClientHandler) OnMessage(_ ogrenet.Session, msg ogrenet.Message) {
	h.echoed <- msg
}

func (h *epollStressClientHandler) OnClose(ogrenet.Session, error) {}

type epollStressProgress struct {
	dialStarted      atomic.Int64
	dialDone         atomic.Int64
	openDone         atomic.Int64
	clientSendDone   atomic.Int64
	clientEchoDone   atomic.Int64
	clientReadClosed atomic.Int64
	clientClose      atomic.Int64
	clientDone       atomic.Int64
	serverMessage    atomic.Int64
	serverSendDone   atomic.Int64
	serverReadClosed atomic.Int64
	serverClose      atomic.Int64
	serverDone       atomic.Int64
}

func (p *epollStressProgress) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf(
		"dial=%d/%d open=%d clientSend=%d echo=%d clientReadClosed=%d clientClose=%d clientDone=%d serverMessage=%d serverSend=%d serverReadClosed=%d serverClose=%d serverDone=%d",
		p.dialDone.Load(), p.dialStarted.Load(), p.openDone.Load(), p.clientSendDone.Load(), p.clientEchoDone.Load(),
		p.clientReadClosed.Load(), p.clientClose.Load(), p.clientDone.Load(), p.serverMessage.Load(), p.serverSendDone.Load(),
		p.serverReadClosed.Load(), p.serverClose.Load(), p.serverDone.Load(),
	)
}

type epollStressReactorSnapshot struct {
	index            int
	resources        int
	sessions         int
	handoff          int
	connecting       int
	codecSetup       int
	opening          int
	active           int
	terminal         int
	closed           int
	callbackNeedOpen int
	callbackOpen     int
	callbackIdle     int
	callbackMessage  int
	callbackNeedClose int
	callbackClose    int
	callbackClosed   int
	writeActive      int
	writeBlocked     int
	readReady        int
	terminalPrepared int
	workerBlocked    int
}

func captureEpollStressReactor(r *epollReactor) epollStressReactorSnapshot {
	snap := epollStressReactorSnapshot{index: r.index, resources: len(r.resources), workerBlocked: len(r.workerBlocked)}
	for _, resource := range r.resources {
		s, ok := resource.(*epollSession)
		if !ok || s == nil {
			continue
		}
		snap.sessions++
		switch s.state {
		case epollSessionHandoff:
			snap.handoff++
		case epollSessionConnecting:
			snap.connecting++
		case epollSessionCodecSetup:
			snap.codecSetup++
		case epollSessionOpening:
			snap.opening++
		case epollSessionActive:
			snap.active++
		case epollSessionTerminal:
			snap.terminal++
		case epollSessionClosed:
			snap.closed++
		}
		switch s.callbackState {
		case epollCallbackNeedOpen:
			snap.callbackNeedOpen++
		case epollCallbackOpenInFlight:
			snap.callbackOpen++
		case epollCallbackIdle:
			snap.callbackIdle++
		case epollCallbackMessageInFlight:
			snap.callbackMessage++
		case epollCallbackNeedClose:
			snap.callbackNeedClose++
		case epollCallbackCloseInFlight:
			snap.callbackClose++
		case epollCallbackClosed:
			snap.callbackClosed++
		}
		if s.writeActive {
			snap.writeActive++
		}
		if s.writeBlocked {
			snap.writeBlocked++
		}
		if s.readReady {
			snap.readReady++
		}
		if s.terminalPrepared {
			snap.terminalPrepared++
		}
	}
	return snap
}

func snapshotEpollStressRuntime(e *epollEngine) string {
	if e == nil {
		return "<nil engine>"
	}
	ch := make(chan epollStressReactorSnapshot, len(e.reactors))
	for _, reactor := range e.reactors {
		r := reactor
		r.signal(newTestInboxItem(func(owner *epollReactor) {
			ch <- captureEpollStressReactor(owner)
		}))
	}

	var b strings.Builder
	fmt.Fprintf(&b, "engine=%+v callbackReserved=%d callbackQueued=%d", e.Stats(), e.callbacks.reservedCount(), e.callbacks.queuedCount())
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for i := 0; i < len(e.reactors); i++ {
		select {
		case snap := <-ch:
			fmt.Fprintf(&b, " reactor[%d]={res:%d sess:%d handoff:%d connect:%d codec:%d opening:%d active:%d terminal:%d closed:%d cbNeedOpen:%d cbOpen:%d cbIdle:%d cbMsg:%d cbNeedClose:%d cbClose:%d cbClosed:%d writeActive:%d writeBlocked:%d readReady:%d prepared:%d workerBlocked:%d}",
				snap.index, snap.resources, snap.sessions, snap.handoff, snap.connecting, snap.codecSetup, snap.opening, snap.active,
				snap.terminal, snap.closed, snap.callbackNeedOpen, snap.callbackOpen, snap.callbackIdle, snap.callbackMessage,
				snap.callbackNeedClose, snap.callbackClose, snap.callbackClosed, snap.writeActive, snap.writeBlocked, snap.readReady,
				snap.terminalPrepared, snap.workerBlocked)
		case <-deadline.C:
			fmt.Fprintf(&b, " reactorSnapshots=%d/%d", i, len(e.reactors))
			return b.String()
		}
	}
	return b.String()
}

func finishEpollStressServerSession(ctx context.Context, s ogrenet.Session, serverInitiates bool, errs chan<- error, done chan<- struct{}, progress *epollStressProgress) {
	defer func() {
		progress.serverDone.Add(1)
		done <- struct{}{}
	}()
	if !serverInitiates {
		half, ok := s.(ogrenet.HalfCloseSession)
		if !ok {
			errs <- fmt.Errorf("server session %d does not implement HalfCloseSession", s.ID())
			_ = s.Close()
		} else {
			select {
			case <-half.ReadClosed():
				progress.serverReadClosed.Add(1)
				progress.serverClose.Add(1)
				_ = s.Close()
			case <-ctx.Done():
				errs <- fmt.Errorf("server session %d waiting client close: %w", s.ID(), context.Cause(ctx))
				progress.serverClose.Add(1)
				_ = s.Close()
			}
		}
	} else {
		progress.serverClose.Add(1)
		_ = s.Close()
	}

	select {
	case <-s.Done():
	case <-ctx.Done():
		errs <- fmt.Errorf("server session %d Done: %w", s.ID(), context.Cause(ctx))
	}
}

func runEpollShortLivedClient(ctx context.Context, e *epollEngine, endpoint ogrenet.Endpoint, i int, progress *epollStressProgress) error {
	h := &epollStressClientHandler{
		opened: make(chan struct{}),
		echoed: make(chan ogrenet.Message, 1),
	}
	progress.dialStarted.Add(1)
	session, err := e.Dial(ctx, endpoint, h)
	if err != nil {
		return fmt.Errorf("dial %d: %w", i, err)
	}
	progress.dialDone.Add(1)
	select {
	case <-h.opened:
		progress.openDone.Add(1)
	case <-ctx.Done():
		return fmt.Errorf("open %d: %w", i, context.Cause(ctx))
	}

	payload := []byte(fmt.Sprintf("%d-%06d", i&1, i))
	if err := session.Send(ctx, ogrenet.Text(string(payload))); err != nil {
		return fmt.Errorf("send %d: %w", i, err)
	}
	progress.clientSendDone.Add(1)
	var echoed ogrenet.Message
	select {
	case echoed = <-h.echoed:
		progress.clientEchoDone.Add(1)
	case <-ctx.Done():
		return fmt.Errorf("echo %d: %w", i, context.Cause(ctx))
	}
	if !bytes.Equal(echoed.Data, payload) {
		return fmt.Errorf("echo %d payload=%q want=%q", i, echoed.Data, payload)
	}

	if i&1 == 0 {
		progress.clientClose.Add(1)
		if err := session.Close(); err != nil {
			return fmt.Errorf("client close %d: %w", i, err)
		}
		select {
		case <-session.Done():
			progress.clientDone.Add(1)
			return nil
		case <-ctx.Done():
			return fmt.Errorf("client Done %d: %w", i, context.Cause(ctx))
		}
	}

	half, ok := session.(ogrenet.HalfCloseSession)
	if !ok {
		return fmt.Errorf("session %d does not implement HalfCloseSession", i)
	}
	select {
	case <-half.ReadClosed():
		progress.clientReadClosed.Add(1)
	case <-ctx.Done():
		return fmt.Errorf("server-close ReadClosed %d: %w", i, context.Cause(ctx))
	}
	progress.clientClose.Add(1)
	if err := session.Close(); err != nil {
		return fmt.Errorf("client reciprocal close %d: %w", i, err)
	}
	select {
	case <-session.Done():
		progress.clientDone.Add(1)
		return nil
	case <-ctx.Done():
		return fmt.Errorf("client reciprocal Done %d: %w", i, context.Cause(ctx))
	}
}

func TestEpollNativeShortLivedConnections(t *testing.T) {
	const (
		connections = 2000
		concurrency = 128
	)

	raw, err := NewEpoll(EpollConfig{
		Pollers:         4,
		CallbackWorkers: 64,
		CallbackQueue:   4096,
		EventBatch:      256,
		IOBudgetBytes:   256 << 10,
		IOBudgetOps:     64,
	})
	if err != nil {
		t.Fatal(err)
	}
	e := raw.(*epollEngine)
	t.Cleanup(func() {
		_ = e.Close()
		waitEpollEngineDone(t, e.Done())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	progress := &epollStressProgress{}
	serverErr := make(chan error, connections*2)
	serverDone := make(chan struct{}, connections)
	ln, err := e.Listen(ctx, ogrenet.Endpoint{
		Scheme: ogrenet.SchemeTCP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.HandlerFuncs{
		Message: func(s ogrenet.Session, msg ogrenet.Message) {
			progress.serverMessage.Add(1)
			if err := s.Send(context.Background(), msg); err != nil {
				serverErr <- err
				progress.serverClose.Add(1)
				_ = s.Close()
				go finishEpollStressServerSession(ctx, s, true, serverErr, serverDone, progress)
				return
			}
			progress.serverSendDone.Add(1)
			serverInitiates := len(msg.Data) != 0 && msg.Data[0] == '1'
			go finishEpollStressServerSession(ctx, s, serverInitiates, serverErr, serverDone, progress)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	jobs := make(chan int)
	errCh := make(chan error, connections)
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for worker := 0; worker < concurrency; worker++ {
		go func() {
			defer wg.Done()
			for i := range jobs {
				if err := runEpollShortLivedClient(ctx, e, ln.Endpoint(), i, progress); err != nil {
					errCh <- err
				}
			}
		}()
	}
	for i := 0; i < connections; i++ {
		select {
		case jobs <- i:
		case <-ctx.Done():
			diagnostic := snapshotEpollStressRuntime(e)
			close(jobs)
			wg.Wait()
			t.Fatalf("dispatching short-lived connection %d: %v; progress={%s}; runtime={%s}", i, context.Cause(ctx), progress.String(), diagnostic)
		}
	}
	close(jobs)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	for completed := 0; completed < connections; completed++ {
		select {
		case <-serverDone:
		case <-ctx.Done():
			t.Fatalf("waiting for server Session %d/%d Done: %v; progress={%s}; runtime={%s}", completed, connections, context.Cause(ctx), progress.String(), snapshotEpollStressRuntime(e))
		}
	}
	select {
	case err := <-serverErr:
		t.Fatalf("server stress error: %v", err)
	default:
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.Done():
	case <-ctx.Done():
		t.Fatalf("Engine.Done after %d short-lived connections: %v; progress={%s}; runtime={%s}", connections, context.Cause(ctx), progress.String(), snapshotEpollStressRuntime(e))
	}
	assertEpollEngineZeroInvariants(t, e)
}
