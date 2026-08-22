//go:build linux && !race

package transport

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

const epollStressProgressTimeout = 10 * time.Second

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

func finishEpollStressServerSession(ctx context.Context, s ogrenet.Session, serverInitiates bool, errs chan<- error, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()
	if !serverInitiates {
		half, ok := s.(ogrenet.HalfCloseSession)
		if !ok {
			errs <- fmt.Errorf("server session %d does not implement HalfCloseSession", s.ID())
			_ = s.Close()
		} else {
			select {
			case <-half.ReadClosed():
				_ = s.Close()
			case <-ctx.Done():
				errs <- fmt.Errorf("server session %d waiting client close: %w", s.ID(), context.Cause(ctx))
				_ = s.Close()
			}
		}
	} else {
		_ = s.Close()
	}

	select {
	case <-s.Done():
	case <-ctx.Done():
		errs <- fmt.Errorf("server session %d Done: %w", s.ID(), context.Cause(ctx))
	}
}

func runEpollShortLivedClient(parent context.Context, e *epollEngine, endpoint ogrenet.Endpoint, i int) error {
	ctx, cancel := context.WithTimeout(parent, epollStressProgressTimeout)
	defer cancel()

	h := &epollStressClientHandler{
		opened: make(chan struct{}),
		echoed: make(chan ogrenet.Message, 1),
	}
	session, err := e.Dial(ctx, endpoint, h)
	if err != nil {
		return fmt.Errorf("dial %d: %w", i, err)
	}
	select {
	case <-h.opened:
	case <-ctx.Done():
		return fmt.Errorf("open %d: %w", i, context.Cause(ctx))
	}

	payload := []byte(fmt.Sprintf("%d-%06d", i&1, i))
	if err := session.Send(ctx, ogrenet.Text(string(payload))); err != nil {
		return fmt.Errorf("send %d: %w", i, err)
	}
	var echoed ogrenet.Message
	select {
	case echoed = <-h.echoed:
	case <-ctx.Done():
		return fmt.Errorf("echo %d: %w", i, context.Cause(ctx))
	}
	if !bytes.Equal(echoed.Data, payload) {
		return fmt.Errorf("echo %d payload=%q want=%q", i, echoed.Data, payload)
	}

	if i&1 == 0 {
		if err := session.Close(); err != nil {
			return fmt.Errorf("client close %d: %w", i, err)
		}
		select {
		case <-session.Done():
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
	case <-ctx.Done():
		return fmt.Errorf("server-close ReadClosed %d: %w", i, context.Cause(ctx))
	}
	if err := session.Close(); err != nil {
		return fmt.Errorf("client reciprocal close %d: %w", i, err)
	}
	select {
	case <-session.Done():
		return nil
	case <-ctx.Done():
		return fmt.Errorf("client reciprocal Done %d: %w", i, context.Cause(ctx))
	}
}

func TestEpollNativeShortLivedConnections(t *testing.T) {
	const (
		// Each loopback TCP connection creates two native Sessions owned by the
		// same Engine: one dialing Session and one accepted Session. Therefore
		// 1000 connections exercise the approved 2000-Session stress target.
		connections = 1000
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverErr := make(chan error, connections*2)
	serverDone := make(chan struct{}, connections)
	ln, err := e.Listen(ctx, ogrenet.Endpoint{
		Scheme: ogrenet.SchemeTCP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.HandlerFuncs{
		Message: func(s ogrenet.Session, msg ogrenet.Message) {
			sendCtx, sendCancel := context.WithTimeout(ctx, epollStressProgressTimeout)
			err := s.Send(sendCtx, msg)
			sendCancel()
			if err != nil {
				serverErr <- err
				_ = s.Close()
				go finishEpollStressServerSession(ctx, s, true, serverErr, serverDone)
				return
			}
			serverInitiates := len(msg.Data) != 0 && msg.Data[0] == '1'
			go finishEpollStressServerSession(ctx, s, serverInitiates, serverErr, serverDone)
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
				if err := runEpollShortLivedClient(ctx, e, ln.Endpoint(), i); err != nil {
					errCh <- err
				}
			}
		}()
	}
	for i := 0; i < connections; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	serverDeadline := time.NewTimer(epollStressProgressTimeout)
	defer serverDeadline.Stop()
	for completed := 0; completed < connections; completed++ {
		select {
		case <-serverDone:
		case <-serverDeadline.C:
			t.Fatalf("waiting for server Session %d/%d Done", completed, connections)
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
	engineDeadline := time.NewTimer(epollStressProgressTimeout)
	defer engineDeadline.Stop()
	select {
	case <-e.Done():
	case <-engineDeadline.C:
		t.Fatalf("Engine.Done after %d loopback connections / %d Sessions", connections, 2*connections)
	}
	assertEpollEngineZeroInvariants(t, e)
}
