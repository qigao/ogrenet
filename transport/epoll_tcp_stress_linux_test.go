//go:build linux

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

func runEpollShortLivedClient(ctx context.Context, e *epollEngine, endpoint ogrenet.Endpoint, i int) error {
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
		return nil
	case <-ctx.Done():
		return fmt.Errorf("server-close ReadClosed %d: %w", i, context.Cause(ctx))
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
	serverErr := make(chan error, connections)
	ln, err := e.Listen(ctx, ogrenet.Endpoint{
		Scheme: ogrenet.SchemeTCP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.HandlerFuncs{
		Message: func(s ogrenet.Session, msg ogrenet.Message) {
			if err := s.Send(context.Background(), msg); err != nil {
				serverErr <- err
				return
			}
			if len(msg.Data) != 0 && msg.Data[0] == '1' {
				_ = s.Close()
			}
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
		select {
		case jobs <- i:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			t.Fatalf("dispatching short-lived connection %d: %v", i, context.Cause(ctx))
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
	select {
	case err := <-serverErr:
		t.Fatalf("server echo: %v", err)
	default:
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.Done():
	case <-ctx.Done():
		t.Fatalf("Engine.Done after %d short-lived connections: %v", connections, context.Cause(ctx))
	}
	assertEpollEngineZeroInvariants(t, e)
}
