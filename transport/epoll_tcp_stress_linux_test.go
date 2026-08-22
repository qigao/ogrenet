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

func TestEpollNativeShortLivedConnections(t *testing.T) {
	const connections = 2000

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

	errCh := make(chan error, connections)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < connections; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			h := &epollStressClientHandler{
				opened: make(chan struct{}),
				echoed: make(chan ogrenet.Message, 1),
			}
			session, err := e.Dial(ctx, ln.Endpoint(), h)
			if err != nil {
				errCh <- fmt.Errorf("dial %d: %w", i, err)
				return
			}
			select {
			case <-h.opened:
			case <-ctx.Done():
				errCh <- fmt.Errorf("open %d: %w", i, context.Cause(ctx))
				return
			}

			payload := []byte(fmt.Sprintf("%d-%06d", i&1, i))
			if err := session.Send(ctx, ogrenet.Text(string(payload))); err != nil {
				errCh <- fmt.Errorf("send %d: %w", i, err)
				return
			}
			var echoed ogrenet.Message
			select {
			case echoed = <-h.echoed:
			case <-ctx.Done():
				errCh <- fmt.Errorf("echo %d: %w", i, context.Cause(ctx))
				return
			}
			if !bytes.Equal(echoed.Data, payload) {
				errCh <- fmt.Errorf("echo %d payload=%q want=%q", i, echoed.Data, payload)
				return
			}

			if i&1 == 0 {
				if err := session.Close(); err != nil {
					errCh <- fmt.Errorf("client close %d: %w", i, err)
					return
				}
				select {
				case <-session.Done():
				case <-ctx.Done():
					errCh <- fmt.Errorf("client Done %d: %w", i, context.Cause(ctx))
				}
				return
			}

			half, ok := session.(ogrenet.HalfCloseSession)
			if !ok {
				errCh <- fmt.Errorf("session %d does not implement HalfCloseSession", i)
				return
			}
			select {
			case <-half.ReadClosed():
			case <-ctx.Done():
				errCh <- fmt.Errorf("server-close ReadClosed %d: %w", i, context.Cause(ctx))
			}
		}()
	}
	close(start)
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
