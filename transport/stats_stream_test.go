package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestStreamStatsCountApplicationPayload(t *testing.T) {
	for _, scheme := range []ogrenet.Scheme{ogrenet.SchemeTCP, ogrenet.SchemeTLS} {
		t.Run(scheme.String(), func(t *testing.T) {
			p := dialSessionPair(t, scheme)
			defer p.close()

			payload := []byte("application-payload-is-not-wire-frame")
			msg := ogrenet.Bin(payload)
			if err := p.client.Send(testContext(t), msg); err != nil {
				t.Fatalf("Send: %v", err)
			}
			select {
			case got := <-p.serverMsgs:
				if string(got.Data) != string(payload) {
					t.Fatalf("received payload=%q, want %q", got.Data, payload)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("message receive timeout")
			}

			clientStats := p.client.Stats()
			if clientStats.BytesTX != uint64(len(payload)) || clientStats.MessagesTX != 1 {
				t.Fatalf("client stats=%+v, want payload tx bytes=%d messages=1", clientStats, len(payload))
			}
			serverStats := p.server.Stats()
			if serverStats.BytesRX != uint64(len(payload)) || serverStats.MessagesRX != 1 {
				t.Fatalf("server stats=%+v, want payload rx bytes=%d messages=1", serverStats, len(payload))
			}
			if clientStats.Age <= 0 || serverStats.Age <= 0 {
				t.Fatalf("session ages must be positive: client=%v server=%v", clientStats.Age, serverStats.Age)
			}
		})
	}
}

func TestStreamTrySendBackpressureCountsOnce(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTCP)
	defer p.close()
	client, ok := p.client.(*conn)
	if !ok {
		t.Fatalf("client type=%T, want *conn", p.client)
	}

	for i := 0; i < cap(client.frameSlots); i++ {
		client.frameSlots <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(client.frameSlots); i++ {
			<-client.frameSlots
		}
	}()

	err := client.TrySend(ogrenet.Bin([]byte("blocked")))
	if !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("TrySend=%v, want ErrWouldBlock", err)
	}
	if got := client.Stats().Backpressure; got != 1 {
		t.Fatalf("backpressure=%d, want 1", got)
	}
}

func testContext(t *testing.T) interface{ Done() <-chan struct{} } {
	t.Helper()
	return nil
}
