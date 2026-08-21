package transport_test

import (
	"context"
	"testing"

	"github.com/qigao/ogrenet"
)

func runTCPContract(t *testing.T, f engineFactory) {
	t.Helper()
	ctx, cancel := contractContext(t)
	defer cancel()

	e := f.new(t)
	accepted := make(chan ogrenet.Session, 1)
	serverSendErr := make(chan error, 1)
	clientEvents := make(chan string, 4)
	clientMessages := make(chan ogrenet.Message, 1)

	ln, err := e.Listen(ctx, ogrenet.Endpoint{
		Scheme: ogrenet.SchemeTCP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.HandlerFuncs{
		Open: func(s ogrenet.Session) {
			accepted <- s
		},
		Message: func(s ogrenet.Session, m ogrenet.Message) {
			serverSendErr <- s.Send(context.Background(), m)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	client, err := e.Dial(ctx, ln.Endpoint(), ogrenet.HandlerFuncs{
		Open: func(ogrenet.Session) {
			clientEvents <- "open"
		},
		Message: func(_ ogrenet.Session, m ogrenet.Message) {
			clientEvents <- "message"
			clientMessages <- m
		},
		Close: func(_ ogrenet.Session, _ error) {
			clientEvents <- "close"
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	peer := recvContract(t, ctx, accepted, "accepted tcp session")

	if event := recvContract(t, ctx, clientEvents, "client OnOpen"); event != "open" {
		t.Fatalf("first client event=%q", event)
	}

	payload := []byte("contract-ping")
	if err := client.Send(ctx, ogrenet.Text(string(payload))); err != nil {
		t.Fatal(err)
	}
	if err := recvContract(t, ctx, serverSendErr, "server echo write"); err != nil {
		t.Fatal(err)
	}
	message := recvContract(t, ctx, clientMessages, "client echo message")
	if string(message.Data) != string(payload) {
		t.Fatalf("payload=%q", message.Data)
	}
	if event := recvContract(t, ctx, clientEvents, "client OnMessage"); event != "message" {
		t.Fatalf("second client event=%q", event)
	}

	clientStats := client.Stats()
	if clientStats.BytesTX != uint64(len(payload)) || clientStats.MessagesTX != 1 {
		t.Fatalf("client tx stats=%+v", clientStats)
	}
	if clientStats.BytesRX != uint64(len(payload)) || clientStats.MessagesRX != 1 {
		t.Fatalf("client rx stats=%+v", clientStats)
	}
	peerStats := peer.Stats()
	if peerStats.BytesRX != uint64(len(payload)) || peerStats.MessagesRX != 1 {
		t.Fatalf("server rx stats=%+v", peerStats)
	}
	if peerStats.BytesTX != uint64(len(payload)) || peerStats.MessagesTX != 1 {
		t.Fatalf("server tx stats=%+v", peerStats)
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	waitContractDone(t, ctx, client.Done(), "client tcp close")
	waitContractDone(t, ctx, peer.Done(), "server tcp close")
	if event := recvContract(t, ctx, clientEvents, "client OnClose"); event != "close" {
		t.Fatalf("third client event=%q", event)
	}
	if err := client.Err(); err != nil {
		t.Fatalf("client terminal err=%v", err)
	}
	if err := peer.Err(); err != nil {
		t.Fatalf("server terminal err=%v", err)
	}
}
