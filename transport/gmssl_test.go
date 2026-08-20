//go:build gmssl && cgo

package transport

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	gmsslsecure "github.com/qigao/ogrenet/secure/gmssl"
)

func TestGmSSLEncryptedTextAndBinaryEcho(t *testing.T) {
	key := []byte("0123456789abcdef")
	serverCipher, err := gmsslsecure.NewSM4GCM(key)
	if err != nil {
		t.Fatal(err)
	}
	clientCipher, err := gmsslsecure.NewSM4GCM(key)
	if err != nil {
		t.Fatal(err)
	}

	server, err := New(WithCipher(serverCipher))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	listener, err := server.Listen(ctx, "tcp", "127.0.0.1:0", ogrenet.HandlerFuncs{
		Message: func(c ogrenet.Conn, msg ogrenet.Message) {
			if err := c.Send(context.Background(), msg); err != nil && !errors.Is(err, ErrClosed) {
				t.Errorf("server echo: %v", err)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	client, err := New(WithCipher(clientCipher))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	received := make(chan ogrenet.Message, 4)
	conn, err := client.Dial(ctx, "tcp", listener.Addr().String(), ogrenet.HandlerFuncs{
		Message: func(_ ogrenet.Conn, msg ogrenet.Message) {
			received <- ogrenet.Message{Type: msg.Type, Data: append([]byte(nil), msg.Data...)}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []ogrenet.Message{
		ogrenet.Text("国密 text payload"),
		ogrenet.Bin([]byte{0x00, 0x01, 0xff, 0x00}),
		ogrenet.Text(""),
		ogrenet.Bin(nil),
	}
	for _, msg := range want {
		if err := conn.Send(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	for _, expected := range want {
		select {
		case got := <-received:
			if got.Type != expected.Type || !bytes.Equal(got.Data, expected.Data) {
				t.Fatalf("got %+v, want %+v", got, expected)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
