package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/qigao/ogrenet/network"
	"github.com/rs/zerolog/log"
)

var _ network.EventHandler = (*Handler)(nil)

type Handler struct{}

type EasyioKey struct{}

type Message struct{ Msg string }

var CtxKey EasyioKey

func (h Handler) OnOpen(c network.Conn) context.Context {
	return context.WithValue(context.Background(), CtxKey, Message{Msg: "helloword"})
}

func (h Handler) OnRead(ctx context.Context, c network.Conn) {
	_, ok := ctx.Value(CtxKey).(Message)
	if !ok {
		return
	}
	b := make([]byte, 10)
	n, err := c.Read(b)
	if err != nil {
		log.Error().Err(err).Msgf("err: %v", err)
	}
	log.Info().Msgf("[Handler] read data: %s", b[:n])

	if _, err = c.Write(b); err != nil {
		log.Error().Err(err).Msgf("err: %v", err)
	}
}

func (h Handler) OnClose(_ context.Context, c network.Conn) {
	log.Info().Msgf("[Handler] closed %d", c.Fd())
}

func main() {
	e := network.NewServer("tcp", ":8090",
		network.WithNumPoller(5), network.WithEventHandler(Handler{}))

	if err := e.Start(); err != nil {
		panic(err)
	}

	defer e.Stop()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
	<-c
}
