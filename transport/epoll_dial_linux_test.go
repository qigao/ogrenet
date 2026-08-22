//go:build linux

package transport

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/qigao/ogrenet"
)

func TestResolveNativeDialLiteralSkipsResolver(t *testing.T) {
	resolver := &testNativeIPResolver{err: errors.New("resolver should not run")}
	endpoint := ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 43210}

	got, err := resolveNativeDialTCP(context.Background(), endpoint, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.called != 0 {
		t.Fatalf("resolver called %d times for literal address", resolver.called)
	}
	if len(got) != 1 || got[0].Port != int(endpoint.Port) || !got[0].IP.Equal(net.ParseIP(endpoint.Host)) {
		t.Fatalf("resolved=%v", got)
	}
}

func TestResolveNativeDialPreservesResolverOrderAndPort(t *testing.T) {
	resolver := &testNativeIPResolver{addrs: []net.IPAddr{
		{IP: net.ParseIP("127.0.0.2")},
		{IP: net.ParseIP("127.0.0.1")},
	}}
	endpoint := ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "example.test", Port: 43211}

	got, err := resolveNativeDialTCP(context.Background(), endpoint, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.called != 1 {
		t.Fatalf("resolver called %d times, want 1", resolver.called)
	}
	if len(got) != 2 {
		t.Fatalf("resolved count=%d, want 2", len(got))
	}
	for i, wantIP := range []net.IP{net.ParseIP("127.0.0.2"), net.ParseIP("127.0.0.1")} {
		if got[i].Port != int(endpoint.Port) || !got[i].IP.Equal(wantIP) {
			t.Fatalf("resolved[%d]=%v, want %v:%d", i, got[i], wantIP, endpoint.Port)
		}
	}
}

func TestEpollNativeDialResolverErrorIsTypedAndCallerCauseWins(t *testing.T) {
	resolverErr := &net.DNSError{Err: "synthetic resolver failure", Name: "example.test"}
	resolver := &testNativeIPResolver{err: resolverErr}
	e := newEpollTestEngine(t, 1)
	endpoint := ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "example.test", Port: 43212}

	_, err := e.dialNativeTCP(context.Background(), endpoint, nil, resolver)
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("resolver error type=%T err=%v", err, err)
	}
	if typed.Op != OpDial || typed.Protocol != ogrenet.SchemeTCP || typed.Kind != ErrorDNS {
		t.Fatalf("resolver error=%+v", typed)
	}
	if !errors.Is(err, resolverErr) {
		t.Fatalf("resolver cause lost: %v", err)
	}

	callerCause := errors.New("caller canceled native dial")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(callerCause)
	resolver.called = 0
	_, err = e.dialNativeTCP(ctx, endpoint, nil, resolver)
	if err != callerCause {
		t.Fatalf("caller cause=%v, want exact %v", err, callerCause)
	}
	if resolver.called != 0 {
		t.Fatalf("resolver called %d times after caller cancellation", resolver.called)
	}
}
