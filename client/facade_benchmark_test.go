package client

import (
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
)

type benchmarkRoundTripper struct {
	err error
}

func (r benchmarkRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	if r.err != nil {
		return nil, r.err
	}
	return &http.Response{
		StatusCode: http.StatusNoContent,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("")),
	}, nil
}

func benchmarkDialFailure() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("benchmark unavailable")}
}

func BenchmarkHTTPProtocolFacadeSingleProtocol(b *testing.B) {
	req, err := http.NewRequest(http.MethodGet, "https://example.test/", nil)
	if err != nil {
		b.Fatal(err)
	}
	direct := benchmarkRoundTripper{}
	facade := &HTTPClientTransport{
		attempts: []protocolTransport{{protocol: HTTP1, rt: direct}},
		fallback: HTTPFallbackDisabled,
	}

	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			resp, err := direct.RoundTrip(req)
			if err != nil {
				b.Fatal(err)
			}
			_ = resp.Body.Close()
		}
	})

	b.Run("facade", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			resp, err := facade.RoundTrip(req)
			if err != nil {
				b.Fatal(err)
			}
			_ = resp.Body.Close()
		}
	})
}

func BenchmarkHTTPProtocolFacadeH3ToH2Fallback(b *testing.B) {
	benchmarkHTTPProtocolFallback(b, HTTP3, HTTP2)
}

func BenchmarkHTTPProtocolFacadeH2ToH1Fallback(b *testing.B) {
	benchmarkHTTPProtocolFallback(b, HTTP2, HTTP1)
}

func benchmarkHTTPProtocolFallback(b *testing.B, first, second HTTPProtocol) {
	req, err := http.NewRequest(http.MethodGet, "https://example.test/", nil)
	if err != nil {
		b.Fatal(err)
	}
	facade := &HTTPClientTransport{
		attempts: []protocolTransport{
			{protocol: first, rt: benchmarkRoundTripper{err: benchmarkDialFailure()}},
			{protocol: second, rt: benchmarkRoundTripper{}},
		},
		fallback: HTTPFallbackSafeReplay,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		resp, err := facade.RoundTrip(req)
		if err != nil {
			b.Fatal(err)
		}
		_ = resp.Body.Close()
	}
}
