package client

import (
	"io"
	"net/http"
	"testing"
)

func BenchmarkHTTP3MultiplexedRequests(b *testing.B) {
	url, tlsCfg := startHTTP3Server(b, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}), nil)
	tr, err := NewHTTP3Transport(HTTP3Config{TLSConfig: tlsCfg})
	if err != nil {
		b.Fatal(err)
	}
	defer tr.Close()
	client := &http.Client{Transport: tr}
	errs := make(chan error, 1)
	report := func(err error) {
		select {
		case errs <- err:
		default:
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(url)
			if err != nil {
				report(err)
				return
			}
			_, copyErr := io.Copy(io.Discard, resp.Body)
			closeErr := resp.Body.Close()
			if copyErr != nil {
				report(copyErr)
				return
			}
			if closeErr != nil {
				report(closeErr)
				return
			}
		}
	})
	b.StopTimer()
	select {
	case err := <-errs:
		b.Fatal(err)
	default:
	}
}
