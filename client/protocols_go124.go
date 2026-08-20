//go:build go1.24

package client

import "net/http"

func applyHTTPProtocols(transport *http.Transport, protocols []HTTPProtocol) error {
	set := new(http.Protocols)
	for _, protocol := range protocols {
		switch protocol {
		case HTTP1:
			set.SetHTTP1(true)
		case HTTP2:
			set.SetHTTP2(true)
		default:
			return ErrInvalidHTTPProtocol
		}
	}
	transport.Protocols = set
	return nil
}
