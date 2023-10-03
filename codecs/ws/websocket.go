package ws

import (
	"net/http"
	"strings"

	"github.com/qigao/ogrenet/errors"
	"github.com/qigao/ogrenet/network"
)

type Upgrader struct {
	// Error specifies the function for generating HTTP error responses. If Error
	// is nil, then http.Error is used to generate the HTTP response.
	Error func(status int, reason error)

	// CheckOrigin returns true if the request Origin header is acceptable. If
	// CheckOrigin is nil, then a safe default is used: return false if the
	// Origin request header is present and the origin host is not equal to
	// request Host header.
	//
	// A CheckOrigin function should carefully validate the request origin to
	// prevent cross-site request forgery.
	CheckOrigin func(header http.Header) bool

	// Subprotocols specifies the server's supported protocols in order of
	// preference. If this field is not nil, then the Upgrade method negotiates a
	// subprotocol by selecting the first match in this list with a protocol
	// requested by the client. If there's no match, then no protocol is
	// negotiated (the Sec-Websocket-Protocol header is not included in the
	// handshake response).
	Subprotocols []string

	// EnableCompression specify if the server should attempt to negotiate per
	// message compression (RFC 7692). Setting this value to true does not
	// guarantee that compression will be supported. Currently only "no context
	// takeover" modes are supported.
	EnableCompression bool
}

func (u *Upgrader) returnError(status int, reason string) (*network.Conn, error) {
	err := errors.HandshakeError{Message: reason}
	return nil, err
}

func (u *Upgrader) Upgrade(fd int, header map[string]string, s *network.OgreNet) (*network.Conn, error) {
	const badHandshake = "websocket: the client is not using the websocket protocol: "
	if Connection, ok := header["Connection"]; ok {
		cnnt := false
		arr := strings.Split(Connection, ",")
		for _, v := range arr {
			if strings.Trim(v, " ") == "Upgrade" {
				cnnt = true
				break
			}
		}
		if cnnt == false {
			return u.returnError(http.StatusBadRequest, badHandshake+"'upgrade' token not found in 'Connection' header")
		}
	}

	if header["Upgrade"] != "websocket" {
		return u.returnError(http.StatusBadRequest, badHandshake+"'websocket' token not found in 'Upgrade' header")
	}

	if header["Method"] != "GET" {
		return u.returnError(http.StatusMethodNotAllowed, badHandshake+"request method is not GET")
	}
	if header["Sec-EventHandle-Version"] != "13" {
		return u.returnError(http.StatusBadRequest, "websocket: unsupported version: 13 not found in 'Sec-Websocket-Version' header")
	}

	//if _, ok := header["Sec-EventHandle-Extensions"]; ok {
	//	return u.returnError(http.StatusInternalServerError, "websocket: application specific 'Sec-EventHandle-Extensions' headers are unsupported")
	//}

	challengeKey := header["Sec-EventHandle-Key"]
	if challengeKey == "" {
		return u.returnError(http.StatusBadRequest, "websocket: not a websocket handshake: 'Sec-EventHandle-Key' header is missing or blank")
	}
	c := network.NewConn(fd, s)
	// Use larger of hijacked buffer and connection write buffer for header.
	wf := s.BytePool.Get().([]byte)
	defer func() {
		wf = make([]byte, 0, s.WriteBufferSize)
		s.BytePool.Put(wf)
	}()
	wf = append(wf, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-EventHandle-Accept: "...)

	wf = append(wf, ComputeAcceptKey(challengeKey)...)

	wf = append(wf, "\r\n"...)
	if compress, ok := header["Accept-Encoding"]; ok {
		arr := strings.Split(compress, ",")
		for _, v := range arr {
			if strings.Trim(v, " ") == "deflate" {
				c.CanCompress = true
				wf = append(wf, "Sec-EventHandle-Extensions: permessage-deflate; server_no_context_takeover; client_no_context_takeover"...)
				// wf = append(wf, header["Sec-EventHandle-Extensions"]...)
			}
		}
	}

	wf = append(wf, "\r\n"...)
	wf = append(wf, "\r\n"...)
	c.HandShake <- Codec{
		MessageType: -1,
		Content:     wf,
	}

	return c, nil
}

func (u *Upgrader) selectSubprotocol(r *http.Request, responseHeader http.Header) string {
	if u.Subprotocols != nil {
		clientProtocols := Subprotocols(r)
		for _, serverProtocol := range u.Subprotocols {
			for _, clientProtocol := range clientProtocols {
				if clientProtocol == serverProtocol {
					return clientProtocol
				}
			}
		}
	} else if responseHeader != nil {
		return responseHeader.Get("Sec-Websocket-Protocol")
	}
	return ""
}

// Subprotocols returns the subprotocols requested by the client in the
// Sec-Websocket-Protocol header.
func Subprotocols(r *http.Request) []string {
	h := strings.TrimSpace(r.Header.Get("Sec-Websocket-Protocol"))
	if h == "" {
		return nil
	}
	protocols := strings.Split(h, ",")
	for i := range protocols {
		protocols[i] = strings.TrimSpace(protocols[i])
	}
	return protocols
}
