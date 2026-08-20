package ogrenet

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// Scheme identifies the transport protocol selected by an Endpoint.
type Scheme uint8

const (
	SchemeTCP Scheme = iota + 1
	SchemeUDP
	SchemeTLS
	SchemeWS
	SchemeWSS
)

var (
	ErrInvalidEndpoint  = errors.New("ogrenet: invalid endpoint")
	ErrUnsupportedScheme = errors.New("ogrenet: unsupported endpoint scheme")
	ErrMissingHost      = errors.New("ogrenet: dial endpoint requires a host")
	ErrMissingPort      = errors.New("ogrenet: endpoint requires a port")
	ErrUnexpectedPath   = errors.New("ogrenet: endpoint scheme does not support a path")
	ErrUnexpectedQuery  = errors.New("ogrenet: endpoint scheme does not support a query")
)

func (s Scheme) String() string {
	switch s {
	case SchemeTCP:
		return "tcp"
	case SchemeUDP:
		return "udp"
	case SchemeTLS:
		return "tls"
	case SchemeWS:
		return "ws"
	case SchemeWSS:
		return "wss"
	default:
		return "unknown"
	}
}

// IsSession reports whether the scheme has connection/session lifecycle.
func (s Scheme) IsSession() bool {
	switch s {
	case SchemeTCP, SchemeTLS, SchemeWS, SchemeWSS:
		return true
	default:
		return false
	}
}

// IsPacket reports whether the scheme uses datagram semantics.
func (s Scheme) IsPacket() bool { return s == SchemeUDP }

// Endpoint is a parsed, strongly typed transport endpoint.
type Endpoint struct {
	Scheme   Scheme
	Host     string
	Port     uint16
	Path     string
	RawQuery string
}

// ParseEndpoint parses a transport endpoint such as tcp://127.0.0.1:9000,
// tls://example.com:443, ws://example.com/chat, or udp://:9000.
func ParseEndpoint(raw string) (Endpoint, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return Endpoint{}, fmt.Errorf("%w: %v", ErrInvalidEndpoint, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return Endpoint{}, ErrInvalidEndpoint
	}
	if u.User != nil || u.Fragment != "" {
		return Endpoint{}, ErrInvalidEndpoint
	}

	scheme, err := parseScheme(u.Scheme)
	if err != nil {
		return Endpoint{}, err
	}

	portText := u.Port()
	port, err := endpointPort(scheme, portText)
	if err != nil {
		return Endpoint{}, err
	}

	e := Endpoint{
		Scheme:   scheme,
		Host:     u.Hostname(),
		Port:     port,
		Path:     u.Path,
		RawQuery: u.RawQuery,
	}
	if err := e.Validate(); err != nil {
		return Endpoint{}, err
	}
	return e, nil
}

func parseScheme(raw string) (Scheme, error) {
	switch strings.ToLower(raw) {
	case "tcp":
		return SchemeTCP, nil
	case "udp":
		return SchemeUDP, nil
	case "tls":
		return SchemeTLS, nil
	case "ws":
		return SchemeWS, nil
	case "wss":
		return SchemeWSS, nil
	default:
		return 0, fmt.Errorf("%w: %s", ErrUnsupportedScheme, raw)
	}
}

func endpointPort(scheme Scheme, raw string) (uint16, error) {
	if raw == "" {
		switch scheme {
		case SchemeTLS, SchemeWSS:
			return 443, nil
		case SchemeWS:
			return 80, nil
		default:
			return 0, ErrMissingPort
		}
	}
	v, err := strconv.ParseUint(raw, 10, 16)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid port %q", ErrInvalidEndpoint, raw)
	}
	return uint16(v), nil
}

// Validate checks scheme-specific endpoint syntax. Host emptiness is allowed so
// the same Endpoint can represent a wildcard listen address; ValidateDial adds
// the host requirement for outbound connections.
func (e Endpoint) Validate() error {
	switch e.Scheme {
	case SchemeTCP, SchemeUDP, SchemeTLS:
		if e.Path != "" && e.Path != "/" {
			return ErrUnexpectedPath
		}
		if e.RawQuery != "" {
			return ErrUnexpectedQuery
		}
	case SchemeWS, SchemeWSS:
		if e.Path == "" {
			e.Path = "/"
		}
	default:
		return ErrUnsupportedScheme
	}
	return nil
}

// ValidateDial validates an endpoint for an outbound connection.
func (e Endpoint) ValidateDial() error {
	if err := e.Validate(); err != nil {
		return err
	}
	if e.Host == "" {
		return ErrMissingHost
	}
	return nil
}

// Address returns the host:port address used by TCP/UDP/TLS sockets.
func (e Endpoint) Address() string {
	return net.JoinHostPort(e.Host, strconv.Itoa(int(e.Port)))
}

// URL returns the canonical endpoint URL.
func (e Endpoint) URL() string {
	path := e.Path
	if (e.Scheme == SchemeWS || e.Scheme == SchemeWSS) && path == "" {
		path = "/"
	}
	host := e.Address()
	if defaultPort(e.Scheme) == e.Port && e.Host != "" {
		host = e.Host
		if strings.Contains(e.Host, ":") {
			host = "[" + e.Host + "]"
		}
	}
	return (&url.URL{
		Scheme:   e.Scheme.String(),
		Host:     host,
		Path:     path,
		RawQuery: e.RawQuery,
	}).String()
}

func (e Endpoint) String() string { return e.URL() }

// WithPort returns a copy with a different port. This is useful with listeners
// bound to port zero, whose concrete Endpoint reports the assigned port.
func (e Endpoint) WithPort(port uint16) Endpoint {
	e.Port = port
	return e
}

func defaultPort(s Scheme) uint16 {
	switch s {
	case SchemeWS:
		return 80
	case SchemeTLS, SchemeWSS:
		return 443
	default:
		return 0
	}
}
