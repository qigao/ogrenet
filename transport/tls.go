package transport

import (
	"context"
	"crypto/tls"

	"github.com/qigao/ogrenet"
)

func (c config) clientTLSConfig(endpoint ogrenet.Endpoint) (*tls.Config, error) {
	var cfg *tls.Config
	if c.clientTLS == nil {
		cfg = &tls.Config{}
	} else {
		cfg = c.clientTLS.Clone()
	}
	if err := enforceTLS13(cfg); err != nil {
		return nil, err
	}
	if cfg.ServerName == "" {
		cfg.ServerName = endpoint.Host
	}
	return cfg, nil
}

func (c config) serverTLSConfig() (*tls.Config, error) {
	if c.serverTLS == nil {
		return nil, ErrTLSConfigRequired
	}
	cfg := c.serverTLS.Clone()
	if err := enforceTLS13(cfg); err != nil {
		return nil, err
	}
	if len(cfg.Certificates) == 0 && cfg.GetCertificate == nil && cfg.GetConfigForClient == nil {
		return nil, ErrTLSCertificateRequired
	}
	return cfg, nil
}

func enforceTLS13(cfg *tls.Config) error {
	if cfg.MinVersion == 0 {
		cfg.MinVersion = tls.VersionTLS13
	} else if cfg.MinVersion < tls.VersionTLS13 {
		return ErrTLSVersion
	}
	if cfg.MaxVersion != 0 && cfg.MaxVersion < tls.VersionTLS13 {
		return ErrTLSVersion
	}
	return nil
}

func (c config) handshakeClient(ctx context.Context, raw *tls.Conn) error {
	hctx, cancel := boundedOperationContext(ctx, c.effectiveTLSHandshakeTimeout())
	defer cancel()
	err := raw.HandshakeContext(hctx)
	return mapOperationTimeout(ctx, hctx, TimeoutHandshake, err)
}

func (c config) handshakeServer(ctx context.Context, raw *tls.Conn) error {
	hctx, cancel := boundedOperationContext(ctx, c.effectiveTLSHandshakeTimeout())
	defer cancel()
	err := raw.HandshakeContext(hctx)
	return mapOperationTimeout(ctx, hctx, TimeoutHandshake, err)
}
