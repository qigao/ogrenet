//go:build linux

package transport

import (
	"runtime"

	"github.com/qigao/ogrenet"
)

func NewEpoll(epcfg EpollConfig, opts ...Option) (ogrenet.Engine, error) {
	resolved, err := resolveEpollConfig(epcfg, runtime.GOMAXPROCS(0))
	if err != nil {
		return nil, err
	}
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	if err := cfg.limits.validate(); err != nil {
		return nil, err
	}
	return &epollEngine{
		cfg:      cfg,
		epollCfg: resolved,
		done:     make(chan struct{}),
	}, nil
}
