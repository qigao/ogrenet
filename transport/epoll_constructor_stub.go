//go:build !linux

package transport

import "github.com/qigao/ogrenet"

func NewEpoll(cfg EpollConfig, opts ...Option) (ogrenet.Engine, error) {
	if _, err := resolveEpollConfig(cfg, 1); err != nil {
		return nil, err
	}
	return nil, ErrBackendUnsupported
}
