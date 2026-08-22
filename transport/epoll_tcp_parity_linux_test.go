//go:build linux

package transport_test

import "testing"

func TestEpollTCPGracefulContracts(t *testing.T) {
	runTCPGracefulContracts(t, epollFactory(contractProfile{TCP: true}))
}

func TestEpollTCPLimitStatsContracts(t *testing.T) {
	runTCPLimitStatsContracts(t, epollFactory(contractProfile{TCP: true}))
}
