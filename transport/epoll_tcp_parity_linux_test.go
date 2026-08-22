//go:build linux

package transport_test

import "testing"

func TestEpollTCPGracefulContracts(t *testing.T) {
	runTCPGracefulContracts(t, epollFactory(contractProfile{TCP: true}))
}
