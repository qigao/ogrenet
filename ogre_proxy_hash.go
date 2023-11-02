package ogrenet

import (
	"fmt"

	"github.com/cespare/xxhash"
)

// In your distributed system, you probably have a custom data type
// for your cluster members. Just add a String function to implement
// consistent.Member interface.
type ConnMember struct {
	conn *Conn
}

func (m ConnMember) String() string {
	connID := fmt.Sprintf("%d", m.conn.fd)
	return connID
}

// consistent package doesn't provide a default hashing function.
// You should provide a proper one to distribute keys/members uniformly.
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
	return xxhash.Sum64(data)
}
