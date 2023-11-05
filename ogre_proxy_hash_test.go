package ogrenet

import (
	"testing"

	"github.com/buraksezer/consistent"
	"github.com/stretchr/testify/assert"
)

func TestHashRing(t *testing.T) {
	t.Run("test add", func(t *testing.T) {
		// Create a new consistent instance
		cfg := consistent.Config{
			PartitionCount:    7,
			ReplicationFactor: 20,
			Load:              1.25,
			Hasher:            hasher{},
		}
		c := consistent.New(nil, cfg)

		// Add some members to the consistent hash table.
		// Add function calculates average load and distributes partitions over members
		msgPool := NewMessagePool()
		messageChan := make(chan *MsgConn, 1024)
		conn := NewNetConn(1, msgPool, messageChan)
		node1 := ConnMember{
			conn: conn,
		}
		c.Add(node1)

		node2 := ConnMember{
			conn: conn,
		}
		c.Add(node2)

		key := []byte("my-key")
		// calculates partition id for the given key
		// partID := hash(key) % partitionCount
		// the partitions are already distributed among members by Add function.
		owner := c.LocateKey(key)
		assert.Equal(t, node2.String(), owner.String())
	})
}
