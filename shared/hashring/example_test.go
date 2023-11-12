package hashring

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExampleHashRing(t *testing.T) {
	c := NewHashRing()

	// adds the hosts to the ring
	c.Add(host01)
	c.Add(host02)

	// Returns the host that owns `key`.
	//
	// As described in https://en.wikipedia.org/wiki/Consistent_hashing
	//
	// It returns ErrNoHosts if the ring has no hosts in it.
	host, err := c.Get("/test/app.html")
	if err != nil {
		log.Fatal(err)
	}
	t.Log(host)
	assert.Equal(t, host02, host)
}

func TestExampleBounded(t *testing.T) {
	c := NewHashRing()

	// adds the hosts to the ring
	c.Add(host01)
	c.Add(host02)

	// It uses Consistent Hashing With Bounded loads
	// https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
	// to pick the least loaded host that can serve the key
	//
	// It returns ErrNoHosts if the ring has no hosts in it.
	//
	host, err := c.GetLeast("/test/app.html")
	if err != nil {
		log.Fatal(err)
	}
	// increases the load of `host`, we have to call it before sending the request
	c.Inc(host)
	// send request or do whatever
	log.Println("send request to", host)
	// call it when the work is done, to update the load of `host`.
	defer c.Done(host)
	assert.Equal(t, host02, host)
}
