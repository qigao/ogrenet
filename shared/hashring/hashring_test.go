package hashring

import (
	"fmt"
	"testing"
)

const (
	host01 = "127.0.0.1:8000"
	host02 = "92.0.0.1:8000"
)

func TestAdd(t *testing.T) {
	c := NewHashRing()

	c.Add(host01)
	if len(c.sortedSet) != replicationFactor {
		t.Fatal("vnodes number is incorrect")
	}
}

func TestGet(t *testing.T) {
	c := NewHashRing()

	c.Add(host01)
	host, err := c.Get(host01)
	if err != nil {
		t.Fatal(err)
	}

	if host != host01 {
		t.Fatal("returned host is not what expected")
	}
}

func TestRemove(t *testing.T) {
	c := NewHashRing()

	c.Add(host01)
	c.Remove(host01)

	if len(c.sortedSet) != 0 && len(c.hosts) != 0 {
		t.Fatal(("remove is not working"))
	}
}

func TestGetLeast(t *testing.T) {
	c := NewHashRing()

	c.Add(host01)
	c.Add(host02)

	for i := 0; i < 100; i++ {
		host, err := c.GetLeast("92.0.0.1:80001")
		if err != nil {
			t.Fatal(err)
		}
		c.Inc(host)
	}

	for k, v := range c.GetLoads() {
		if v > c.MaxLoad() {
			t.Fatalf("host %s is overloaded. %d > %d\n", k, v, c.MaxLoad())
		}
	}
	fmt.Println("Max load per node", c.MaxLoad())
	fmt.Println(c.GetLoads())
}

func TestIncDone(t *testing.T) {
	c := NewHashRing()

	c.Add(host01)
	c.Add(host02)

	host, err := c.GetLeast("92.0.0.1:80001")
	if err != nil {
		t.Fatal(err)
	}

	c.Inc(host)
	if c.loadMap[host].Load != 1 {
		t.Fatalf("host %s load should be 1\n", host)
	}

	c.Done(host)
	if c.loadMap[host].Load != 0 {
		t.Fatalf("host %s load should be 0\n", host)
	}
}

func TestHosts(t *testing.T) {
	hosts := []string{
		host01,
		host02,
	}

	c := NewHashRing()
	for _, h := range hosts {
		c.Add(h)
	}
	fmt.Println("hosts in the ring", c.Hosts())

	addedHosts := c.Hosts()
	for _, h := range hosts {
		found := false
		for _, ah := range addedHosts {
			if h == ah {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("missing host", h)
		}
	}
	c.Remove(host01)
	fmt.Println("hosts in the ring", c.Hosts())
}

func TestDelSlice(t *testing.T) {
	items := []uint64{0, 1, 2, 3, 5, 20, 22, 23, 25, 27, 28, 30, 35, 37, 1008, 1009}
	deletes := []uint64{25, 37, 1009, 3, 100000}

	c := &HashRing{}
	c.sortedSet = append(c.sortedSet, items...)

	fmt.Printf("before deletion%+v\n", c.sortedSet)

	for _, val := range deletes {
		c.delSlice(val)
	}

	for _, val := range deletes {
		for _, item := range c.sortedSet {
			if item == val {
				t.Fatalf("%d wasn't deleted\n", val)
			}
		}
	}

	fmt.Printf("after deletions: %+v\n", c.sortedSet)
}
