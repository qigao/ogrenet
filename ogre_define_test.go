package ogrenet

import (
	"sync"
	"testing"

	"github.com/qigao/ogrenet/codecs/passthru"
	"github.com/qigao/ogrenet/shared/avl"
	"github.com/qigao/ogrenet/shared/bimap"
	"github.com/stretchr/testify/assert"
)

func TestProxy(t *testing.T) {
	// create a new Proxy instance
	p := Proxy{
		epoll:     &OgreEpoll{},
		connMap:   sync.Map{},
		timerTree: &avl.AVLTree{},
		limiter:   Limiter{},
		codecPool: &passthru.CodecPool{},
		keepAlive: true,
		pattern:   "test",
		endpoints: &bimap.BiMap[int, string]{},
		msgChan:   make(chan *MsgConn),
	}

	// test that all fields are initialized correctly
	assert.NotNil(t, p.epoll)
	assert.NotNil(t, p.connMap)
	assert.NotNil(t, p.timerTree)
	assert.NotNil(t, p.limiter)
	assert.NotNil(t, p.codecPool)
	assert.Nil(t, p.handle)
	assert.True(t, p.keepAlive)
	assert.NotNil(t, p.proxyMethod)
	assert.Equal(t, ProxyNone, p.proxyMethod)
	assert.Equal(t, "test", p.pattern)
	assert.NotNil(t, p.endpoints)
	assert.NotNil(t, p.msgChan)
}
