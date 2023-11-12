package ogrenet

import (
	"sync"
	"testing"

	"github.com/qigao/ogrenet/shared/avl"
	"github.com/stretchr/testify/assert"
)

func TestProxy(t *testing.T) {
	// create a new OgreNet instance
	p := OgreNet{
		epoll:     &OgreEpoll{},
		connMap:   sync.Map{},
		timerTree: &avl.AVLTree{},
		msgChan:   make(chan *MsgConn),
	}

	// test that all fields are initialized correctly
	assert.NotNil(t, p.epoll)
	assert.NotNil(t, p.connMap)
	assert.NotNil(t, p.timerTree)
	assert.Nil(t, p.handle)
	assert.NotNil(t, p.msgChan)
}
