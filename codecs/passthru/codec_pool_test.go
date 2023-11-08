package passthru

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCodecPool_NewRegisterCodec(t *testing.T) {
	pool := NewCodecPool()
	binary.LittleEndian.PutUint32(cseq[:], uint32(current))
	cseqArr := [4]byte(cseq)
	c := pool.NewRegisterCodec(clientID, cseqArr)
	assert.Equal(t, Register, c.Head.CMD)
	assert.Equal(t, clientID, c.Head.ID)
	assert.Equal(t, uint16(0x01), c.Head.BodyLen)
	assert.Equal(t, uint8(0x00), c.Head.Version)
	assert.Equal(t, []uint8([]byte{0x00}), c.Body)
	pool.PutCodec(&c)
}

func TestCodecPool_NewDataCodec(t *testing.T) {
	pool := NewCodecPool()
	data := []byte{0x01, 0x02, 0x03}
	binary.LittleEndian.PutUint32(cseq[:], uint32(current))
	c := pool.NewDataCodec(clientID, cseq, data)
	assert.Equal(t, Data, c.Head.CMD)
	assert.Equal(t, clientID, c.Head.ID)
	assert.Equal(t, uint16(0x03), c.Head.BodyLen)
	assert.Equal(t, uint8(0x00), c.Head.Version)
	assert.Equal(t, data, c.Body)
	pool.PutCodec(&c)
}
