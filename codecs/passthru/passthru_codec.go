package passthru

import (
	"encoding/binary"
	"time"

	"github.com/qigao/ogrenet/shared/crc16"
)

func NewPassThruHead(cmd CodecType, id [4]byte, len uint16) *HeadCodec {
	cseq := [4]byte{}
	current := time.Now().Unix()
	binary.BigEndian.PutUint32(cseq[:], uint32(current))
	return NewHeadCodec(version, cmd, id, len, cseq)
}

func NewEmptyPassThruCodec() *CodecPassThru {
	return NewRegisterCodec([4]byte{0x00, 0x00, 0x00, 0x00})
}

func NewRegisterCodec(id [4]byte) *CodecPassThru {
	registerTail := NewTailCodec(0)
	registerHead := NewPassThruHead(Register, id, 0)
	return &CodecPassThru{
		Head: registerHead,
		Body: nil,
		Tail: registerTail,
	}
}

func NewUnRegisterCodec(id [4]byte) *CodecPassThru {
	unregisterTail := NewTailCodec(0)
	unregisterHead := NewPassThruHead(Unregister, id, 0)
	return &CodecPassThru{
		Head: unregisterHead,
		Body: nil,
		Tail: unregisterTail,
	}
}

func NewAckCodec(id [4]byte) *CodecPassThru {
	ackTail := NewTailCodec(0)
	ackHead := NewPassThruHead(Ack, id, 0)
	return &CodecPassThru{
		Head: ackHead,
		Body: nil,
		Tail: ackTail,
	}
}

func NewHeartBeatCodec(id [4]byte) *CodecPassThru {
	heartbeatTail := NewTailCodec(0)
	heartbeatHead := NewPassThruHead(HeartBeat, id, 0)
	return &CodecPassThru{
		Head: heartbeatHead,
		Body: nil,
		Tail: heartbeatTail,
	}
}

func NewDataCodec(id [4]byte, data []byte) *CodecPassThru {
	crc := crc16.CheckSum(data)
	dataTail := NewTailCodec(crc)
	dataHead := NewPassThruHead(Data, id, uint16(len(data)))
	return &CodecPassThru{
		Head: dataHead,
		Body: data,
		Tail: dataTail,
	}
}

func NewCloseCodec(id [4]byte) *CodecPassThru {
	closeTail := NewTailCodec(0)
	closeHead := NewPassThruHead(Close, id, 0)
	return &CodecPassThru{
		Head: closeHead,
		Body: nil,
		Tail: closeTail,
	}
}

func NewReConnectCodec(id [4]byte) *CodecPassThru {
	reconnectTail := NewTailCodec(0)
	reconnectHead := NewPassThruHead(ReConnect, id, 0)
	return &CodecPassThru{
		Head: reconnectHead,
		Body: nil,
		Tail: reconnectTail,
	}
}
