package passthru

import (
	"encoding/binary"
	"time"
)

func NewPassThruHead(cmd CodecType, id [4]byte, len uint16) *HeadCodec {
	cseq := [4]byte{}
	current := time.Now().Unix()
	binary.BigEndian.PutUint32(cseq[:], uint32(current))
	return NewHeadCodec(version, cmd, id, len, cseq)
}

func NewEmptyPassThruCodec() *CodecPassThru {
	tail := NewTailCodec(zeroBytes)
	head := NewHeadCodec(0, Unknow, [4]byte{0x00, 0x00, 0x00, 0x00}, 1, [4]byte{0x00, 0x00, 0x00, 0x00})
	return &CodecPassThru{
		Head: head,
		Body: zeroBytes,
		Tail: tail,
	}
}

func NewRegisterCodec(id [4]byte) *CodecPassThru {
	registerTail := NewTailCodec(zeroBytes)
	registerHead := NewPassThruHead(Register, id, 1)
	return &CodecPassThru{
		Head: registerHead,
		Body: zeroBytes,
		Tail: registerTail,
	}
}

func NewUnRegisterCodec(id [4]byte) *CodecPassThru {
	unregisterTail := NewTailCodec(zeroBytes)
	unregisterHead := NewPassThruHead(UnRegister, id, 1)
	return &CodecPassThru{
		Head: unregisterHead,
		Body: zeroBytes,
		Tail: unregisterTail,
	}
}

func NewAckCodec(id [4]byte, data []byte) *CodecPassThru {
	dataTail := NewTailCodec(data)
	dataHead := NewPassThruHead(Ack, id, uint16(len(data)))
	return &CodecPassThru{
		Head: dataHead,
		Body: data,
		Tail: dataTail,
	}
}

func NewHeartBeatCodec(id [4]byte) *CodecPassThru {
	heartbeatTail := NewTailCodec(zeroBytes)
	heartbeatHead := NewPassThruHead(HeartBeat, id, 1)
	return &CodecPassThru{
		Head: heartbeatHead,
		Body: zeroBytes,
		Tail: heartbeatTail,
	}
}

func NewDataCodec(id [4]byte, data []byte) *CodecPassThru {
	dataTail := NewTailCodec(data)
	dataLen := len(data)
	dataHead := NewPassThruHead(Data, id, uint16(dataLen))
	return &CodecPassThru{
		Head: dataHead,
		Body: data,
		Tail: dataTail,
	}
}

func NewCloseCodec(id [4]byte) *CodecPassThru {
	closeTail := NewTailCodec(zeroBytes)
	closeHead := NewPassThruHead(Close, id, 1)
	return &CodecPassThru{
		Head: closeHead,
		Body: zeroBytes,
		Tail: closeTail,
	}
}

func NewReConnectCodec(id [4]byte) *CodecPassThru {
	reconnectTail := NewTailCodec(zeroBytes)
	reconnectHead := NewPassThruHead(ReConnect, id, 1)
	return &CodecPassThru{
		Head: reconnectHead,
		Body: zeroBytes,
		Tail: reconnectTail,
	}
}
