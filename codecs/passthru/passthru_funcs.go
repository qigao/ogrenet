package passthru

import (
	"encoding/binary"
	"time"

	"github.com/qigao/ogrenet/codecs"
)

func NewPassThruHead(cmd CodecType, id [4]byte, len uint16) *HeadCodec {
	cseq := [4]byte{}
	current := time.Now().Unix()
	binary.BigEndian.PutUint32(cseq[:], uint32(current))
	return NewHeadCodec(version, cmd, id, len, cseq)
}

func NewEmptyPassThruCodec() *CodecPassThru {
	tail := NewTailCodec(codecs.ZeroBytes)
	head := NewHeadCodec(0, Unknown, codecs.Empty, 1, codecs.Empty)

	return &CodecPassThru{
		Head: head,
		Body: codecs.ZeroBytes,
		Tail: tail,
	}
}

func NewRegisterCodec(id [4]byte) *CodecPassThru {
	registerTail := NewTailCodec(codecs.ZeroBytes)
	registerHead := NewPassThruHead(Register, id, 1)

	return &CodecPassThru{
		Head: registerHead,
		Body: codecs.ZeroBytes,
		Tail: registerTail,
	}
}

func NewUnRegisterCodec(id [4]byte) *CodecPassThru {
	unregisterTail := NewTailCodec(codecs.ZeroBytes)
	unregisterHead := NewPassThruHead(UnRegister, id, 1)

	return &CodecPassThru{
		Head: unregisterHead,
		Body: codecs.ZeroBytes,
		Tail: unregisterTail,
	}
}

func NewAckCodec(id [4]byte, data []byte) *CodecPassThru {
	dataTail := NewTailCodec(data)
	dataHead := NewPassThruHead(Ack, id, uint16(len(data)))

	return &CodecPassThru{
		Head: dataHead,
		Body: codecs.ZeroBytes,
		Tail: dataTail,
	}
}

func NewHeartBeatCodec(id [4]byte) *CodecPassThru {
	heartbeatTail := NewTailCodec(codecs.ZeroBytes)
	heartbeatHead := NewPassThruHead(HeartBeat, id, 1)

	return &CodecPassThru{
		Head: heartbeatHead,
		Body: codecs.ZeroBytes,
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
	closeTail := NewTailCodec(codecs.ZeroBytes)
	closeHead := NewPassThruHead(Close, id, 1)

	return &CodecPassThru{
		Head: closeHead,
		Body: codecs.ZeroBytes,
		Tail: closeTail,
	}
}

func NewReConnectCodec(id [4]byte) *CodecPassThru {
	reconnectTail := NewTailCodec(codecs.ZeroBytes)
	reconnectHead := NewPassThruHead(ReConnect, id, 1)

	return &CodecPassThru{
		Head: reconnectHead,
		Body: codecs.ZeroBytes,
		Tail: reconnectTail,
	}
}
