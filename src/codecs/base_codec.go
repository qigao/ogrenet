package codecs

import (
	"errors"
)

const MaxBufferSize = 2 * 1024 * 1024 // 最大消息长度

var ErrBufferInvalidId = errors.New("buffer: invalid id")

type Message struct {
	OptValidateId bool // 防重放攻击开关

	buf    []byte
	msgId  uint64
	msgLen uint32
	msg    Codec

	onMessage  func(msg Codec)
	onError    func(err error)
	parserFunc func([]byte) (Codec, error)
	hasError   bool
}

func NewBuffer(parserFunc func([]byte) (Codec, error)) *Message {
	return &Message{
		parserFunc: parserFunc,
		msgId:      1,
	}
}

func (b *Message) Write(rawBuf []byte) {
	if b.hasError {
		return
	}

	if len(rawBuf) == 0 {
		return
	}

	buf := make([]byte, len(rawBuf))
	copy(buf, rawBuf)

	if b.msgLen > 0 {
		buf = b.writeBytes(buf)
		if len(buf) == 0 {
			return
		}
	}

	if len(b.buf) == 0 {
		b.buf = make([]byte, len(buf))
		copy(b.buf, buf)
	} else {
		b.buf = append(b.buf, buf...)
	}

	for {
		// 检查消息头
		msg, err := b.parserFunc(b.buf)
		if err != nil {
			if b.onError != nil {
				b.reset()
				b.onError(err)
			}
			return
		}
		// 防重放攻击
		if b.OptValidateId && msg.MsgId() <= b.msgId {
			if b.onError != nil {
				b.reset()
				b.onError(ErrBufferInvalidId)
			}
			return
		}
		b.msgId = msg.MsgId()
		if msg.GetLength() == 0 {
			if b.onMessage != nil {
				b.onMessage(msg)
				// 由于onMessage可能会改变buffer，所以这里需要做判断
				if len(b.buf) == int(msg.HeaderLength()) {
					b.buf = nil
					return
				}
			}
		} else {
			b.msg = msg
		}

		b.msgLen = msg.GetLength()

		// 写入剩下的数据
		b.buf = b.buf[msg.HeaderLength():]
		if b.msgLen > 0 {
			b.buf = b.writeBytes(b.buf)
		} else {
			if b.msg != nil && b.onMessage != nil {
				b.onMessage(b.msg)
			}
		}
	}
}

func (b *Message) OnMessage(f func(msg Codec)) {
	b.onMessage = f
}

func (b *Message) OnError(f func(err error)) {
	b.onError = f
}

func (b *Message) Reset() {
	b.reset()
}

func (b *Message) reset() {
	// 可能有异步操作的风险，暂时先注释掉
	return
}

func (b *Message) writeBytes(buf []byte) []byte {
	l := uint32(len(buf))
	if l <= b.msgLen {
		b.msgLen = b.msgLen - l

		if b.msg != nil && b.onMessage != nil {
			b.msg.SetData(buf)
			if b.msgLen == 0 {
				b.onMessage(b.msg)
			}
		}

		return nil
	}

	// if l > msgLen
	if b.msg != nil && b.onMessage != nil {
		b.msg.SetData(buf[:b.msgLen])
		b.onMessage(b.msg)
	}

	buf = buf[b.msgLen:]
	b.msgLen = 0

	return buf
}
