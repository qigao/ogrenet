package codecs

import (
	"github.com/qigao/ogrenet/errors"
)

const MaxBufferSize = 2 * 1024 * 1024 // 最大消息长度

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

func NewMessage(parserFunc func([]byte) (Codec, error)) *Message {
	return &Message{
		parserFunc: parserFunc,
		msgId:      1,
	}
}

func (m *Message) Write(rawBuf []byte) {
	if m.hasError {
		return
	}

	if len(rawBuf) == 0 {
		return
	}

	buf := make([]byte, len(rawBuf))
	copy(buf, rawBuf)

	if m.msgLen > 0 {
		buf = m.writeBytes(buf)
		if len(buf) == 0 {
			return
		}
	}

	if len(m.buf) == 0 {
		m.buf = make([]byte, len(buf))
		copy(m.buf, buf)
	} else {
		m.buf = append(m.buf, buf...)
	}

	for {
		// 检查消息头
		msg, err := m.parserFunc(m.buf)
		if err != nil {
			if m.onError != nil {
				m.reset()
				m.onError(err)
			}
			return
		}
		// 防重放攻击
		if m.OptValidateId && msg.MsgId() <= m.msgId {
			if m.onError != nil {
				m.reset()
				m.onError(errors.ErrBufferInvalidId)
			}
			return
		}
		m.msgId = msg.MsgId()
		if msg.GetLength() == 0 {
			if m.onMessage != nil {
				m.onMessage(msg)
				// 由于onMessage可能会改变buffer，所以这里需要做判断
				if len(m.buf) == int(msg.HeaderLength()) {
					m.buf = nil
					return
				}
			}
		} else {
			m.msg = msg
		}

		m.msgLen = msg.GetLength()

		// 写入剩下的数据
		m.buf = m.buf[msg.HeaderLength():]
		if m.msgLen > 0 {
			m.buf = m.writeBytes(m.buf)
		} else {
			if m.msg != nil && m.onMessage != nil {
				m.onMessage(m.msg)
			}
		}
	}
}

func (m *Message) OnMessage(f func(msg Codec)) {
	m.onMessage = f
}

func (m *Message) OnError(f func(err error)) {
	m.onError = f
}

func (m *Message) Reset() {
	m.reset()
}

func (m *Message) reset() {
	// 可能有异步操作的风险，暂时先注释掉
	return
}

func (m *Message) writeBytes(buf []byte) []byte {
	l := uint32(len(buf))
	if l <= m.msgLen {
		m.msgLen = m.msgLen - l

		if m.msg != nil && m.onMessage != nil {
			m.msg.SetData(buf)
			if m.msgLen == 0 {
				m.onMessage(m.msg)
			}
		}

		return nil
	}

	// if l > msgLen
	if m.msg != nil && m.onMessage != nil {
		m.msg.SetData(buf[:m.msgLen])
		m.onMessage(m.msg)
	}

	buf = buf[m.msgLen:]
	m.msgLen = 0

	return buf
}
