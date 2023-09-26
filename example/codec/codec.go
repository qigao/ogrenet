package codec

import (
	"encoding/binary"
	"github.com/qigao/ogrenet/codecs"
	"github.com/qigao/ogrenet/errors"
	"sync/atomic"
)

const (
	MessageVersion = 0x1 // 版本， 0-f

	MessageIdIndex      = 1
	MessageTypeIndex    = 9
	MessageLengthIndex  = 10
	MessageHeaderLength = 14
)

// Message tls 通讯消息体
// Type：
// 0x00 AH 登录信息包  AH ----> 控制器
// 0x01 控制器 AH登录响应包  控制器 ------> AH
// 0x02 AH 登出响应包 AH  --------> 控制器
// 0x03 AH/控制器 keepalive 心跳包
// 0x04 AH AH服务信息包

const (
	LoginRequestCode         = 0x00 // ah/ih ----> control 登录消息
	LoginResponseCode        = 0x01 // control ----> ah/ih 登录响应消息
	AHLogoutRequestCode      = 0x02 // ah ----> control 注销消息
	KeepaliveRequestCode     = 0x03 // ah <----> control 心跳消息
	ServerProtectRequestCode = 0x04 // control ----> ah ah保护服务消息
	IHOnlineRequestCode      = 0x05 // control ----> ah ih认证消息
	AHListRequestCode        = 0x06 // control ----> ih ah信息列表
	IHLogoutRequestCode      = 0x07 // ih ----> control 注销请求消息
	IHOnlineResponseCode     = 0x08 // ah ----> control ih上线后ah业务相关数据信息体响应消息
	CustomRequestCode        = 0xff // 自定义消息
)

var messageId = uint64(0)

type ExampleCodec struct {
	Version byte
	Id      uint64
	Type    byte
	Length  uint32
	Data    []byte
}

// Marshal 编码消息
func (t *ExampleCodec) Marshal() []byte {
	if t.Id <= 0 {
		t.Id = atomic.AddUint64(&messageId, 1)
	}

	if t.Version == 0 {
		t.Version = MessageVersion
	}
	result := []byte{t.Version}

	// ID
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, t.Id)
	result = append(result, buf...)

	// Type
	result = append(result, t.Type)

	// Length
	t.Length = uint32(len(t.Data))
	binary.BigEndian.PutUint32(buf, t.Length)
	result = append(result, buf[:4]...)

	// Data
	result = append(result, t.Data...)
	return result
}

// MsgId 获取消息ID
func (t *ExampleCodec) MsgId() uint64 {
	return t.Id
}

// SetData 设置消息体
func (t *ExampleCodec) SetData(buf []byte) {
	if t.Data == nil {
		t.Data = buf
	} else {
		t.Data = append(t.Data, buf...)
	}
}

// HeaderLength 获取消息头长度
func (t *ExampleCodec) HeaderLength() uint32 {
	return MessageHeaderLength
}

// GetLength 获取消息体长度
func (t *ExampleCodec) GetLength() uint32 {
	return t.Length
}

// CheckHeader 分析并检测消息头
func CheckHeader(buf []byte) (codecs.Codec, error) {
	msg := ExampleCodec{}
	if buf == nil || len(buf) == 0 {
		return nil, errors.ErrBufferInvalidIsNil
	}
	msg.Version = buf[0]
	// 检查消息版本
	if len(buf) > 0 && buf[0] != MessageVersion {
		return nil, errors.ErrBufferInvalidStart
	}

	if len(buf) < MessageHeaderLength {
		return nil, errors.ErrBufferInvalidHeader
	}

	l := binary.BigEndian.Uint32(buf[MessageLengthIndex : MessageLengthIndex+4])
	if l > codecs.MaxBufferSize { // 每次通讯数据不超过一定尺寸
		return nil, errors.ErrBufferDataTooLong
	}
	msg.Length = l
	// 解析Header
	msg.Id = binary.BigEndian.Uint64(buf[MessageIdIndex : MessageIdIndex+8])
	msg.Type = buf[MessageTypeIndex]
	return &msg, nil
}
