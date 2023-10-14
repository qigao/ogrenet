package codec

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
