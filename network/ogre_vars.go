package network

import "github.com/qigao/ogrenet/codecs/passthru"

var (
	Heartbeat  = []byte{byte(passthru.HeartBeat)}
	Register   = []byte{byte(passthru.Register)}
	Unregister = []byte{byte(passthru.UnRegister)}
	Close      = []byte{byte(passthru.Close)}
	Data       = []byte{byte(passthru.Data)}
)
