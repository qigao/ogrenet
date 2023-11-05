package ogrenet

import "github.com/qigao/ogrenet/codecs/passthru"

var (
	HeartbeatCMD  = []byte{byte(passthru.HeartBeat)}
	RegisterCMD   = []byte{byte(passthru.Register)}
	UnregisterCMD = []byte{byte(passthru.UnRegister)}
	CloseCMD      = []byte{byte(passthru.Close)}
	DataCMD       = []byte{byte(passthru.Data)}
)
