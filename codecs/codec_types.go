package codecs

type (
	CSEQ = [4]byte
	ID   = [4]byte
)

var (
	Empty     = [4]byte{0x00, 0x00, 0x00, 0x00}
	ZeroBytes = []byte{0x00}
)

const ZeroCRC16 = 0x40bf
