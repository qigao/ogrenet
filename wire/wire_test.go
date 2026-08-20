package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/secure"
)

func TestTextRoundTrip(t *testing.T) {
	codec := New(nil)
	want := ogrenet.Text("hello 世界")
	frame, err := codec.Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := codec.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != want.Type || !bytes.Equal(got.Data, want.Data) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestEncryptedTextAndBinaryRoundTrip(t *testing.T) {
	cipher, err := secure.NewAESGCM([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	codec := New(cipher)

	tests := []ogrenet.Message{
		ogrenet.Text("secret text"),
		ogrenet.Bin([]byte{0, 1, 2, 0xff}),
		ogrenet.Text(""),
		ogrenet.Bin(nil),
	}
	for _, want := range tests {
		frame, err := codec.Encode(want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := codec.Decode(frame)
		if err != nil {
			t.Fatal(err)
		}
		if got.Type != want.Type || !bytes.Equal(got.Data, want.Data) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	}
}

func TestDecodeOnePartialFrame(t *testing.T) {
	codec := New(nil)
	frame, err := codec.Encode(ogrenet.Text("partial"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(frame); i++ {
		if _, _, err := codec.DecodeOne(frame[:i]); !errors.Is(err, ErrNeedMore) {
			t.Fatalf("prefix %d: got %v, want ErrNeedMore", i, err)
		}
	}
}

func TestEncryptedFrameRejectsPayloadTamper(t *testing.T) {
	cipher, err := secure.NewAESGCM([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	codec := New(cipher)
	frame, err := codec.Encode(ogrenet.Bin([]byte("payload")))
	if err != nil {
		t.Fatal(err)
	}
	frame[len(frame)-1] ^= 1
	if _, err := codec.Decode(frame); err == nil {
		t.Fatal("tampered encrypted frame decoded successfully")
	}
}

func TestEncryptedFrameRejectsSemanticHeaderTamper(t *testing.T) {
	cipher, err := secure.NewAESGCM([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	codec := New(cipher)
	frame, err := codec.Encode(ogrenet.Bin([]byte("payload")))
	if err != nil {
		t.Fatal(err)
	}

	frame[3] ^= FlagText
	if _, err := codec.Decode(frame); err == nil {
		t.Fatal("frame with tampered Text/Binary flag decoded successfully")
	}
}

func TestUnsupportedFlagsRejected(t *testing.T) {
	codec := New(nil)
	frame, err := codec.Encode(ogrenet.Text("hello"))
	if err != nil {
		t.Fatal(err)
	}
	frame[3] |= 1 << 7
	if _, err := codec.Decode(frame); !errors.Is(err, ErrUnsupportedFlags) {
		t.Fatalf("got %v, want ErrUnsupportedFlags", err)
	}
}

func TestInvalidEncryptedHeaderRejectedBeforePayloadArrives(t *testing.T) {
	header := makeHeader(FlagEncrypted, secure.AlgAESGCM, DefaultMaxPayload)
	if _, _, err := New(nil).DecodeOne(header); !errors.Is(err, ErrMissingCipher) {
		t.Fatalf("missing cipher header = %v, want ErrMissingCipher", err)
	}

	cipher, err := secure.NewAESGCM([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	header = makeHeader(FlagEncrypted, secure.AlgSM4GCM, DefaultMaxPayload)
	if _, _, err := New(cipher).DecodeOne(header); !errors.Is(err, ErrAlgorithmMismatch) {
		t.Fatalf("mismatched cipher header = %v, want ErrAlgorithmMismatch", err)
	}
}

func TestUnencryptedFrameCannotAdvertiseCipherAlgorithm(t *testing.T) {
	header := makeHeader(0, secure.AlgAESGCM, 0)
	if _, _, err := New(nil).DecodeOne(header); !errors.Is(err, ErrAlgorithmMismatch) {
		t.Fatalf("DecodeOne = %v, want ErrAlgorithmMismatch", err)
	}
	if _, err := ParseHeader(header); !errors.Is(err, ErrAlgorithmMismatch) {
		t.Fatalf("ParseHeader = %v, want ErrAlgorithmMismatch", err)
	}
}

func TestMaxPayloadBoundsPlaintextAndDecodedPayload(t *testing.T) {
	codec := New(nil)
	codec.MaxPayload = 4
	if _, err := codec.Encode(ogrenet.Bin([]byte("12345"))); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversized plaintext Encode = %v, want ErrFrameTooLarge", err)
	}

	expander := expandingCipher{}
	codec = New(expander)
	codec.MaxPayload = 4
	frame := makeHeader(FlagEncrypted, expander.Algorithm(), 1)
	frame = append(frame, 0x01)
	if _, err := codec.Decode(frame); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expanded plaintext Decode = %v, want ErrFrameTooLarge", err)
	}
}

func makeHeader(flags uint8, algorithm secure.Algorithm, length uint32) []byte {
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint16(header[0:2], Magic)
	header[2] = Version
	header[3] = flags
	binary.BigEndian.PutUint16(header[4:6], uint16(algorithm))
	binary.BigEndian.PutUint32(header[6:10], length)
	return header
}

type expandingCipher struct{}

func (expandingCipher) Algorithm() secure.Algorithm { return secure.AlgLegacyAES128CFB }
func (expandingCipher) Seal(dst, plaintext []byte) ([]byte, error) {
	return append(dst, plaintext...), nil
}
func (expandingCipher) Open(dst, _ []byte) ([]byte, error) {
	return append(dst, []byte("12345")...), nil
}
