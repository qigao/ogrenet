package wire

import (
	"bytes"
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

func TestEncryptedFrameRejectsTamper(t *testing.T) {
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
