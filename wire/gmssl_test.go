//go:build gmssl && cgo

package wire

import (
	"bytes"
	"testing"

	"github.com/qigao/ogrenet"
	gmsslsecure "github.com/qigao/ogrenet/secure/gmssl"
)

func TestGmSSLTextAndBinaryRoundTrip(t *testing.T) {
	cipher, err := gmsslsecure.NewSM4GCM([]byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	codec := New(cipher)

	tests := []ogrenet.Message{
		ogrenet.Text("国密 text payload"),
		ogrenet.Text(""),
		ogrenet.Bin([]byte{0x00, 0x01, 0xff, 0x00}),
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
