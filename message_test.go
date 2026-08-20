package ogrenet

import (
	"errors"
	"testing"
)

func TestPayloadTypeIDsStable(t *testing.T) {
	if got := uint8(PayloadBinary); got != 0x00 {
		t.Fatalf("PayloadBinary = %#x, want 0x00", got)
	}
	if got := uint8(PayloadText); got != 0x01 {
		t.Fatalf("PayloadText = %#x, want 0x01", got)
	}
}

func TestMessageValidation(t *testing.T) {
	if err := Text("hello").Validate(); err != nil {
		t.Fatal(err)
	}
	if err := Bin([]byte{0xff, 0x00}).Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := Message{Type: PayloadText, Data: []byte{0xff}}
	if !errors.Is(invalid.Validate(), ErrInvalidUTF8) {
		t.Fatalf("got %v, want ErrInvalidUTF8", invalid.Validate())
	}
}

func TestBinCopiesInput(t *testing.T) {
	src := []byte{1, 2, 3}
	msg := Bin(src)
	src[0] = 9
	if msg.Data[0] != 1 {
		t.Fatal("Bin retained caller-owned backing storage")
	}
}
