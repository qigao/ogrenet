package legacy

import (
	"bytes"
	"testing"

	"github.com/qigao/ogrenet/secure"
)

func TestLegacyAESCFBRoundTrip(t *testing.T) {
	constructors := []struct {
		name string
		new  func([]byte, []byte) (secure.Cipher, error)
	}{
		{name: "aes128", new: NewAES128CFB},
		{name: "aes192", new: NewAES192CFB},
		{name: "aes256", new: NewAES256CFB},
	}

	for _, tt := range constructors {
		t.Run(tt.name, func(t *testing.T) {
			c, err := tt.new([]byte("short-key"), []byte("short-iv"))
			if err != nil {
				t.Fatal(err)
			}
			plain := []byte("legacy payload")
			sealed, err := c.Seal(nil, plain)
			if err != nil {
				t.Fatal(err)
			}
			opened, err := c.Open(nil, sealed)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(opened, plain) {
				t.Fatalf("got %x, want %x", opened, plain)
			}
		})
	}
}
