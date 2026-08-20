package transport

import (
	"errors"
	"testing"

	"github.com/qigao/ogrenet/secure"
	"github.com/qigao/ogrenet/wire"
)

type factoryCipher struct {
	id int
}

func (*factoryCipher) Algorithm() secure.Algorithm { return secure.AlgAESGCM }

func (*factoryCipher) Seal(dst, plaintext []byte) ([]byte, error) {
	return append(dst, plaintext...), nil
}

func (*factoryCipher) Open(dst, ciphertext []byte) ([]byte, error) {
	return append(dst, ciphertext...), nil
}

func TestCipherFactoryCreatesPerConnectionCipher(t *testing.T) {
	cfg := defaultConfig()
	calls := 0
	if err := WithCipherFactory(func() (secure.Cipher, error) {
		calls++
		return &factoryCipher{id: calls}, nil
	})(&cfg); err != nil {
		t.Fatal(err)
	}

	first, err := cfg.newFramer()
	if err != nil {
		t.Fatal(err)
	}
	second, err := cfg.newFramer()
	if err != nil {
		t.Fatal(err)
	}

	firstCipher := first.(*wire.Codec).Cipher.(*factoryCipher)
	secondCipher := second.(*wire.Codec).Cipher.(*factoryCipher)
	if firstCipher == secondCipher || firstCipher.id == secondCipher.id {
		t.Fatal("cipher factory reused state across connections")
	}
	if calls != 2 {
		t.Fatalf("factory calls = %d, want 2", calls)
	}
}

func TestCipherFactoryValidationAndErrors(t *testing.T) {
	cfg := defaultConfig()
	if err := WithCipherFactory(nil)(&cfg); !errors.Is(err, ErrNilCipherFactory) {
		t.Fatalf("WithCipherFactory(nil) = %v, want ErrNilCipherFactory", err)
	}

	cfg = defaultConfig()
	if err := WithCipherFactory(func() (secure.Cipher, error) { return nil, nil })(&cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.newFramer(); !errors.Is(err, ErrNilCipher) {
		t.Fatalf("nil cipher = %v, want ErrNilCipher", err)
	}

	sentinel := errors.New("factory failed")
	cfg = defaultConfig()
	if err := WithCipherFactory(func() (secure.Cipher, error) { return nil, sentinel })(&cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.newFramer(); !errors.Is(err, sentinel) {
		t.Fatalf("factory error = %v, want wrapped sentinel", err)
	}
}

func TestCipherOptionsLastOneWins(t *testing.T) {
	shared := &factoryCipher{id: 7}
	calls := 0
	factory := func() (secure.Cipher, error) {
		calls++
		return &factoryCipher{id: calls}, nil
	}

	cfg := defaultConfig()
	if err := WithCipherFactory(factory)(&cfg); err != nil {
		t.Fatal(err)
	}
	if err := WithCipher(shared)(&cfg); err != nil {
		t.Fatal(err)
	}
	framer, err := cfg.newFramer()
	if err != nil {
		t.Fatal(err)
	}
	if got := framer.(*wire.Codec).Cipher; got != shared {
		t.Fatal("WithCipher did not override WithCipherFactory")
	}
	if calls != 0 {
		t.Fatalf("overridden factory called %d times", calls)
	}

	cfg = defaultConfig()
	if err := WithCipher(shared)(&cfg); err != nil {
		t.Fatal(err)
	}
	if err := WithCipherFactory(factory)(&cfg); err != nil {
		t.Fatal(err)
	}
	framer, err = cfg.newFramer()
	if err != nil {
		t.Fatal(err)
	}
	if got := framer.(*wire.Codec).Cipher; got == shared {
		t.Fatal("WithCipherFactory did not override WithCipher")
	}
}

func TestWriteQueueRejectsOverflowingAdmissionCapacity(t *testing.T) {
	cfg := defaultConfig()
	maxInt := int(^uint(0) >> 1)
	if err := WithWriteQueue(maxInt)(&cfg); !errors.Is(err, ErrInvalidQueueSize) {
		t.Fatalf("WithWriteQueue(maxInt) = %v, want ErrInvalidQueueSize", err)
	}
}
