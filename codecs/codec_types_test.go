package codecs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func BenchmarkTimeStamp_Marshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		t := time.Date(2001, 2, 1, 14, 30, 12, 0o5, time.UTC)

		// Calling MarshalBinary() method
		_, error := t.MarshalBinary()
		assert.NoError(b, error)
	}
}

func BenchmarkTimeStamp_Binary(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = TimeBasedCseq()
	}
}
