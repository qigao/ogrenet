package transport

import (
	"context"
	"errors"
	"testing"

	"github.com/qigao/ogrenet"
)

func BenchmarkErrorWrapKnown(b *testing.B) {
	raw := errors.New("reset")
	cause := categorized(ErrConnectionReset, raw)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = envelopeOperational(OpRead, ogrenet.SchemeTCP, nil, nil, ErrorReset, cause)
	}
}

func BenchmarkErrorWrapUnknown(b *testing.B) {
	raw := errors.New("opaque")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = envelopeOperational(OpRead, ogrenet.SchemeTCP, nil, nil, ErrorUnknown, raw)
	}
}

func BenchmarkErrorClassifyReset(b *testing.B) {
	raw := categorized(ErrConnectionReset, errors.New("reset"))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = classifyOperational(OpRead, ogrenet.SchemeTCP, nil, nil, raw, hintNone)
	}
}

func BenchmarkErrorClassifyTimeout(b *testing.B) {
	raw := &TimeoutError{Kind: TimeoutWrite, Cause: context.DeadlineExceeded}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = classifyOperational(OpWrite, ogrenet.SchemeTCP, nil, nil, raw, hintNone)
	}
}
