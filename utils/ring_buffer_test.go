package utils

import (
	"testing"
)

func TestNewRingBuffer(t *testing.T) {
	inCh := make(chan int)
	outCh := make(chan int, 8) // try to change outCh buffer to understand the result
	rb := NewRingBuffer(inCh, outCh)
	go rb.Run()

	for i := 0; i < 10; i++ {
		if i > 3 {
			inCh <- i
		}
		if i == 5 {
			break
		}
	}

	close(inCh)

	for res := range outCh {
		t.Log(res)
	}
}
