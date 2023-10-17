package test

import (
	"fmt"
	"testing"
)

func TestBit(t *testing.T) {
	bitsize := 32 << (^uint(0) >> 63)
	fmt.Printf("%b\n", bitsize)
	fmt.Printf("%b\n", 32<<1)
	maxintHeadBit := 1 << (bitsize - 2)
	fmt.Printf("%b\n", maxintHeadBit)
}
