package main

import (
	"fmt"
	"testing"
	"unsafe"
)

func Test_unsafeString(t *testing.T) {
	bytes := []byte("a;1.0")
	fmt.Println(unsafeString(unsafe.Pointer(&bytes[0]), 0, 1))
}
