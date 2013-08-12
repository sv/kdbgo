package kdb

import (
	"bytes"
	"fmt"
	"testing"
)

func TestEBool(t *testing.T) {
	fmt.Println("Encoding true")
	buf := new(bytes.Buffer)
	err := Encode(buf, RESPONSE, true)
	if err != nil {
		t.Error("Encoding failed", err)
	}
	fmt.Printf("%x\n", buf)

}
func TestEInt(t *testing.T) {
	fmt.Println("Encoding 1i")
	buf := new(bytes.Buffer)
	err := Encode(buf, RESPONSE, int32(1))
	if err != nil {
		t.Error("Encoding failed", err)
	}
	fmt.Printf("%x\n", buf)

}
func TestEIntList(t *testing.T) {
	fmt.Println("Encoding enlist 1i")
	buf := new(bytes.Buffer)
	err := Encode(buf, RESPONSE, []int32{1})
	if err != nil {
		t.Error("Encoding failed", err)
	}
	fmt.Printf("%x\n", buf)

}

func TestESymbol(t *testing.T) {
	fmt.Println("Encoding \"GOOG\"")
	buf := new(bytes.Buffer)
	err := Encode(buf, RESPONSE, "GOOG")
	if err != nil {
		t.Error("Encoding failed", err)
	}
	fmt.Printf("%x\n", buf)

}
