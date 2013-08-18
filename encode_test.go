package kdb

import (
	"bytes"
	"errors"
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

func TestEByteVector(t *testing.T) {
	fmt.Println("Encoding `byte$til 5")
	buf := new(bytes.Buffer)
	err := Encode(buf, RESPONSE, []byte{0, 1, 2, 3, 4})
	if err != nil {
		t.Error("Encoding failed", err)
	}
	fmt.Printf("%x\n", buf)

}

func TestECharArray(t *testing.T) {
	fmt.Println("Encoding \"GOOG\"")
	buf := new(bytes.Buffer)
	err := Encode(buf, RESPONSE, "GOOG")
	if err != nil {
		t.Error("Encoding failed", err)
	}
	fmt.Printf("%x\n", buf)

}

func TestESymbolArray(t *testing.T) {
	fmt.Println("Encoding `abc`bc`c")
	buf := new(bytes.Buffer)
	err := Encode(buf, RESPONSE, []string{"abc", "bc", "c"})
	if err != nil {
		t.Error("Encoding failed", err)
	}
	fmt.Printf("%x\n", buf)

}

func TestEDictWithAtoms(t *testing.T) {
	fmt.Println("Encoding `a`b!2 3")
	buf := new(bytes.Buffer)
	dict := Dict{[]string{"a", "b"}, []int32{2, 3}}
	err := Encode(buf, RESPONSE, dict)
	if err != nil {
		t.Error("Encoding failed", err)
	}
	fmt.Printf("%x\n", buf)

}

func TestEDictWithVectors(t *testing.T) {
	fmt.Println("Encoding `a`b!enlist each 2 3")
	buf := new(bytes.Buffer)
	dict := Dict{[]string{"a", "b"}, []interface{}{[]int32{2}, []int32{3}}}
	err := Encode(buf, RESPONSE, dict)
	if err != nil {
		t.Error("Encoding failed", err)
	}
	fmt.Printf("%x\n", buf)

}

func TestETable(t *testing.T) {
	fmt.Println("Encoding ([]a:enlist 2;b:enlist 3)")
	buf := new(bytes.Buffer)
	dict := Table{[]string{"a", "b"}, []interface{}{[]int32{2}, []int32{3}}}
	err := Encode(buf, RESPONSE, dict)
	if err != nil {
		t.Error("Encoding failed", err)
	}
	fmt.Printf("%x\n", buf)

}

func TestEGeneralList(t *testing.T) {
	fmt.Println("Encoding `byte$enlist til 5")
	buf := new(bytes.Buffer)
	var list = []interface{}{[]byte{0, 1, 2, 3, 4}}
	err := Encode(buf, RESPONSE, list)
	if err != nil {
		t.Error("Encoding failed", err)
	}
	fmt.Printf("%x\n", buf)

}

func TestEError(t *testing.T) {
	fmt.Println("Encoding 'type error")
	buf := new(bytes.Buffer)
	e := errors.New("type")
	err := Encode(buf, RESPONSE, e)
	if err != nil {
		t.Error("Encoding failed", err)
	}
	fmt.Printf("%x\n", buf)

}
