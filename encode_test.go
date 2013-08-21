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
	err := Encode(buf, ASYNC, false)
	if err != nil {
		t.Error("Encoding failed", err)
	}
	if !bytes.Equal(buf.Bytes(), BoolBytes) {
		t.Error("Encoding  is incorrect")
	}

}
func TestEInt(t *testing.T) {
	fmt.Println("Encoding 1i")
	buf := new(bytes.Buffer)
	err := Encode(buf, ASYNC, int32(1))
	if err != nil {
		t.Error("Encoding failed", err)
	}
	if !bytes.Equal(buf.Bytes(), IntBytes) {
		t.Error("Encoding is incorrect")
	}

}
func TestEIntList(t *testing.T) {
	fmt.Println("Encoding enlist 1i")
	buf := new(bytes.Buffer)
	err := Encode(buf, ASYNC, []int32{1})
	if err != nil {
		t.Error("Encoding failed", err)
	}
	if !bytes.Equal(buf.Bytes(), IntVectorBytes) {
		t.Error("Encoding is incorrect")
	}

}

func TestEByteVector(t *testing.T) {
	fmt.Println("Encoding `byte$til 5")
	buf := new(bytes.Buffer)
	err := Encode(buf, ASYNC, []byte{0, 1, 2, 3, 4})
	if err != nil {
		t.Error("Encoding failed", err)
	}
	if !bytes.Equal(buf.Bytes(), ByteVectorBytes) {
		t.Error("Encoding is incorrect")
	}

}

func TestECharArray(t *testing.T) {
	fmt.Println("Encoding \"GOOG\"")
	buf := new(bytes.Buffer)
	err := Encode(buf, ASYNC, "GOOG")
	if err != nil {
		t.Error("Encoding failed", err)
	}
	if !bytes.Equal(buf.Bytes(), CharArrayBytes) {
		t.Error("Encoding is incorrect")
	}

}

func TestESymbolArray(t *testing.T) {
	fmt.Println("Encoding `abc`bc`c")
	buf := new(bytes.Buffer)
	err := Encode(buf, ASYNC, []string{"abc", "bc", "c"})
	if err != nil {
		t.Error("Encoding failed", err)
	}
	shouldbe := SymbolVectorBytes
	if !bytes.Equal(buf.Bytes(), shouldbe) {
		t.Error("Encoding is incorrect")
	}

}

func TestEDictWithAtoms(t *testing.T) {
	fmt.Println("Encoding `a`b!2 3")
	buf := new(bytes.Buffer)
	dict := Dict{[]string{"a", "b"}, []int32{2, 3}}
	err := Encode(buf, ASYNC, dict)
	if err != nil {
		t.Error("Encoding failed", err)
	}
	if !bytes.Equal(buf.Bytes(), DictWithAtomsBytes) {
		t.Error("Encoding is incorrect")
	}

}

func TestEDictWithVectors(t *testing.T) {
	fmt.Println("Encoding `a`b!enlist each 2 3")
	buf := new(bytes.Buffer)
	dict := Dict{[]string{"a", "b"}, []interface{}{[]int32{2}, []int32{3}}}
	err := Encode(buf, ASYNC, dict)
	if err != nil {
		t.Error("Encoding failed", err)
	}
	if !bytes.Equal(buf.Bytes(), DictWithVectorsBytes) {
		t.Error("Encoding is incorrect")
	}

}

func TestETable(t *testing.T) {
	fmt.Println("Encoding ([]a:enlist 2;b:enlist 3)")
	buf := new(bytes.Buffer)
	dict := Table{[]string{"a", "b"}, []interface{}{[]int32{2}, []int32{3}}}
	err := Encode(buf, ASYNC, dict)
	if err != nil {
		t.Error("Encoding failed", err)
	}
	if !bytes.Equal(buf.Bytes(), TableBytes) {
		t.Error("Encoding is incorrect")
	}

}

func TestEGeneralList(t *testing.T) {
	fmt.Println("Encoding `byte$enlist til 5")
	buf := new(bytes.Buffer)
	var list = []interface{}{[]byte{0, 1, 2, 3, 4}}
	err := Encode(buf, ASYNC, list)
	if err != nil {
		t.Error("Encoding failed", err)
	}
	if !bytes.Equal(buf.Bytes(), GeneralListBytes) {
		t.Error("Encoding is incorrect")
	}

}

func TestEError(t *testing.T) {
	fmt.Println("Encoding 'type error")
	buf := new(bytes.Buffer)
	e := errors.New("type")
	err := Encode(buf, ASYNC, e)
	if err != nil {
		t.Error("Encoding failed", err)
	}
	if !bytes.Equal(buf.Bytes(), ErrorBytes) {
		t.Error("Encoding is incorrect")
	}

}

func TestEFunction(t *testing.T) {
	fmt.Println("Encoding function in root namespace")
	buf := new(bytes.Buffer)
	err := Encode(buf, ASYNC, Function{Namespace:"",Body:"{x+y}"})
	if err != nil {
		t.Error("Encoding failed", err)
	}
	if !bytes.Equal(buf.Bytes(), FuncBytes) {
		fmt.Println(buf.Bytes())
		fmt.Println(FuncBytes)
		t.Error("Encoding is incorrect")
	}
}

func TestEFunctionNonRoot(t *testing.T) {
	fmt.Println("Encoding function in non-root namespace")
	buf := new(bytes.Buffer)
	err := Encode(buf, ASYNC, Function{Namespace:"d",Body:"{x+y}"})
	if err != nil {
		t.Error("Encoding failed", err)
	}
	if !bytes.Equal(buf.Bytes(), FuncNonRootBytes) {
		fmt.Println(buf.Bytes())
		fmt.Println(FuncNonRootBytes)
		t.Error("Encoding is incorrect")
	}
}
