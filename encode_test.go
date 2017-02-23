package kdb

import (
	"bytes"
	"errors"
	//"fmt"
	"testing"
)

var encodingTests = []struct {
	desc     string // description
	input    *K     // input
	expected []byte // expected result
}{
	{"false", &K{-KB, NONE, false}, BoolBytes},
	{"1i", Int(1), IntBytes},
	{"enlist 1i", &K{KI, NONE, []int32{1}}, IntVectorBytes},
	{"`byte$til 5", &K{KG, NONE, []byte{0, 1, 2, 3, 4}}, ByteVectorBytes},
	{"\"GOOG\"", &K{KC, NONE, "GOOG"}, CharArrayBytes},
	{"`abc`bc`c", SymbolV([]string{"abc", "bc", "c"}), SymbolVectorBytes},
	{"`a`b!2 3", NewDict(SymbolV([]string{"a", "b"}), &K{KI, NONE, []int32{2, 3}}), DictWithAtomsBytes},
	{"{x+y} in .d", NewFunc("d", "{x+y}"), FuncNonRootBytes},
	{"{x+y}", NewFunc("", "{x+y}"), FuncBytes},
	{"'type", Error(errors.New("type")), ErrorBytes},
	{"(\"ac\";`b;`)", &K{K0, NONE, []*K{{KC, NONE, "ac"}, Symbol("b"), Symbol("")}}, GenericList2Bytes},
	{"`byte$enlist til 5", &K{K0, NONE, []*K{{KG, NONE, []byte{0, 1, 2, 3, 4}}}}, GeneralListBytes},
	{"([]a:enlist 2;b:enlist 3)", NewTable([]string{"a", "b"},
		[]*K{{KI, NONE, []int32{2}}, {KI, NONE, []int32{3}}}),
		TableBytes},
	{"`a`b!enlist each 2 3", NewDict(SymbolV([]string{"a", "b"}),
		&K{K0, NONE, []*K{{KI, NONE, []int32{2}}, {KI, NONE, []int32{3}}}}),
		DictWithVectorsBytes},
	{"1#2013.06.10T22:03:49.713", &K{KZ, NONE, []float64{4909.9193253819449}}, DateTimeVecBytes},
	{"1#2013.06.10", &K{KD, NONE, []int32{4909}}, DateVecBytes},
	{"1#21:53:37.963", &K{KT, NONE, []int32{78817963}}, TimeVecBytes},
	{"21:22:01 + 1 2", &K{KV, NONE, []int32{76922, 76923}}, SecondVecBytes},
	{"21:22*til 2", &K{KU, NONE, []int32{0, 1282}}, MinuteVecBytes},
	{"2013.06m +til 3", &K{KM, NONE, []int32{161, 162, 163}}, MonthVecBytes},
}

func TestEncoding(t *testing.T) {
	for _, tt := range encodingTests {
		// fmt.Println(tt.desc)
		buf := new(bytes.Buffer)
		err := Encode(buf, ASYNC, tt.input)
		if err != nil {
			t.Errorf("Encoding '%s' failed:%s", tt.desc, err)
			continue
		}
		if !bytes.Equal(buf.Bytes(), tt.expected) {
			t.Errorf("Encoded '%s' incorrectly. Expected '%v', got '%v'\n", tt.desc, tt.expected, buf.Bytes())
		}
	}
}
