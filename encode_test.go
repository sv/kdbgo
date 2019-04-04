package kdb

import (
	"bytes"
	"errors"
	"time"
	//"fmt"
	"github.com/nu7hatch/gouuid"
	"testing"
)

var encodingTests = []struct {
	desc     string // description
	input    *K     // input
	expected []byte // expected result
}{
	{"0b", &K{-KB, NONE, false}, BoolBytes},
	{"01b", &K{KB, NONE, []bool{false, true}}, BoolVecBytes},
	{"0xab", &K{-KG, NONE, byte(0xab)}, []byte{0x01, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0xfc, 0xab}},
	{"257h", &K{-KH, NONE, int16(257)}, []byte{0x01, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0xfb, 0x01, 0x01}},
	{"1i", Int(1), IntBytes},
	{"enlist 1i", IntV([]int32{1}), IntVectorBytes},
	{"`byte$til 5", &K{KG, NONE, []byte{0, 1, 2, 3, 4}}, ByteVectorBytes},
	{"\"GOOG\"", &K{KC, NONE, "GOOG"}, CharArrayBytes},
	{"`GOOG", Symbol("GOOG"), SymbolBytes},
	{"`abc`bc`c", SymbolV([]string{"abc", "bc", "c"}), SymbolVectorBytes},
	{"`a`b!2 3", NewDict(SymbolV([]string{"a", "b"}), IntV([]int32{2, 3})), DictWithAtomsBytes},
	{"{x+y} in .d", NewFunc("d", "{x+y}"), FuncNonRootBytes},
	{"{x+y}", NewFunc("", "{x+y}"), FuncBytes},
	{"'type", Error(errors.New("type")), ErrorBytes},
	{"(\"ac\";`b;`)", NewList(&K{KC, NONE, "ac"}, Symbol("b"), Symbol("")), GenericList2Bytes},
	{"`byte$enlist til 5", NewList([]*K{{KG, NONE, []byte{0, 1, 2, 3, 4}}}...), GeneralListBytes},
	{"([]a:enlist 2;b:enlist 3)", NewTable([]string{"a", "b"},
		[]*K{IntV([]int32{2}), IntV([]int32{3})}),
		TableBytes},
	{"([a:enlist 2i]b:enlist 3i)", NewDict(NewTable([]string{"a"}, []*K{IntV([]int32{2})}), NewTable([]string{"b"}, []*K{IntV([]int32{3})})), KeyedTableBytes},
	{"`a`b!enlist each 2 3", NewDict(SymbolV([]string{"a", "b"}),
		NewList([]*K{IntV([]int32{2}), IntV([]int32{3})}...)),
		DictWithVectorsBytes},
	{"1#2013.06.10T22:03:49.713", &K{KZ, NONE, []time.Time{DatetimeAsTime}}, DateTimeVecBytes},
	{"1#2013.06.10", &K{KD, NONE, []time.Time{DateAsTime}}, DateVecBytes},
	{"1#21:53:37.963", &K{KT, NONE, []int32{78817963}}, TimeVecBytes},
	{"21:22:01 + 1 2", &K{KV, NONE, []int32{76922, 76923}}, SecondVecBytes},
	{"21:22*til 2", &K{KU, NONE, []int32{0, 1282}}, MinuteVecBytes},
	{"2013.06m +til 3", &K{KM, NONE, []Month{161, 162, 163}}, MonthVecBytes},
	{"2018.01.26D01:49:00.884361000", &K{-KP, NONE, TimestampAsTime}, TimestampAsBytes},
	{"2#2018.01.26D01:49:00.884361000", &K{KP, NONE, []time.Time{TimestampAsTime, TimestampAsTime}}, TimestampVectorAsBytes},
	{"8c6b8b64-6815-6084-0a3e-178401251b68", &K{-UU, NONE, uuid.UUID{0x8c, 0x6b, 0x8b, 0x64, 0x68, 0x15, 0x60, 0x84, 0x0a, 0x3e, 0x17, 0x84, 0x01, 0x25, 0x1b, 0x68}}, GUIDBytes},
	{"0x0 sv/: 16 cut `byte$til 32", &K{UU, NONE, []uuid.UUID{{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}, {0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f}}}, GUIDVecBytes},
	{"0D01:22:33.444555666*1+til 2", &K{KN, NONE, []time.Duration{4953444555666, 9906889111332}}, TimespanVecBytes},
	{"`s#`a`b!2 3", &K{XD, SORTED, Dict{&K{KS, SORTED, []string{"a", "b"}}, IntV([]int32{2, 3})}}, SortedDictBytes},
	{"`s#([]a:enlist 2;b:enlist 3)", &K{XT, SORTED, Table{[]string{"a", "b"}, []*K{{KI, PARTED, []int32{2}}, IntV([]int32{3})}}}, SortedTableBytes},
	{"-8!sums", &K{KSCAN, NONE, &K{KFUNCBP, NONE, byte(1)}}, []byte{0x01, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x6c, 0x66, 0x01}},
	{"-8!2000.01.01", Date(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)), []byte{0x01, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x00, 0x00, 0xf2, 0x00, 0x00, 0x00, 0x00}},
	{"-8!(\"ab\"@0x40\\:)", &K{KCOMP, NONE, []*K{{KPROJ, NONE, []*K{{KFUNCBP, NONE, uint8(18)}, {KC, NONE, "ab"}}}, {KEACHLEFT, NONE, &K{-KG, NONE, byte(0x40)}}}}, []byte{0x01, 0x00, 0x00, 0x00, 0x1f, 0x00, 0x00, 0x00, 0x69, 0x02, 0x00, 0x00, 0x00, 0x68, 0x02, 0x00, 0x00, 0x00, 0x66, 0x12, 0x0a, 0x00, 0x02, 0x00, 0x00, 0x00, 0x61, 0x62, 0x6f, 0xfc, 0x40}},
}

func TestEncoding(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	for _, tt := range encodingTests {
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

func BenchmarkEncodeAll(b *testing.B) {
	buf := new(bytes.Buffer)

	for i := 0; i < b.N; i++ {
		for _, tt := range encodingTests {
			buf.Reset()
			err := Encode(buf, ASYNC, tt.input)
			if err != nil {
				b.Errorf("Encoding '%s' failed:%s", tt.desc, err)
				continue
			}
		}
	}
}
