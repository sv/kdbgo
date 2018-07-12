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
	desc     string
	input    *K
	expected []byte
}{
	// Boolean
	{"0b", &K{-KB, NONE, false}, BoolBytes},
	{"01b", &K{KB, NONE, []bool{false, true}}, BoolVecBytes},

	// UUID
	{"8c6b8b64-6815-6084-0a3e-178401251b68", &K{-UU, NONE, uuid.UUID{0x8c, 0x6b, 0x8b, 0x64, 0x68, 0x15, 0x60, 0x84, 0x0a, 0x3e, 0x17, 0x84, 0x01, 0x25, 0x1b, 0x68}}, GuidBytes},
	{"00010203-0405-0607-0809-0a0b0c0d0e0f 10111213-1415-1617-1819-1a1b1c1d1e1f", &K{UU, NONE, []uuid.UUID{{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}, {0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f}}}, GuidVecBytes},

	// Byte/Int8
	{"0x01", &K{-KG, NONE, byte(1)}, []byte{0x01, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0xfc, 0x01}},
	{"0x0102", &K{KG, NONE, []byte{1, 2}}, []byte{0x01, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x04, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x02}},

	// Short/Int16
	{"1h", &K{-KH, NONE, int16(1)}, []byte{0x01, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0xfb, 0x01, 0x00}},
	{"1 2h", &K{KH, NONE, []int16{1, 2}}, []byte{0x01, 0x00, 0x00, 0x00, 0x12, 0x00, 0x00, 0x00, 0x05, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00}},

	// Int/Int32
	{"1i", Int(1), []byte{0x01, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x00, 0x00, 0xfa, 0x01, 0x00, 0x00, 0x00}},
	{"1 2i", &K{KI, NONE, []int32{1, 2}}, []byte{0x01, 0x00, 0x00, 0x00, 0x16, 0x00, 0x00, 0x00, 0x06, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00}},

	// Long/Int64
	{"1j", Long(1), []byte{0x01, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0xf9, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	{"1 2j", &K{KJ, NONE, []int64{1, 2}}, []byte{0x01, 0x00, 0x00, 0x00, 0x1e, 0x00, 0x00, 0x00, 0x07, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},


	{`"GOOG"`, &K{KC, NONE, "GOOG"}, CharArrayBytes},
	{"`abc`bc`c", SymbolV([]string{"abc", "bc", "c"}), SymbolVectorBytes},
	{"`a`b!2 3", NewDict(SymbolV([]string{"a", "b"}), &K{KI, NONE, []int32{2, 3}}), DictWithAtomsBytes},
	{"{x+y} in .d", NewFunc("d", "{x+y}"), FuncNonRootBytes},
	{"{x+y}", NewFunc("", "{x+y}"), FuncBytes},
	{"'type", Error(errors.New("type")), ErrorBytes},
	{"(\"ac\";`b;`)", NewList(&K{KC, NONE, "ac"}, Symbol("b"), Symbol("")), GenericList2Bytes},
	{"([]a:enlist 2;b:enlist 3)", NewTable([]string{"a", "b"},
		[]*K{{KI, NONE, []int32{2}}, {KI, NONE, []int32{3}}}),
		TableBytes},
	{"([a:enlist 2i]b:enlist 3i)", NewDict(NewTable([]string{"a"}, []*K{{KI, NONE, []int32{2}}}), NewTable([]string{"b"}, []*K{{KI, NONE, []int32{3}}})), KeyedTableBytes},
	{"`a`b!enlist each 2 3", NewDict(SymbolV([]string{"a", "b"}),
		&K{K0, NONE, []*K{{KI, NONE, []int32{2}}, {KI, NONE, []int32{3}}}}),
		DictWithVectorsBytes},
	{"1#2013.06.10T22:03:49.713", &K{KZ, NONE, []float64{4909.9193253819449}}, DateTimeVecBytes},
	{"1#2013.06.10", &K{KD, NONE, []int32{4909}}, DateVecBytes},
	{"1#21:53:37.963", &K{KT, NONE, []int32{78817963}}, TimeVecBytes},
	{"21:22:01 + 1 2", &K{KV, NONE, []int32{76922, 76923}}, SecondVecBytes},
	{"21:22*til 2", &K{KU, NONE, []int32{0, 1282}}, MinuteVecBytes},
	{"2013.06m +til 3", &K{KM, NONE, []Month{161, 162, 163}}, MonthVecBytes},
	{"2018.01.26D01:49:00.884361000", &K{-KP, NONE, TimestampAsTime}, TimestampAsBytes},
	{"2#2018.01.26D01:49:00.884361000", &K{KP, NONE, []time.Time{TimestampAsTime, TimestampAsTime}}, TimestampVectorAsBytes},
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
