package kdb

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"math"
	"reflect"
	"time"
	"unicode"
)

// Request type
const (
	ASYNC = iota
	SYNC
	RESPONSE
)

// Vector attributes
type Attr int8

const (
	NONE Attr = iota
	SORTED
	UNIQUE
	PARTED
	GROUPED
)

// Q type constants
const (
	K0 int8 = 0 // generic type
	//      type bytes qtype     ctype  accessor
	KB int8 = 1  // 1 boolean   char   kG
	UU int8 = 2  // 16 guid     U      kU
	KG int8 = 4  // 1 byte      char   kG
	KH int8 = 5  // 2 short     short  kH
	KI int8 = 6  // 4 int       int    kI
	KJ int8 = 7  // 8 long      long   kJ
	KE int8 = 8  // 4 real      float  kE
	KF int8 = 9  // 8 float     double kF
	KC int8 = 10 // 1 char      char   kC
	KS int8 = 11 // * symbol    char*  kS

	KP int8 = 12 // 8 timestamp long   kJ (nanoseconds from 2000.01.01)
	KM int8 = 13 // 4 month     int    kI (months from 2000.01.01)
	KD int8 = 14 // 4 date      int    kI (days from 2000.01.01)
	KZ int8 = 15 // 8 datetime  double kF (DO NOT USE)
	KN int8 = 16 // 8 timespan  long   kJ (nanoseconds)
	KU int8 = 17 // 4 minute    int    kI
	KV int8 = 18 // 4 second    int    kI
	KT int8 = 19 // 4 time      int    kI (millisecond)

	// table,dict
	XT int8 = 98  //   x->k is XD
	XD int8 = 99  //   kK(x)[0] is keys. kK(x)[1] is values.
	SD int8 = 127 // sorted dict

	// function types
	KFUNC      int8 = 100
	KFUNCUP    int8 = 101 // unary primitive
	KFUNCBP    int8 = 102 // binary primitive
	KFUNCTR    int8 = 103 // ternary (operator)
	KPROJ      int8 = 104 // projection
	KCOMP      int8 = 105 // composition
	KEACH      int8 = 106 // f'
	KOVER      int8 = 107 //	f/
	KSCAN      int8 = 108 // f\
	KPRIOR     int8 = 109 // f':
	KEACHRIGHT int8 = 110 // f/:
	KEACHLEFT  int8 = 111 // f\:
	KDYNLOAD   int8 = 112 // dynamic load

	// error type
	KERR int8 = -128
)

// The Qipc header follows the contract below
// 0x00         - 1 Byte 	- Architecture used for encoding the message, BigEndian (0) or LittleEndian (1)
// 0x00         - 1 Byte 	- Message type,  Async (0) or Sync (1) or response (2)
// 0x0000       - 2 Bytes 	- Compressed & Reserved flag
// 0x00000000   - 4 Bytes	- Message length
type ipcHeader struct {
	ByteOrder   byte
	RequestType byte
	Compressed  byte
	Reserved    byte
	MsgSize     uint32
}

// Short nil
const Nh int16 = math.MinInt16

// Short infinity
const Wh int16 = math.MaxInt16

// Int nil
const Ni int32 = math.MinInt32

// Int Infinity
const Wi int32 = math.MaxInt32

// Long nil
const Nj int64 = math.MinInt64

// Long Infinity
const Wj int64 = math.MaxInt64

// Double nil
var Nf float64 = math.NaN()

// Double Infinity
var Wf float64 = math.Inf(1)

// K structure
type K struct {
	Type int8
	Attr Attr
	Data interface{}
}

func Bool(x bool) *K {
	return &K{Type: -KB, Attr: NONE, Data: x}
}

func BoolV(x []bool) *K {
	return &K{Type: KB, Attr: NONE, Data: x}
}

func UUID(x uuid.UUID) *K {
	return &K{Type: -UU, Attr: NONE, Data: x}
}

func UUIDV(x []uuid.UUID) *K {
	return &K{Type: UU, Attr: NONE, Data: x}
}

func Byte(x byte) *K {
	return &K{Type: -KG, Attr: NONE, Data: x}
}

func ByteV(x []byte) *K {
	return &K{Type: KG, Attr: NONE, Data: x}
}

func Short(x int16) *K {
	return &K{Type: -KH, Attr: NONE, Data: x}
}

func ShortV(x []int16) *K {
	return &K{Type: KH, Attr: NONE, Data: x}
}

func Int(x int32) *K {
	return &K{Type: -KI, Attr: NONE, Data: x}
}

func IntV(x []int32) *K {
	return &K{Type: KI, Attr: NONE, Data: x}
}

func Long(x int64) *K {
	return &K{Type: -KJ, Attr: NONE, Data: x}
}

func LongV(x []int64) *K {
	return &K{Type: KJ, Attr: NONE, Data: x}
}

func Real(x float32) *K {
	return &K{Type: -KE, Attr: NONE, Data: x}
}

func RealV(x []float32) *K {
	return &K{Type: KE, Attr: NONE, Data: x}
}

func Float(x float64) *K {
	return &K{Type: -KF, Attr: NONE, Data: x}
}

func FloatV(x []float64) *K {
	return &K{Type: KF, Attr: NONE, Data: x}
}

func String(x string) *K {
	return &K{Type: KC, Attr: NONE, Data: x}
}

func Symbol(x string) *K {
	return &K{Type: -KS, Attr: NONE, Data: x}
}

func SymbolV(x []string) *K {
	return &K{Type: KS, Attr: NONE, Data: x}
}

func Timestamp(x time.Time) *K {
	return &K{Type: -KP, Attr: NONE, Data: x.UTC()}
}

func TimestampV(x []time.Time) *K {
	for i, t := range x {
		x[i] = t.UTC()
	}
	return &K{Type: KP, Attr: NONE, Data: x}
}

func Date(x time.Time) *K {
	return &K{Type: -KD, Attr: NONE, Data: x.UTC()}
}

func DateV(x []time.Time) *K {
	for i, t := range x {
		x[i] = t.UTC()
	}
	return &K{Type: KD, Attr: NONE, Data: x}
}

func Time(x time.Time) *K {
	return &K{Type: -KT, Attr: NONE, Data: x.UTC()}
}

func TimeV(x []time.Time) *K {
	for i, t := range x {
		x[i] = t.UTC()
	}
	return &K{Type: KT, Attr: NONE, Data: x}
}

func Error(x error) *K {
	return &K{Type: KERR, Attr: NONE, Data: x}
}

func Enlist(x ...*K) *K {
	return &K{Type: K0, Attr: NONE, Data: x}
}

func NewTable(cols []string, data ...*K) *K {
	return &K{Type: XT, Attr: NONE, Data: Table{cols, data}}
}

func NewDict(k, v *K) *K {
	return &K{Type: XD, Attr: NONE, Data: Dict{k, v}}
}

func NewFunc(ctx, body string) *K {
	return &K{Type: KFUNC, Attr: NONE, Data: Function{Namespace: ctx, Body: body}}
}

// Len returns number of elements in K structure
// Special cases:
// Atoms and functions = 1
// Dictionaries = number of keys
// Tables = number of rows
func (k *K) Len() int {
	if k.Type < K0 || k.Type >= KFUNC {
		return 1
	} else if k.Type >= K0 && k.Type <= KT {
		return reflect.ValueOf(k.Data).Len()
	} else if k.Type == XD {
		return k.Data.(Dict).Key.Len()
	} else if k.Type == XT {
		return k.Data.(Table).Data[0].Len()
	} else {
		return -1
	}
}

// Index returns i'th element of K structure
func (k *K) Index(i int) interface{} {
	if k.Type < K0 || k.Type > XT {
		return nil
	}
	if k.Len() == 0 {
		// need to return null of that type
		if k.Type == K0 {
			return &K{K0, NONE, make([]*K, 0)}
		}
		return nil

	}
	if k.Type >= K0 && k.Type <= KT {
		return reflect.ValueOf(k.Data).Index(i).Interface()
	}
	// case for table
	// need to return dict with header
	if k.Type != XT {
		return nil
	}
	var t = k.Data.(Table)
	return &K{XD, NONE, t.Index(i)}
}

var attrPrint = map[Attr]string{NONE: "", SORTED: "`s#", UNIQUE: "`u#", PARTED: "`p#", GROUPED: "`g#"}

// Convert K structure to string
func (k K) String() string {
	if k.Type < K0 {
		return fmt.Sprint(k.Data)
	}
	if k.Type > K0 && k.Type <= KT {
		return fmt.Sprint(attrPrint[k.Attr], k.Data)
	}
	switch k.Type {
	case K0:
		list := k.Data.([]*K)
		var buf bytes.Buffer
		buf.WriteString(attrPrint[k.Attr])
		buf.WriteString("(")
		for i, l := range list {
			buf.WriteString(l.String())
			if i < len(list)-1 {
				buf.WriteString(";")
			}
		}
		buf.WriteString(")")
		return buf.String()
	case XD:
		return attrPrint[k.Attr] + k.Data.(Dict).String()
	case XT:
		return attrPrint[k.Attr] + k.Data.(Table).String()
	case KFUNC:
		return k.Data.(Function).Body
	default:
		return "unknown"
	}
}

// Message is malformated or invalid
var ErrBadMsg = errors.New("Bad Message")

// Message header is invalid
var ErrBadHeader = errors.New("Bad header")

// Cannot process sync requests
var ErrSyncRequest = errors.New("nosyncrequest")

// Epoch offset for Q time. Q epoch starts on 1st Jan 2000
var qEpoch = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

// Month
type Month int32

func (m Month) String() string {
	return fmt.Sprintf("%v.%02vm", 2000+int(m/12), int(m)%12)
}

// Minute
type Minute time.Time

func (m Minute) String() string {
	timeVal := time.Time(m)
	return fmt.Sprintf("%02v:%02v", timeVal.Hour(), timeVal.Minute())

}

// Second hh:mm:ss
type Second time.Time

func (s Second) String() string {
	timeVal := time.Time(s)
	return fmt.Sprintf("%02v:%02v:%02v", timeVal.Hour(), timeVal.Minute(), timeVal.Second())
}

// Table
type Table struct {
	Columns []string
	Data    []*K
}

func (tbl *Table) Index(i int) Dict {
	var d = Dict{}
	d.Key = &K{KS, NONE, tbl.Columns}
	vslice := make([]*K, len(tbl.Columns))
	d.Value = &K{K0, NONE, vslice}
	for ci := range tbl.Columns {
		kd := tbl.Data[ci].Index(i)
		dtype := tbl.Data[ci].Type
		if dtype == K0 {
			dtype = kd.(*K).Type
		} else if dtype > K0 && dtype <= KT {
			dtype = -dtype
		}
		vslice[ci] = &K{dtype, NONE, kd}
	}
	return d
}

func (tbl Table) String() string {
	var buf bytes.Buffer
	buf.WriteString("+")
	buf.WriteString(fmt.Sprint(tbl.Columns))
	buf.WriteString("!")
	buf.WriteString("(")
	for i, l := range tbl.Data {
		buf.WriteString(l.String())
		if i < len(tbl.Data)-1 {
			buf.WriteString(";")
		}
	}
	buf.WriteString(")")
	return buf.String()

}

// Dictionary: ordered key->value mapping.
// Key and Value must be slices of the same length
type Dict struct {
	Key   *K
	Value *K
}

func (d Dict) String() string {
	return fmt.Sprintf("%v!%v", d.Key.Data, d.Value.Data)
}

// utility function to titlecase first letter of the string
func titleInitial(str string) string {
	for i, v := range str {
		return string(unicode.ToTitle(v)) + str[i+1:]
	}
	return ""
}

// Unmarshall dict to struct
func UnmarshalDict(t Dict, v interface{}) error {
	var keys = t.Key.Data.([]string)
	var vals = t.Value.Data.([]*K)
	vv := reflect.ValueOf(v)
	if vv.Kind() != reflect.Ptr || vv.IsNil() {
		return errors.New("Invalid target type. Should be non null pointer")
	}
	vv = reflect.Indirect(vv)
	for i := range keys {
		val := vals[i].Data
		fv := vv.FieldByName(titleInitial(keys[i]))
		if !fv.IsValid() {
			continue
		}
		if fv.CanSet() && reflect.TypeOf(val).AssignableTo(fv.Type()) {
			fv.Set(reflect.ValueOf(val))
		}
	}
	return nil
}

// Unmarshall dict to map[string]{}interface
func UnmarshalDictToMap(t Dict, v interface{}) error {
	vv := reflect.ValueOf(v)
	if vv.Kind() == reflect.Map {
		// check if keys are
		kt := vv.Type()
		if kt.Key().Kind() != reflect.String {
			return errors.New("target type should be map[string]T")
		}

		if vv.IsNil() {
			vv.Set(reflect.MakeMap(kt))
		}
	} else {
		return errors.New("target type should be map[string]T")
	}

	if t.Key == nil || t.Value == nil {
		return nil //nothing to decode
	}

	var keys = t.Key.Data.([]string)
	var vals = t.Value.Data.([]*K)

	for i := range keys {
		val := reflect.ValueOf(vals[i].Data)
		kv := reflect.ValueOf(titleInitial(keys[i]))
		vv.SetMapIndex(kv, val)
	}

	return nil
}

func UnmarshalTable(t Table, v interface{}) (interface{}, error) {
	vv := reflect.ValueOf(v)
	if vv.Kind() != reflect.Ptr || vv.IsNil() {
		return nil, errors.New("Invalid target type. Shoult be non null pointer")
	}
	vv = reflect.Indirect(vv)
	for i := 0; i < int(t.Data[0].Len()); i++ {
		emptyelem := reflect.New(vv.Type().Elem())
		err := UnmarshalDict(t.Index(i), emptyelem.Interface())
		if err != nil {
			fmt.Println("Failed to unmrshall dict", err)
			return nil, err
		}
		vv = reflect.Append(vv, reflect.Indirect(emptyelem))
	}
	return vv.Interface(), nil
}

// Function
type Function struct {
	Namespace string
	Body      string
}
