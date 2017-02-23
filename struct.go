package kdb

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"
	"unicode"
)

// Request type
const (
	ASYNC    int = 0
	SYNC     int = 1
	RESPONSE int = 2
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

type ipcHeader struct {
	ByteOrder   byte
	RequestType byte
	Compressed  byte
	Reserved    byte
	MsgSize     uint32
}

// short nil
const Nh int16 = math.MinInt16

// short infinity
const Wh int16 = math.MaxInt16

// int nil
const Ni int32 = math.MinInt32

// int infinity
const Wi int32 = math.MaxInt32

// long nil
const Nj int64 = math.MinInt64

// long infinity
const Wj int64 = math.MaxInt64

// double nil
var Nf float64 = math.NaN()

// double infinity +ve
var Wf float64 = math.Inf(1)

type K struct {
	Type int8
	Attr Attr
	Data interface{}
}

func Int(x int32) *K {
	return &K{-KI, NONE, x}
}

func Long(x int64) *K {
	return &K{-KJ, NONE, x}
}

func Real(x float32) *K {
	return &K{-KE, NONE, x}
}

func Float(x float64) *K {
	return &K{-KF, NONE, x}
}

func Error(x error) *K {
	return &K{KERR, NONE, x}
}

func Symbol(x string) *K {
	return &K{-KS, NONE, x}
}
func SymbolV(x []string) *K {
	return &K{KS, NONE, x}
}

func Atom(t int8, x interface{}) *K {
	return &K{t, NONE, x}
}

func NewList(x ...*K) *K {
	return &K{K0, NONE, x}
}

func NewFunc(ctx, body string) *K {
	return &K{KFUNC, NONE, Function{Namespace: ctx, Body: body}}
}

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

func (k K) String() string {
	if k.Type < K0 {
		return fmt.Sprint(k.Data)
	}
	if k.Type > K0 && k.Type <= KT {
		return fmt.Sprint(k.Data)
	}
	switch k.Type {
	case K0:
		list := k.Data.([]*K)
		var buf bytes.Buffer
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
		return k.Data.(Dict).String()
	case XT:
		return k.Data.(Table).String()
	case KFUNC:
		return k.Data.(Function).Body
	default:
		return "unknown"
	}
}

// message is malformated or invalid
var ErrBadMsg = errors.New("Bad Message")

// msg header is invalid
var ErrBadHeader = errors.New("Bad header")

// Cannot process sync requests
var ErrSyncRequest = errors.New("nosyncrequest")

// offset between Q time
var qEpoch = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

// kdb month
type Month int32

func (m Month) String() string {
	return fmt.Sprintf("%v.%02vm", 2000+int(m/12), int(m)%12)
}

// kdb minute type
type Minute time.Time

func (m Minute) String() string {
	time := time.Time(m)
	return fmt.Sprintf("%02v:%02v", time.Hour(), time.Minute())

}

// kdb second hh:mm:ss
type Second time.Time

func (s Second) String() string {
	time := time.Time(s)
	return fmt.Sprintf("%02v:%02v:%02v", time.Hour(), time.Minute(), time.Second())
}

// kdb time hh:mm:ss.SSS
type Time time.Time

func (t Time) String() string {
	time := time.Time(t)
	return fmt.Sprintf("%02v:%02v:%02v.%03v", time.Hour(), time.Minute(), time.Second(), int(time.Nanosecond()/1000000))
}

// Table
type Table struct {
	Columns []string
	Data    []*K
}

func NewTable(cols []string, data []*K) *K {
	return &K{XT, NONE, Table{cols, data}}
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
// Key and Value should be slices of the same length
type Dict struct {
	Key   *K
	Value *K
}

func NewDict(k, v *K) *K {
	return &K{XD, NONE, Dict{k, v}}
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

// unmarshall dict to struct
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

// Unmarshall Q dictionary to map[string]{}interface
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

// Struct that represents q function
type Function struct {
	Namespace string
	Body      string
}
