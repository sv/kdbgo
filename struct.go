package kdb

import (
	"errors"
	"fmt"
	"time"
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

	KN int8 = 16 // 8 timespan  long   kJ (nanoseconds)
	KU int8 = 17 // 4 minute    int    kI
	KV int8 = 18 // 4 second    int    kI
	KT int8 = 19 // 4 time      int    kI (millisecond)
	KZ int8 = 15 // 8 datetime  double kF (DO NOT USE)

	// table,dict
	XT int8 = 98 //   x->k is XD
	XD int8 = 99 //   kK(x)[0] is keys. kK(x)[1] is values.

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
	MsgSize     int32
}

//const Nh int16 = 0xFFFF8000
const Wh int16 = 0x7FFF

//const Ni int32 = 0x80000000
const Wi int32 = 0x7FFFFFFF

//const Nj int64 = 0x8000000000000000
const Wj int64 = 0x7FFFFFFFFFFFFFFF

//const Nf float64 = (0 / 0.0)
//const Wf float64 = (1 / 0.0)

type k struct {
	Type int8
	Attr Attr
	Data interface{}
}

type K struct {
	*k
}

// message is malformated or invalid
var ErrBadMsg = errors.New("Bad Message")

// msg header is invalid
var ErrBadHeader = errors.New("Bad header")

// Cannot process sync requests
var ErrSyncRequest = errors.New("nosyncrequest")

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

// kdb second
type Second time.Time

func (s Second) String() string {
	time := time.Time(s)
	return fmt.Sprintf("%02v:%02v:%02v", time.Hour(), time.Minute(), time.Second())
}

// kdb time
type Time time.Time

func (t Time) String() string {
	time := time.Time(t)
	return fmt.Sprintf("%02v:%02v:%02v.%03v", time.Hour(), time.Minute(), time.Second(), int(time.Nanosecond()/1000000))
}

// Table
type Table struct {
	Columns []string
	Data    []K
}

// Dictionary: ordered key->value mapping
type Dict struct {
	Keys   K
	Values K
}

func (d Dict) String() string {
	return fmt.Sprintf("%v!%v", d.Keys, d.Values)
}

// Struct that represents q function
type Function struct {
	Namespace string
	Body      string
}
