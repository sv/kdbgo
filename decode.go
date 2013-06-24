package kdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"io"
	"time"
)

const (
	ASYNC    int = 0
	SYNC     int = 1
	RESPONSE int = 2
)

type Attr int8

const (
	NONE Attr = iota
	SORTED
	UNIQUE
	PARTED
	GROUPED
)

var ErrBadMsg = errors.New("Bad Message")
var ErrBadHeader = errors.New("Bad header")

var QEpoch = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

type Month int32
type Minute time.Time
type Second time.Time
type Time time.Time

type Table struct {
	columns []string
	data    []interface{}
}

type Dict struct {
	Keys   interface{}
	Values interface{}
}

func makeArray(vectype int8, veclen int32) interface{} {
	switch vectype {
	case 1, 4, 10:
		return make([]byte, veclen)
	case 2:
		return make([]uuid.UUID, veclen)
	case 5:
		return make([]int16, veclen)
	case 6, 14, 17, 18, 19:
		return make([]int32, veclen)
	case 13:
		return make([]Month, veclen)
	case 16, 12:
		return make([]time.Duration, veclen)
	case 7:
		return make([]int64, veclen)
	case 8:
		return make([]float32, veclen)
	case 9, 15:
		return make([]float64, veclen)
	case 11:
		return make([]string, veclen)
	}

	return nil
}

type ipcHeader struct {
	ByteOrder   byte
	RequestType byte
	Compressed  byte
	Reserved    byte
	MsgSize     int32
}

func (h *ipcHeader) getByteOrder() binary.ByteOrder {
	var order binary.ByteOrder
	order = binary.LittleEndian
	if h.ByteOrder == 0x00 {
		order = binary.BigEndian
	}
	return order
}
func Decode(src io.Reader) (kobj interface{}, e error) {
	var r = bufio.NewReader(src)
	var header ipcHeader
	//fmt.Println("Reading header")
	err := binary.Read(r, binary.LittleEndian, &header)
	if err != nil {
		fmt.Println("binary.Read failed:", err)
	}
	//fmt.Println("Header -> ", header)
	var order = header.getByteOrder()
	return readData(r, order)
}
func readData(r *bufio.Reader, order binary.ByteOrder) (kobj interface{}, err error) {
	var msgtype int8
	//var msglen = header.MsgSize
	binary.Read(r, order, &msgtype)
	//fmt.Println("Msg Type:", msgtype)
	switch msgtype {
	case -1:
		var b byte
		binary.Read(r, order, &b)
		return b != 0x0, nil

	case -2:
		var u uuid.UUID
		binary.Read(r, order, &u)
		return u, nil

	case -4:
		var b byte
		binary.Read(r, order, &b)
		return b, nil
	case -5:
		var sh int16
		binary.Read(r, order, &sh)
		return sh, nil

	case -6:
		var i int32
		binary.Read(r, order, &i)
		return i, nil
	case -7:
		var j int64
		binary.Read(r, order, &j)
		return j, nil
	case -8:
		var e float32
		binary.Read(r, order, &e)
		return e, nil
	case -9:
		var f float64
		binary.Read(r, order, &f)
		return f, nil
	case -10:
		var c byte
		binary.Read(r, order, &c)
		return c, nil // should be rune?
	case -11:
		line, err := r.ReadBytes(0)
		if err != nil {
			return nil, err
		}
		str := string(line[:len(line)-1])

		return str, nil
	case -12:
		var ts time.Duration
		binary.Read(r, order, &ts)
		return ts, nil
	case -13:
		var m Month
		binary.Read(r, order, &m)
		return m, nil
	case -16:
		var span time.Duration
		binary.Read(r, order, &span)
		return span, nil
	case -14, -15, -17, -18, -19:
		return nil, errors.New("NotImplemetedYet")
	case 1, 2, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19:
		var vecattr Attr
		binary.Read(r, order, &vecattr)
		//fmt.Println("vecattr -> ", vecattr)
		var veclen int32
		err = binary.Read(r, order, &veclen)
		if err != nil {
			fmt.Println("Reading vector length failed -> %v", err)
		}
		var arr = makeArray(msgtype, veclen)
		err = binary.Read(r, order, arr)
		if err != nil {
			fmt.Println("Error during conversion -> ", err)
			return nil, err
		}
		if msgtype == 10 {
			return string(arr.([]byte)), nil
		}

		if msgtype == 12 {
			arr := arr.([]time.Duration)
			var timearr = make([]time.Time, veclen)
			for i := 0; i < int(veclen); i++ {
				timearr[i] = QEpoch.Add(arr[i])
			}
			return timearr, nil
		}
		if msgtype == 19 {
			arr := arr.([]int32)
			var timearr = make([]Time, veclen)
			for i := 0; i < int(veclen); i++ {
				timearr[i] = Time(QEpoch.Add(time.Duration(arr[i]) * time.Millisecond))
			}
			return timearr, nil
		}
		return arr, nil
	case 0:
		var vecattr Attr
		binary.Read(r, order, &vecattr)
		var veclen int32
		err = binary.Read(r, order, &veclen)
		if err != nil {
			fmt.Println("Reading vector length failed -> %v", err)
		}
		var arr = make([]interface{}, veclen)
		for i := 0; i < int(veclen); i++ {
			v, err := readData(r, order)
			if err != nil {
				return nil, err
			}
			arr[i] = v
		}
		return arr, nil
	case 11:
		var vecattr Attr
		binary.Read(r, order, &vecattr)
		var veclen int32
		err = binary.Read(r, order, &veclen)
		if err != nil {
			fmt.Println("Reading vector length failed -> %v", err)
		}
		var arr = makeArray(msgtype, veclen).([]string)
		for i := 0; i < int(veclen); i++ {
			line, err := r.ReadBytes(0)
			if err != nil {
				return nil, err
			}
			arr[i] = string(line[:len(line)-1])
		}
		return arr, nil
	case 99, 127:
		k, err := readData(r, order)
		if err != nil {
			return nil, err
		}
		v, err := readData(r, order)
		if err != nil {
			return nil, err
		}
		return Dict{k, v}, nil
	case 98:
		var vecattr Attr
		binary.Read(r, order, &vecattr)
		d, err := readData(r, order)
		if err != nil {
			return nil, err
		}
		dict := d.(Dict)
		return Table{dict.Keys.([]string), dict.Values.([]interface{})}, nil

	case 100:
		var f struct{ Namespace, Body string }
		line, err := r.ReadBytes(0)
		if err != nil {
			return nil, err
		}
		f.Namespace = string(line[:len(line)-1])
		b, err := readData(r, order)
		if err != nil {
			return nil, err
		}
		f.Body = b.(string)
		return f, nil
	case -128:
		line, err := r.ReadBytes(0)
		if err != nil {
			return nil, err
		}
		errmsg := string(line[:len(line)-1])
		return nil, errors.New(errmsg)
	}
	return nil, ErrBadMsg
}
