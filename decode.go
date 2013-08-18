package kdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/golang/glog"
	"github.com/nu7hatch/gouuid"
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
	Columns []string
	Data    []interface{}
}

type Dict struct {
	Keys   interface{}
	Values interface{}
}

type Function struct {
	Namespace string
	Body      string
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

func uncompress(b []byte) (dst []byte) {
	n := int32(0)
	r := int32(0)
	f := int32(0)
	s := int32(8)
	p := int32(s)
	i := int16(0)
	var usize int32
	binary.Read(bytes.NewReader(b[0:4]), binary.LittleEndian, &usize)
	dst = make([]byte, usize)
	glog.V(1).Infoln("Uncompressed size=", usize)
	d := int32(4)
	aa := make([]int32, 256)
	for int(s) < len(dst) {
		if i == 0 {
			f = 0xff & int32(b[d])
			d++
			i = 1
		}
		if (f & int32(i)) != 0 {
			r = aa[0xff&int32(b[d])]
			d++
			dst[s] = dst[r]
			s++
			r++
			dst[s] = dst[r]
			s++
			r++
			n = 0xff & int32(b[d])
			d++
			for m := int32(0); m < n; m++ {
				dst[s+m] = dst[r+m]
			}
		} else {
			dst[s] = b[d]
			s++
			d++
		}
		for p < s-1 {
			aa[(0xff&int32(dst[p]))^(0xff&int32(dst[p+1]))] = p
			p++
		}
		if (f & int32(i)) != 0 {
			s += n
			p = s
		}
		i *= 2
		if i == 256 {
			i = 0
		}
	}
	return dst
}
func Decode(src *bufio.Reader) (kobj interface{}, e error) {
	var header ipcHeader
	e = binary.Read(src, binary.LittleEndian, &header)
	if e != nil {
		glog.Errorln("binary.Read failed:", e)
		return nil, e
	}
	glog.V(1).Infoln("Header -> ", header)
	if int(header.RequestType) == SYNC {
		return nil, errors.New("Sync request not yet supported")
	}
	var order = header.getByteOrder()
	if header.Compressed == 0x01 {
		glog.V(1).Infoln("Decoding compressed data. Size = ", header.MsgSize)
		compressed := make([]byte, header.MsgSize-8)
		start := 0
		glog.V(1).Infoln("Filling buffer", start, len(compressed))
		for start < int(len(compressed)) {
			glog.V(1).Infoln("Reading bytes = ", len(compressed[start:]))
			n, e := src.Read(compressed[start:])
			if e != nil {
				glog.Errorln("Decode:readcompressed error", e)
				return nil, e
			}
			start += n
		}
		glog.V(1).Infoln("Uncompressing...")
		var uncompressed = uncompress(compressed)
		glog.V(1).Infoln("Done.")
		glog.V(2).Infoln(uncompressed[8:1000])
		var buf = bufio.NewReader(bytes.NewReader(uncompressed[8:]))
		glog.V(1).Infoln("Decoding data")
		return readData(buf, order)
	}
	kobj, e = readData(src, order)
	glog.V(1).Infoln("Object decoded", e)
	glog.V(1).Infoln("buffered = ", src.Buffered())
	return kobj, e
}
func readData(r *bufio.Reader, order binary.ByteOrder) (kobj interface{}, err error) {
	var msgtype int8
	err = binary.Read(r, order, &msgtype)
	if err != nil {
		glog.Errorln("readData:msgtype", err)
		return nil, err
	}
	glog.V(1).Infoln("Msg Type:", msgtype)
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
		err = binary.Read(r, order, &vecattr)
		if err != nil {
			glog.Errorln("readData: Failed to read vecattr", err)
			return nil, err
		}
		var veclen int32
		err = binary.Read(r, order, &veclen)
		if err != nil {
			glog.Errorln("Reading vector length failed -> ", err)
			return nil, err
		}
		var arr = makeArray(msgtype, veclen)
		err = binary.Read(r, order, arr)
		if err != nil {
			glog.Errorln("Error during conversion -> ", err)
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
		err = binary.Read(r, order, &vecattr)
		if err != nil {
			glog.Errorln("readData: Failed to read vecattr", err)
			return nil, err
		}
		var veclen int32
		err = binary.Read(r, order, &veclen)
		if err != nil {
			glog.Errorln("Reading vector length failed -> ", err)
			return nil, err
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
		err = binary.Read(r, order, &vecattr)
		if err != nil {
			glog.Errorln("readData: Failed to read vecattr", err)
			return nil, err
		}
		var veclen int32
		err = binary.Read(r, order, &veclen)
		if err != nil {
			glog.Errorln("Reading vector length failed -> ", err)
			return nil, err
		}
		var arr = makeArray(msgtype, veclen).([]string)
		for i := 0; i < int(veclen); i++ {
			line, err := r.ReadSlice(0)
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
		err = binary.Read(r, order, &vecattr)
		if err != nil {
			glog.Errorln("readData: Failed to read vecattr", err)
			return nil, err
		}
		d, err := readData(r, order)
		if err != nil {
			return nil, err
		}
		dict := d.(Dict)
		return Table{dict.Keys.([]string), dict.Values.([]interface{})}, nil

	case 100:
		var f Function
		line, err := r.ReadSlice(0)
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
	case 101, 102, 103:
		var primitiveidx byte
		err = binary.Read(r, order, &primitiveidx)
		if err != nil {
			return nil, err
		}
		return primitiveidx, nil
	case 104, 105:
		// 104 - projection
		// 105 - composition
		var n int32
		err = binary.Read(r, order, &n)
		var res = make([]interface{}, n)
		for i := 0; i < int(n); i++ {
			res[i], err = readData(r, order)
			if err != nil {
				return nil, err
			}
		}
		return res, nil
	case 106, 107, 108, 109, 110, 111:
		// 106 - f'
		// 107 - f/
		// 108 - f\
		// 109 - f':
		// 110 - f/:
		// 111 - f\:
		return readData(r, order)
	case 112:
		// 112 - dynamic load
		return nil, errors.New("type is unsupported")
	case -128:
		line, err := r.ReadSlice(0)
		if err != nil {
			return nil, err
		}
		errmsg := string(line[:len(line)-1])
		return nil, errors.New(errmsg)
	}
	return nil, ErrBadMsg
}
