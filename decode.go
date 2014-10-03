package kdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"time"
	"unsafe"

	"github.com/golang/glog"
	"github.com/nu7hatch/gouuid"
)

var typeSize = map[int8]int{
	1: 1, 4: 1, 10: 1,
	2: 16,
	5: 2,
	6: 4, 8: 4, 14: 4, 17: 4, 18: 4, 19: 4,
	7: 8, 9: 8, 15: 8}

var typeReflect = map[int8]reflect.Type{
	6:  reflect.TypeOf([]int32{}),
	7:  reflect.TypeOf([]int64{}),
	8:  reflect.TypeOf([]float32{}),
	9:  reflect.TypeOf([]float64{}),
	14: reflect.TypeOf([]int32{}),
	15: reflect.TypeOf([]float64{}),
	17: reflect.TypeOf([]int32{}),
	18: reflect.TypeOf([]int32{}),
	19: reflect.TypeOf([]int32{})}

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

// Decodes data from src in q ipc format.
func Decode(src *bufio.Reader) (data *K, msgtype int, e error) {
	var header ipcHeader
	e = binary.Read(src, binary.LittleEndian, &header)
	if e != nil {
		glog.Errorln("Failed to read message header:", e)
		return nil, -1, e
	}
	glog.V(1).Infoln("Header -> ", header)
	// try to buffer entire message in one go
	src.Peek(int(header.MsgSize - 8))

	var order = header.getByteOrder()
	if header.Compressed == 0x01 {
		glog.V(1).Infoln("Decoding compressed data. Size = ", header.MsgSize)
		compressed := make([]byte, header.MsgSize-8)
		glog.V(1).Infoln("Filling buffer", len(compressed))
		_, e = io.ReadFull(src, compressed)
		if e != nil {
			glog.Errorln("Decode:readcompressed error - ", e)
			return nil, int(header.RequestType), e
		}
		glog.V(1).Infoln("Uncompressing...")
		var uncompressed = uncompress(compressed)
		glog.V(1).Infoln("Done.")
		glog.V(2).Infoln(uncompressed[8:1000])
		var buf = bufio.NewReader(bytes.NewReader(uncompressed[8:]))
		glog.V(1).Infoln("Decoding data")
		data, e = readData(buf, order)
		return data, int(header.RequestType), e
	}
	data, e = readData(src, order)
	glog.V(1).Infoln("Object decoded", e)
	glog.V(1).Infoln("buffered = ", src.Buffered())
	return data, int(header.RequestType), e
}

func readData(r *bufio.Reader, order binary.ByteOrder) (kobj *K, err error) {
	var msgtype int8
	err = binary.Read(r, order, &msgtype)
	if err != nil {
		glog.Errorln("readData:msgtype", err)
		return nil, err
	}
	glog.V(1).Infoln("Msg Type:", msgtype)
	switch msgtype {
	case -KB:
		var b byte
		binary.Read(r, order, &b)
		return &K{msgtype, NONE, b != 0x0}, nil

	case -UU:
		var u uuid.UUID
		binary.Read(r, order, &u)
		return &K{msgtype, NONE, u}, nil

	case -KG:
		var b byte
		binary.Read(r, order, &b)
		return &K{msgtype, NONE, b}, nil
	case -KH:
		var sh int16
		binary.Read(r, order, &sh)
		return &K{msgtype, NONE, sh}, nil

	case -KI:
		var i int32
		binary.Read(r, order, &i)
		return &K{msgtype, NONE, i}, nil
	case -KJ:
		var j int64
		binary.Read(r, order, &j)
		return &K{msgtype, NONE, j}, nil
	case -KE:
		var e float32
		binary.Read(r, order, &e)
		return &K{msgtype, NONE, e}, nil
	case -KF:
		var f float64
		binary.Read(r, order, &f)
		return &K{msgtype, NONE, f}, nil
	case -KC:
		var c byte
		binary.Read(r, order, &c)
		return &K{msgtype, NONE, c}, nil // should be rune?
	case -KS:
		line, err := r.ReadBytes(0)
		if err != nil {
			return nil, err
		}
		str := string(line[:len(line)-1])

		return &K{msgtype, NONE, str}, nil
	case -KP:
		var ts time.Duration
		binary.Read(r, order, &ts)
		return &K{msgtype, NONE, ts}, nil
	case -KM:
		var m Month
		binary.Read(r, order, &m)
		return &K{msgtype, NONE, m}, nil
	case -KN:
		var span time.Duration
		binary.Read(r, order, &span)
		return &K{msgtype, NONE, span}, nil
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
		var arr interface{}
		if msgtype >= 6 && msgtype <= 9 {
			bytedata := make([]byte, int(veclen)*typeSize[msgtype])
			_, err = io.ReadFull(r, bytedata)
			head := (*reflect.SliceHeader)(unsafe.Pointer(&bytedata))
			head.Len = int(veclen)
			head.Cap = int(veclen)
			arr = reflect.Indirect(reflect.NewAt(typeReflect[msgtype], unsafe.Pointer(&bytedata))).Interface()
		} else {
			arr = makeArray(msgtype, veclen)
			err = binary.Read(r, order, arr)
		}
		if err != nil {
			glog.Errorln("Error during conversion -> ", err)
			return nil, err
		}
		if msgtype == KC {
			return &K{msgtype, vecattr, string(arr.([]byte))}, nil
		}

		if msgtype == 12 {
			arr := arr.([]time.Duration)
			var timearr = make([]time.Time, veclen)
			for i := 0; i < int(veclen); i++ {
				timearr[i] = qEpoch.Add(arr[i])
			}
			return &K{msgtype, vecattr, timearr}, nil
		}

		if msgtype == 14 {
			arr := arr.([]int32)
			var timearr = make([]time.Time, veclen)
			for i := 0; i < int(veclen); i++ {
				d := time.Duration(arr[i]) * 24 * time.Hour
				timearr[i] = qEpoch.Add(d)
			}
			return &K{msgtype, vecattr, timearr}, nil
		}
		if msgtype == 15 {
			arr := arr.([]float64)
			var timearr = make([]time.Time, veclen)
			for i := 0; i < int(veclen); i++ {
				d := time.Duration(86400000*arr[i]) * time.Millisecond
				timearr[i] = qEpoch.Add(d)
			}
			return &K{msgtype, vecattr, timearr}, nil
		}
		if msgtype == 17 {
			arr := arr.([]int32)
			var timearr = make([]Minute, veclen)
			for i := 0; i < int(veclen); i++ {
				d := time.Duration(arr[i]) * time.Minute
				timearr[i] = Minute(time.Time{}.Add(d))
			}
			return &K{msgtype, vecattr, timearr}, nil
		}
		if msgtype == 18 {
			arr := arr.([]int32)
			var timearr = make([]Second, veclen)
			for i := 0; i < int(veclen); i++ {
				d := time.Duration(arr[i]) * time.Second
				timearr[i] = Second(time.Time{}.Add(d))
			}
			return &K{msgtype, vecattr, timearr}, nil
		}
		if msgtype == 19 {
			arr := arr.([]int32)
			var timearr = make([]Time, veclen)
			for i := 0; i < int(veclen); i++ {
				timearr[i] = Time(qEpoch.Add(time.Duration(arr[i]) * time.Millisecond))
			}
			return &K{msgtype, vecattr, timearr}, nil
		}
		return &K{msgtype, vecattr, arr}, nil
	case K0:
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
		return &K{msgtype, vecattr, arr}, nil
	case KS:
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
		return &K{msgtype, vecattr, arr}, nil
	case 99, 127:
		var res Dict
		dk, err := readData(r, order)
		if err != nil {
			return nil, err
		}
		dv, err := readData(r, order)
		if err != nil {
			return nil, err
		}
		res = Dict{&K{K0, NONE, dk}, &K{K0, NONE, dv}}
		return &K{msgtype, NONE, res}, nil
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
		dict := d.Data.(Dict)
		return &K{msgtype, vecattr, Table{dict.Keys.Data.([]string), dict.Values.Data.([]*K)}}, nil

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
		f.Body = b.Data.(string)
		return &K{msgtype, NONE, f}, nil
	case 101, 102, 103:
		var primitiveidx byte
		err = binary.Read(r, order, &primitiveidx)
		if err != nil {
			return nil, err
		}
		return &K{msgtype, NONE, primitiveidx}, nil
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
		return &K{msgtype, NONE, res}, nil
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
