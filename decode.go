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
	// try to buffer entire message in one go
	src.Peek(int(header.MsgSize - 8))

	var order = header.getByteOrder()
	if header.Compressed == 0x01 {
		compressed := make([]byte, header.MsgSize-8)
		_, e = io.ReadFull(src, compressed)
		if e != nil {
			glog.Errorln("Decode:readcompressed error - ", e)
			return nil, int(header.RequestType), e
		}
		var uncompressed = uncompress(compressed)
		var buf = bufio.NewReader(bytes.NewReader(uncompressed[8:]))
		data, e = readData(buf, order)
		return data, int(header.RequestType), e
	}
	data, e = readData(src, order)
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

	case -KI, -KD, -KU, -KV:
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
	case -KF, -KZ:
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
		return &K{msgtype, NONE, qEpoch.Add(ts)}, nil
	case -KM:
		var m Month
		binary.Read(r, order, &m)
		return &K{msgtype, NONE, m}, nil
	case -KN:
		var span time.Duration
		binary.Read(r, order, &span)
		return &K{msgtype, NONE, span}, nil
	case KB, UU, KG, KH, KI, KJ, KE, KF, KC, KP, KM, KD, KN, KU, KV, KT, KZ:
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
		if msgtype >= KI && msgtype <= KF {
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

		if msgtype == KP {
			arr := arr.([]time.Duration)
			var timearr = make([]time.Time, veclen)
			for i := 0; i < int(veclen); i++ {
				timearr[i] = qEpoch.Add(arr[i])
			}
			return &K{msgtype, vecattr, timearr}, nil
		}

		if msgtype == KD {
			arr := arr.([]int32)
			var timearr = make([]time.Time, veclen)
			for i := 0; i < int(veclen); i++ {
				d := time.Duration(arr[i]) * 24 * time.Hour
				timearr[i] = qEpoch.Add(d)
			}
			return &K{msgtype, vecattr, timearr}, nil
		}
		if msgtype == KZ {
			arr := arr.([]float64)
			var timearr = make([]time.Time, veclen)
			for i := 0; i < int(veclen); i++ {
				d := time.Duration(86400000*arr[i]) * time.Millisecond
				timearr[i] = qEpoch.Add(d)
			}
			return &K{msgtype, vecattr, timearr}, nil
		}
		if msgtype == KU {
			arr := arr.([]int32)
			var timearr = make([]Minute, veclen)
			for i := 0; i < int(veclen); i++ {
				d := time.Duration(arr[i]) * time.Minute
				timearr[i] = Minute(time.Time{}.Add(d))
			}
			return &K{msgtype, vecattr, timearr}, nil
		}
		if msgtype == KV {
			arr := arr.([]int32)
			var timearr = make([]Second, veclen)
			for i := 0; i < int(veclen); i++ {
				d := time.Duration(arr[i]) * time.Second
				timearr[i] = Second(time.Time{}.Add(d))
			}
			return &K{msgtype, vecattr, timearr}, nil
		}
		if msgtype == KT {
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
		var arr = make([]*K, veclen)
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
	case XD, SD:
		var res Dict
		dk, err := readData(r, order)
		if err != nil {
			return nil, err
		}
		dv, err := readData(r, order)
		if err != nil {
			return nil, err
		}
		res = Dict{dk, dv}
		return &K{msgtype, NONE, res}, nil
	case XT:
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
		colNames := dict.Key.Data.([]string)
		colValues := dict.Value.Data.([]*K)
		return &K{msgtype, vecattr, Table{colNames, colValues}}, nil

	case KFUNC:
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
	case KFUNCUP, KFUNCBP, KFUNCTR:
		var primitiveidx byte
		err = binary.Read(r, order, &primitiveidx)
		if err != nil {
			return nil, err
		}
		return &K{msgtype, NONE, primitiveidx}, nil
	case KPROJ, KCOMP:
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
	case KEACH, KOVER, KSCAN, KPRIOR, KEACHRIGHT, KEACHLEFT:
		return readData(r, order)
	case KDYNLOAD:
		// 112 - dynamic load
		return nil, errors.New("type is unsupported")
	case KERR:
		line, err := r.ReadSlice(0)
		if err != nil {
			return nil, err
		}
		errmsg := string(line[:len(line)-1])
		return nil, errors.New(errmsg)
	}
	return nil, ErrBadMsg
}
