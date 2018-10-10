package kdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"strconv"
	"time"
)

func writeData(dbuf io.Writer, order binary.ByteOrder, data *K) (err error) {
	switch data.Type {
	case K0:
		tosend := data.Data.([]*K)
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, data.Attr) // attributes
		binary.Write(dbuf, order, int32(len(tosend)))
		for i := 0; i < len(tosend); i++ {
			err = writeData(dbuf, order, tosend[i])
			if err != nil {
				return err
			}
		}
	case -KS:
		tosend := data.Data.(string)

		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, []byte(tosend))
		binary.Write(dbuf, order, byte(0))
	case KC:
		tosend := data.Data.(string)

		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, data.Attr) // attributes
		binary.Write(dbuf, order, int32(len(tosend)))
		binary.Write(dbuf, order, []byte(tosend))
	case KS:
		tosend := data.Data.([]string)
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, data.Attr) // attributes
		binary.Write(dbuf, order, int32(len(tosend)))
		for i := 0; i < len(tosend); i++ {
			binary.Write(dbuf, order, []byte(tosend[i]))
			binary.Write(dbuf, order, byte(0))
		}
	case -KB, -KG, -KI, -KJ, -KE, -KF, -KU, -KV, -KT, -KM, -KN, -KH, -UU, -KC,
		KFUNCUP, KFUNCBP, KFUNCTR:
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, data.Data)
	case -KP:
		tosend := data.Data.(time.Time)
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, tosend.Sub(qEpoch))
	case -KZ:
		tosend := data.Data.(time.Time)
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, -1*(float64(qEpoch.Sub(tosend)/time.Millisecond)/86400000))
	case -KD:
		tosend := data.Data.(time.Time)
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, -1*(int32(qEpoch.Sub(tosend)/time.Hour)/24))
	case KP:
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, data.Attr) // attributes
		binary.Write(dbuf, order, int32(reflect.ValueOf(data.Data).Len()))
		tosend := data.Data.([]time.Time)
		for _, ts := range tosend {
			binary.Write(dbuf, order, ts.Sub(qEpoch))
		}
	case KZ:
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, data.Attr) // attributes
		binary.Write(dbuf, order, int32(reflect.ValueOf(data.Data).Len()))
		tosend := data.Data.([]time.Time)
		for _, ts := range tosend {
			binary.Write(dbuf, order, -1*(float64(qEpoch.Sub(ts)/time.Millisecond)/86400000))
		}
	case KD:
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, data.Attr) // attributes
		binary.Write(dbuf, order, int32(reflect.ValueOf(data.Data).Len()))
		tosend := data.Data.([]time.Time)
		for _, ts := range tosend {
			binary.Write(dbuf, order, -1*(int32(qEpoch.Sub(ts)/time.Hour)/24))
		}
	case KB:
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, data.Attr) // attributes
		binary.Write(dbuf, order, int32(reflect.ValueOf(data.Data).Len()))
		tosend := data.Data.([]bool)
		boolmap := map[bool]byte{false: 0x00, true: 0x01}
		for _, b := range tosend {
			binary.Write(dbuf, order, boolmap[b])
		}
	case KG, KI, KJ, KE, KF, KU, KV, KT, KM, KN, KH, UU:
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, data.Attr) // attributes
		binary.Write(dbuf, order, int32(reflect.ValueOf(data.Data).Len()))
		binary.Write(dbuf, order, data.Data)
	case XD:
		tosend := data.Data.(Dict)
		binary.Write(dbuf, order, XD)
		err = writeData(dbuf, order, tosend.Key)
		if err != nil {
			return err
		}
		err = writeData(dbuf, order, tosend.Value)
		if err != nil {
			return err
		}
	case XT:
		tosend := data.Data.(Table)
		binary.Write(dbuf, order, XT)
		binary.Write(dbuf, order, data.Attr) // attributes
		err = writeData(dbuf, order, &K{XD, NONE, Dict{&K{KS, NONE, tosend.Columns}, &K{K0, NONE, tosend.Data}}})
		if err != nil {
			return err
		}
	case KERR:
		tosend := data.Data.(error)
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, []byte(tosend.Error()))
		binary.Write(dbuf, order, byte(0))
	case KFUNC, KOVER, KSCAN, KEACH, KPRIOR, KEACHRIGHT, KEACHLEFT:
		tosend := data.Data.(Function)
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, []byte(tosend.Namespace))
		binary.Write(dbuf, order, byte(0))
		err = writeData(dbuf, order, &K{KC, NONE, tosend.Body})
		if err != nil {
			return err
		}
	case KPROJ, KCOMP:
		tosend := data.Data.([]interface{})
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, int32(len(tosend)))
		for i := 0; i < len(tosend); i++ {
			err = writeData(dbuf, order, &K{tosend[i].(*K).Type, NONE, tosend[i].(*K).Data})
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("unknown type " + strconv.Itoa(int(data.Type)))
	}
	return nil

}

func min32(a, b int32) int32 {
	if a > b {
		return b
	}
	return a
}

// Compress b using Q IPC compression
func Compress(b []byte) (dst []byte) {
	if len(b) <= 17 {
		return b
	}
	i := byte(0)
	//j := int32(len(b))
	f, h0, h := int32(0), int32(0), int32(0)
	g := false
	dst = make([]byte, len(b)/2)
	lenbuf := make([]byte, 4)
	c := 12
	d := c
	e := len(dst)
	p := 0
	q, r, s0 := int32(0), int32(0), int32(0)
	s := int32(8)
	t := int32(len(b))
	a := make([]int32, 256)
	copy(dst[:4], b[:4])
	dst[2] = 1
	binary.LittleEndian.PutUint32(lenbuf, uint32(len(b)))
	copy(dst[8:], lenbuf)
	for ; s < t; i *= 2 {
		if 0 == i {
			if d > e-17 {
				return b
			}
			i = 1
			dst[c] = byte(f)
			c = d
			d++
			f = 0
		}

		g = (s > t-3)
		if !g {
			h = int32(0xff & (b[s] ^ b[s+1]))
			p = int(a[h])
			g = (0 == p) || (0 != (b[s] ^ b[p]))
		}

		if 0 < s0 {
			a[h0] = s0
			s0 = 0
		}
		if g {
			h0 = h
			s0 = s
			dst[d] = b[s]
			d++
			s++
		} else {
			a[h] = s
			f |= int32(i)
			p += 2
			s += 2
			r = s
			q = min32(s+255, t)
			for ; b[p] == b[s] && s+1 < q; s++ {
				p++
			}
			dst[d] = byte(h)
			d++
			dst[d] = byte(s - r)
			d++
		}
	}
	dst[c] = byte(f)
	binary.LittleEndian.PutUint32(lenbuf, uint32(d))
	copy(dst[4:], lenbuf)
	return dst[:d:d]
}

func EncodeRaw(msgtype int, data *K) ([]byte, error) {
	var order = binary.LittleEndian
	dbuf := new(bytes.Buffer)
	err := writeData(dbuf, order, data)
	if err != nil {
		return nil, err
	}
	msglen := uint32(8 + dbuf.Len())
	var header = ipcHeader{1, byte(msgtype), 0, 0, msglen}
	buf := new(bytes.Buffer)
	err = binary.Write(buf, order, header)
	err = binary.Write(buf, order, dbuf.Bytes())
	return buf.Bytes(), nil
}

// Encode data to ipc format as msgtype(sync/async/response) to specified writer
func Encode(w io.Writer, msgtype int, data *K) (err error) {
	buf, err := EncodeRaw(msgtype, data)
	if err != nil {
		return nil
	}
	_, err = w.Write(Compress(buf))
	//_, err = w.Write(buf.Bytes())
	return err
}
