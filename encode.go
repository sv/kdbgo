package kdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"strconv"
)

func writeData(dbuf io.Writer, order binary.ByteOrder, data *K) (err error) {
	switch data.Type {
	case K0:
		tosend := data.Data.([]*K)
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, NONE) // attributes
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
		binary.Write(dbuf, order, NONE) // attributes
		binary.Write(dbuf, order, int32(len(tosend)))
		binary.Write(dbuf, order, []byte(tosend))
	case KS:
		tosend := data.Data.([]string)
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, NONE) // attributes
		binary.Write(dbuf, order, int32(len(tosend)))
		for i := 0; i < len(tosend); i++ {
			binary.Write(dbuf, order, []byte(tosend[i]))
			binary.Write(dbuf, order, byte(0))
		}
	case -KB:
		tosend := data.Data.(bool)
		binary.Write(dbuf, order, int8(data.Type))
		var val byte
		if tosend {
			val = 0x01
		} else {
			val = 0x00
		}
		binary.Write(dbuf, order, val)
	case -KI, -KJ, -KE, -KF:
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, data.Data)
	case KG, KI, KJ, KE, KF, KZ, KT, KD, KV, KU, KM, KP, KN:
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, NONE) // attributes
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
		binary.Write(dbuf, order, NONE) // attributes
		err = writeData(dbuf, order, &K{XD, NONE, Dict{&K{KS, NONE, tosend.Columns}, &K{K0, NONE, tosend.Data}}})
		if err != nil {
			return err
		}
	case KERR:
		tosend := data.Data.(error)
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, []byte(tosend.Error()))
		binary.Write(dbuf, order, byte(0))
	case KFUNC:
		tosend := data.Data.(Function)
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, []byte(tosend.Namespace))
		binary.Write(dbuf, order, byte(0))
		err = writeData(dbuf, order, &K{KC, NONE, tosend.Body})
		if err != nil {
			return err
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

/*
func compress(b []byte) (dst []byte) {
	i := byte(0)
	var g int32
	//j := int32(len(b))
	f, h0, h, g, head := int32(0), int32(0), int32(0), int32(0), int32(17)
	dst = make([]byte, len(b)/2)
	c := 12
	d := c
	e := len(dst)
	p := 0
	q, r, s0 := 0, 0, 0
	s := int32(8)
	t := int32(len(b))
	a := make([]int32, 256)
	copy(dst[:4], b[:4])
	dst[2] = 1
	//dst[8:]=[]byte(strconv.Itoa(int(j)))
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

		h = int(b[s] ^ b[s+1])
		p = a[h]
		g = (s > t-3) || (0 == p) || (0 != (b[s] ^ b[p]))

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
			for ; b[p] == b[s] && s < q; s++ {
				p++
			}
			dst[d] = byte(h)
			d++
			dst[d] = byte(s - r)
			d++
		}
	}
	dst[c] = byte(f)
	//dst[4:8]=d
	return dst
}
*/
// Encode data to ipc format as msgtype(sync/async/response) to specified writer
func Encode(w io.Writer, msgtype int, data *K) (err error) {
	var order = binary.LittleEndian
	dbuf := new(bytes.Buffer)
	err = writeData(dbuf, order, data)
	if err != nil {
		return err
	}
	msglen := uint32(8 + dbuf.Len())
	var header = ipcHeader{1, byte(msgtype), 0, 0, msglen}
	buf := new(bytes.Buffer)
	err = binary.Write(buf, order, header)
	err = binary.Write(buf, order, dbuf.Bytes())
	_, err = w.Write(buf.Bytes())
	return err
}
