package kdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"time"
)

// TODO: Handle all the errors returned by `Write` calls
// To read more about Qipc protocol, see https://code.kx.com/wiki/Reference/ipcprotocol
// Negative types are scalar and positive ones are vector. 0 is mixed list
func writeData(dbuf *bytes.Buffer, order binary.ByteOrder, k *K) error {
	binary.Write(dbuf, order, k.Type)
	switch {
	case K0 <= k.Type && k.Type <= KT:
		// For all vector types, write the attribute (s,u,p,g OR none) & length of the vector
		binary.Write(dbuf, order, k.Attr)
		binary.Write(dbuf, order, int32(reflect.ValueOf(k.Data).Len()))
	case k.Type == XT:
		// For table, only, write the attribute
		binary.Write(dbuf, order, k.Attr)
	}

	switch k.Type {
	case K0: // Mixed List
		for _, k := range k.Data.([]*K) {
			if err := writeData(dbuf, order, k); err != nil {
				return err
			}
		}
	case -KB, -UU, -KG, -KH, -KI, -KJ, -KE, -KF, -KC, -KM, -KZ, -KN, -KU, -KV,
		KB, UU, KG, KH, KI, KJ, KE, KF, KM, KZ, KN, KU, KV: // Bool, Int, Float, and Byte
		// Note: UUID is backed by byte array of length 16
		binary.Write(dbuf, order, k.Data)
	case KC: // String
		dbuf.WriteString(k.Data.(string))
	case -KS: // Symbol
		dbuf.WriteString(k.Data.(string))
		binary.Write(dbuf, order, byte(0)) // Null terminator
	case KS: // Symbol
		for _, symbol := range k.Data.([]string) {
			dbuf.WriteString(symbol)
			binary.Write(dbuf, order, byte(0)) // Null terminator
		}
	case -KP: // Timestamp
		binary.Write(dbuf, order, k.Data.(time.Time).Sub(qEpoch))
	case KP: // Timestamp
		for _, ts := range k.Data.([]time.Time) {
			binary.Write(dbuf, order, ts.Sub(qEpoch))
		}
	case -KD: // Date
		date := k.Data.(time.Time)
		days := (date.Truncate(time.Hour * 24).Unix() - qEpoch.Unix()) / 86400
		binary.Write(dbuf, order, int32(days))
	case KD: // Date
		for _, date := range k.Data.([]time.Time) {
			days := (date.Truncate(time.Hour * 24).Unix() - qEpoch.Unix()) / 86400
			binary.Write(dbuf, order, int32(days))
		}
	case -KT: // Time
		t := k.Data.(time.Time)
		nanos := time.Duration(t.Hour())*time.Hour +
			time.Duration(t.Minute())*time.Minute +
			time.Duration(t.Second())*time.Second +
			time.Duration(t.Nanosecond())
		binary.Write(dbuf, order, int32(nanos/time.Millisecond))
	case KT: // Time
		for _, t := range k.Data.([]time.Time) {
			nanos := time.Duration(t.Hour())*time.Hour +
				time.Duration(t.Minute())*time.Minute +
				time.Duration(t.Second())*time.Second +
				time.Duration(t.Nanosecond())
			binary.Write(dbuf, order, int32(nanos/time.Millisecond))
		}
	case XD: // Dictionary
		dict := k.Data.(Dict)
		err := writeData(dbuf, order, dict.Key)
		if err != nil {
			return err
		}
		err = writeData(dbuf, order, dict.Value)
		if err != nil {
			return err
		}
	case XT: // Table
		table := k.Data.(Table)
		err := writeData(dbuf, order, NewDict(SymbolV(table.Columns), Enlist(table.Data...)))
		if err != nil {
			return err
		}
	case KERR:
		err := k.Data.(error)
		dbuf.WriteString(err.Error())
		binary.Write(dbuf, order, byte(0)) // Null terminator
	case KFUNC:
		fn := k.Data.(Function)
		dbuf.WriteString(fn.Namespace)
		binary.Write(dbuf, order, byte(0)) // Null terminator
		err := writeData(dbuf, order, String(fn.Body))
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("encode: unsupported type: %d", k.Type)
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
	_, err = w.Write(Compress(buf.Bytes()))
	return err
}
