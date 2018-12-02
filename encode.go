package kdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"time"
)

// Encode data to ipc format as msgtype(sync/async/response) to specified writer
func Encode(w io.Writer, msgtype int, k *K) (err error) {
	writer := &errWriter{
		buff:  new(bytes.Buffer),
		order: binary.LittleEndian,
		err:   nil,
	}
	encode(writer, k)
	if writer.err != nil {
		return err
	}

	msglen := uint32(8 + writer.buff.Len())
	var header = ipcHeader{1, byte(msgtype), 0, 0, msglen}
	buf := new(bytes.Buffer)
	err = binary.Write(buf, writer.order, header)
	err = binary.Write(buf, writer.order, writer.buff.Bytes())
	_, err = w.Write(Compress(buf.Bytes()))
	return err
}

type errWriter struct {
	buff  *bytes.Buffer
	order binary.ByteOrder
	err   error
}

func (ew *errWriter) write(v interface{}) {
	if ew.err != nil {
		return
	}

	switch x := v.(type) {
	case string:
		_, ew.err = ew.buff.WriteString(x)
	default:
		ew.err = binary.Write(ew.buff, ew.order, v)
	}
}

var null = byte(0)

// To read more about Qipc protocol, see https://code.kx.com/wiki/Reference/ipcprotocol
// Negative types are scalar and positive ones are vector. 0 is mixed list
func encode(w *errWriter, k *K) {
	w.write(k.Type)
	switch {
	case K0 <= k.Type && k.Type <= KT:
		// For all vector types, write the attribute (s,u,p,g OR none) & length of the vector
		w.write(k.Attr)
		w.write(int32(reflect.ValueOf(k.Data).Len()))
	case k.Type == XT:
		// For table, only, write the attribute
		w.write(k.Attr)
	}

	switch k.Type {
	case K0: // Mixed List
		for _, k := range k.Data.([]*K) {
			encode(w, k)
		}
	case -KB, -UU, -KG, -KH, -KI, -KJ, -KE, -KF, -KC, -KM, -KZ, -KN, -KU, -KV,
		+KB, +UU, +KG, +KH, +KI, +KJ, +KE, +KF, +KC, +KM, +KZ, +KN, +KU, +KV: // Bool(s), Int(s), Float(s), and Byte(s), String
		// Note: UUID is backed by byte array of length 16
		w.write(k.Data)
	case -KS: // Symbol
		w.write(k.Data)
		w.write(null)
	case +KS: // Symbol(s)
		for _, symbol := range k.Data.([]string) {
			w.write(symbol)
			w.write(null)
		}
	case -KP: // Timestamp
		w.write(toQTimestamp(k.Data.(time.Time)))
	case +KP: // Timestamp(s)
		for _, ts := range k.Data.([]time.Time) {
			w.write(toQTimestamp(ts))
		}
	case -KD: // Date
		w.write(toQDate(k.Data.(time.Time)))
	case +KD: // Date(s)
		for _, date := range k.Data.([]time.Time) {
			w.write(toQDate(date))
		}
	case -KT: // Time
		w.write(toQTime(k.Data.(time.Time)))
	case +KT: // Time(s)
		for _, t := range k.Data.([]time.Time) {
			w.write(toQTime(t))
		}
	case XD:
		dict := k.Data.(Dict)
		encode(w, dict.Key)
		encode(w, dict.Value)
	case XT:
		table := k.Data.(Table)
		encode(w, NewDict(SymbolV(table.Columns), Enlist(table.Data...)))
	case KFUNC:
		fn := k.Data.(Function)
		w.write(fn.Namespace)
		w.write(null)
		encode(w, String(fn.Body))
	case KERR:
		err := k.Data.(error)
		w.write(err.Error())
		w.write(null)
	default:
		w.err = fmt.Errorf("encode: unsupported type: %d", k.Type)
	}
}

func toQTimestamp(t time.Time) int64 {
	return int64(t.Sub(qEpoch))
}

func toQDate(t time.Time) int32 {
	secs := t.Truncate(time.Hour * 24).Unix() - qEpoch.Unix()
	return int32(secs / 86400)
}

func toQTime(t time.Time) int32 {
	nanos := time.Duration(t.Hour())*time.Hour +
		time.Duration(t.Minute())*time.Minute +
		time.Duration(t.Second())*time.Second +
		time.Duration(t.Nanosecond())
	return int32(nanos / time.Millisecond)
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
