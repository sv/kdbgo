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
	t := data.Type
	if t == XD && data.Attr == SORTED {
		t = SD
	}
	binary.Write(dbuf, order, t)
	if t >= K0 && t < XD {
		binary.Write(dbuf, order, data.Attr) // attributes
		if t != XT {
			binary.Write(dbuf, order, int32(reflect.ValueOf(data.Data).Len()))
		}
	}
	switch data.Type {
	case K0:
		tosend := data.Data.([]*K)
		for i := 0; i < len(tosend); i++ {
			err = writeData(dbuf, order, tosend[i])
			if err != nil {
				return err
			}
		}
	case -KS:
		tosend := data.Data.(string)
		binary.Write(dbuf, order, []byte(tosend))
		binary.Write(dbuf, order, byte(0))
	case KC:
		tosend := data.Data.(string)
		binary.Write(dbuf, order, []byte(tosend))
	case KS:
		tosend := data.Data.([]string)
		for i := 0; i < len(tosend); i++ {
			binary.Write(dbuf, order, []byte(tosend[i]))
			binary.Write(dbuf, order, byte(0))
		}
	case -KB, -KC, -KG, -KI, -KJ, -KE, -KF, -UU:
		binary.Write(dbuf, order, data.Data)
	case -KP:
		tosend := data.Data.(time.Time)
		binary.Write(dbuf, order, tosend.Sub(qEpoch))
	case -KZ:
		tosend := data.Data.(time.Time)
		binary.Write(dbuf, order, -1*(float64(qEpoch.Sub(tosend)/time.Millisecond)/86400000))
	case -KD:
		tosend := data.Data.(time.Time)
		binary.Write(dbuf, order, -1*int32(qEpoch.Sub(tosend)/time.Hour)/24)
	case KP:
		tosend := data.Data.([]time.Time)
		for _, ts := range tosend {
			binary.Write(dbuf, order, ts.Sub(qEpoch))
		}
	case KB, KG, KH, KI, KJ, KE, KF, KT, KV, KU, KM, KN, UU:
		binary.Write(dbuf, order, data.Data)
	case KZ:
		tosend := data.Data.([]time.Time)
		for _, ts := range tosend {
			binary.Write(dbuf, order, -1*(float64(qEpoch.Sub(ts)/time.Millisecond)/86400000))
		}
	case KD:
		tosend := data.Data.([]time.Time)
		for _, ts := range tosend {
			binary.Write(dbuf, order, -1*(int32(qEpoch.Sub(ts)/time.Hour)/24))
		}
	case XD:
		tosend := data.Data.(Dict)
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
		return writeData(dbuf, order, NewDict(SymbolV(tosend.Columns), &K{K0, NONE, tosend.Data}))
	case KERR:
		tosend := data.Data.(error)
		binary.Write(dbuf, order, []byte(tosend.Error()))
		binary.Write(dbuf, order, byte(0))
	case KFUNC:
		tosend := data.Data.(Function)
		binary.Write(dbuf, order, []byte(tosend.Namespace))
		binary.Write(dbuf, order, byte(0))
		return writeData(dbuf, order, &K{KC, NONE, tosend.Body})
	case KFUNCUP, KFUNCBP, KFUNCTR:
		binary.Write(dbuf, order, data.Data.(byte))
	case KPROJ, KCOMP:
		tosend := data.Data.([]*K)
		binary.Write(dbuf, order, uint32(len(tosend)))
		for i := 0; i < len(tosend); i++ {
			writeData(dbuf, order, tosend[i])
		}
	case KPROJ, KCOMP:
		d := data.Data.([]*K)
		err = binary.Write(dbuf, order, int32(len(d)))
		if err != nil {
			return err
		}
		for i := 0; i < len(d); i++ {
			err = writeData(dbuf, order, d[i])
			if err != nil {
				return err
			}
		}
	case KEACH, KOVER, KSCAN, KPRIOR, KEACHRIGHT, KEACHLEFT:
		return writeData(dbuf, order, data.Data.(*K))
	case KFUNCUP, KFUNCBP, KFUNCTR:
		b := data.Data.(byte)
		err = binary.Write(dbuf, order, &b)
		if err != nil {
			return err
		}
	default:
		return errors.New("unknown type " + strconv.Itoa(int(data.Type)))
	}
	return nil

}

// Encode data to ipc format as msgtype(sync/async/response) to specified writer
func Encode(w io.Writer, msgtype ReqType, data *K) error {
	var order = binary.LittleEndian
	buf := new(bytes.Buffer)

	// As a place holder header, write 8 bytes to the buffer
	header := [8]byte{}
	if _, err := buf.Write(header[:]); err != nil {
		return err
	}

	// Then write the qipc encoded data
	if err := writeData(buf, order, data); err != nil {
		return err
	}

	// Now that we have the length of the buffer, create the correct header
	header[0] = 1 // byte order
	header[1] = byte(msgtype)
	header[2] = 0
	header[3] = 0
	order.PutUint32(header[4:], uint32(buf.Len()))

	// Write the correct header to the original buffer
	b := buf.Bytes()
	copy(b, header[:])

	_, err := w.Write(Compress(b))
	return err
}
