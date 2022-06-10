package kdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"reflect"
	"strconv"
	"time"
)

func writeData(dbuf io.Writer, order binary.ByteOrder, data *K) (err error) {
	binary.Write(dbuf, order, data.Type)
	if data.Type >= K0 && data.Type < XD {
		binary.Write(dbuf, order, data.Attr) // attributes

	}
	switch data.Type {
	case K0:
		tosend := data.Data.([]*K)
		binary.Write(dbuf, order, int32(len(tosend)))
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
		binary.Write(dbuf, order, int32(len(tosend)))
		binary.Write(dbuf, order, []byte(tosend))
	case KS:
		tosend := data.Data.([]string)
		binary.Write(dbuf, order, int32(len(tosend)))
		for i := 0; i < len(tosend); i++ {
			binary.Write(dbuf, order, []byte(tosend[i]))
			binary.Write(dbuf, order, byte(0))
		}
	case -KB:
		tosend := data.Data.(bool)
		var val byte
		if tosend {
			val = 0x01
		} else {
			val = 0x00
		}
		binary.Write(dbuf, order, val)
	case -KG, -KH, -KI, -KJ, -KE, -KF, -UU:
		binary.Write(dbuf, order, data.Data)
	case -KP:
		tosend := data.Data.(time.Time)
		binary.Write(dbuf, order, tosend.Sub(qEpoch))
	case KP:
		binary.Write(dbuf, order, int32(reflect.ValueOf(data.Data).Len()))
		tosend := data.Data.([]time.Time)
		for _, ts := range tosend {
			binary.Write(dbuf, order, ts.Sub(qEpoch))
		}
	case KB:
		binary.Write(dbuf, order, int32(reflect.ValueOf(data.Data).Len()))
		tosend := data.Data.([]bool)
		boolmap := map[bool]byte{false: 0x00, true: 0x01}
		for _, b := range tosend {
			binary.Write(dbuf, order, boolmap[b])
		}
	case KG, KI, KJ, KE, KF, KZ, KT, KD, KV, KU, KM, KN, UU:
		binary.Write(dbuf, order, int32(reflect.ValueOf(data.Data).Len()))
		binary.Write(dbuf, order, data.Data)
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
		err = writeData(dbuf, order, NewDict(SymbolV(tosend.Columns), &K{K0, NONE, tosend.Data}))
		if err != nil {
			return err
		}
	case KERR:
		tosend := data.Data.(error)
		binary.Write(dbuf, order, []byte(tosend.Error()))
		binary.Write(dbuf, order, byte(0))
	case KFUNC:
		tosend := data.Data.(Function)
		binary.Write(dbuf, order, []byte(tosend.Namespace))
		binary.Write(dbuf, order, byte(0))
		err = writeData(dbuf, order, &K{KC, NONE, tosend.Body})
		if err != nil {
			return err
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

func WriteToFile(filename string, data *K) error {
	var order = binary.LittleEndian
	buf := new(bytes.Buffer)

	buf.Write([]byte{0xFF, 0x01})
	if err := writeData(buf, order, data); err != nil {
		return err
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	f.Write(buf.Bytes())
	defer f.Close()

	return nil
}
