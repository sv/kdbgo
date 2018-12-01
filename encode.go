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
	case -KI, -KJ, -KE, -KF, -UU:
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
	default:
		return errors.New("unknown type " + strconv.Itoa(int(data.Type)))
	}
	return nil

}

// Encode data to ipc format as msgtype(sync/async/response) to specified writer
func Encode(w io.Writer, msgtype ReqType, data *K) (err error) {
	var order = binary.LittleEndian
	dbuf := new(bytes.Buffer)
	err = writeData(dbuf, order, data)
	if err != nil {
		return err
	}
	msglen := uint32(8 + dbuf.Len())
	var header = ipcHeader{1, msgtype, 0, 0, msglen}
	buf := new(bytes.Buffer)
	err = binary.Write(buf, order, header)
	err = binary.Write(buf, order, dbuf.Bytes())
	_, err = w.Write(Compress(buf.Bytes()))
	return err
}
