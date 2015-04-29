package kdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"

	"github.com/golang/glog"
)

func writeData(dbuf io.Writer, order binary.ByteOrder, data *K) (err error) {
	glog.V(1).Infoln(reflect.TypeOf(data))
	switch data.Type {
	case K0:
		tosend := data.Data.([]*K)
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, NONE) // attributes
		binary.Write(dbuf, order, int32(len(tosend)))
		for i := 0; i < len(tosend); i++ {
			writeData(dbuf, order, tosend[i])
		}
	case -KS:
		tosend := data.Data.(string)

		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, NONE) // attributes
		binary.Write(dbuf, order, int32(len(tosend)))
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
	case KG, KI, KJ, KE, KF:
		binary.Write(dbuf, order, int8(data.Type))
		binary.Write(dbuf, order, NONE) // attributes
		binary.Write(dbuf, order, int32(reflect.ValueOf(data.Data).Len()))
		binary.Write(dbuf, order, data.Data)
	case XD:
		tosend := data.Data.(Dict)
		binary.Write(dbuf, order, XD)
		writeData(dbuf, order, tosend.Key)
		writeData(dbuf, order, tosend.Value)
	case XT:
		tosend := data.Data.(Table)
		binary.Write(dbuf, order, XT)
		binary.Write(dbuf, order, NONE) // attributes
		writeData(dbuf, order, &K{XD, NONE, Dict{&K{KS, NONE, tosend.Columns}, &K{K0, NONE, tosend.Data}}})
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
		writeData(dbuf, order, &K{KC, NONE, tosend.Body})

	default:
		return errors.New("unknown type")
	}
	return nil

}

// Encode data to ipc format as msgtype(sync/async/response) to specified writer
func Encode(w io.Writer, msgtype int, data *K) (err error) {
	var order = binary.LittleEndian
	dbuf := new(bytes.Buffer)
	err = writeData(dbuf, order, data)
	if err != nil {
		return err
	}
	msglen := int32(8 + dbuf.Len())
	var header = ipcHeader{1, byte(msgtype), 0, 0, msglen}
	buf := new(bytes.Buffer)
	err = binary.Write(buf, order, header)
	err = binary.Write(buf, order, dbuf.Bytes())
	_, err = w.Write(buf.Bytes())
	return err
}
