package kdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/golang/glog"
	"io"
	"reflect"
)

func writeData(dbuf io.Writer, order binary.ByteOrder, data interface{}) (err error) {
	usereflect := false
	glog.V(1).Infoln(reflect.TypeOf(data))
	switch data.(type) {
	case string:
		data := data.(string)

		binary.Write(dbuf, order, int8(10))
		binary.Write(dbuf, order, NONE) // attributes
		binary.Write(dbuf, order, int32(len(data)))
		binary.Write(dbuf, order, []byte(data))
	case []string:
		data := data.([]string)
		binary.Write(dbuf, order, int8(11))
		binary.Write(dbuf, order, NONE) // attributes
		binary.Write(dbuf, order, int32(len(data)))
		for i := 0; i < len(data); i++ {
			binary.Write(dbuf, order, []byte(data[i]))
			binary.Write(dbuf, order, byte(0))
		}
	case bool:
		data := data.(bool)
		binary.Write(dbuf, order, int8(-1))
		var val byte
		if data {
			val = 0x01
		} else {
			val = 0x00
		}
		binary.Write(dbuf, order, val)
	case int32:
		binary.Write(dbuf, order, int8(-6))
		binary.Write(dbuf, order, data)
	case int64:
		binary.Write(dbuf, order, int8(-7))
		binary.Write(dbuf, order, data)
	case float32:
		binary.Write(dbuf, order, int8(-8))
		binary.Write(dbuf, order, data)
	case float64:
		binary.Write(dbuf, order, int8(-9))
		binary.Write(dbuf, order, data)
	case []int32:
		binary.Write(dbuf, order, int8(6))
		binary.Write(dbuf, order, NONE) // attributes
		binary.Write(dbuf, order, int32(reflect.ValueOf(data).Len()))
		binary.Write(dbuf, order, data)
	case []int64:
		binary.Write(dbuf, order, int8(7))
		binary.Write(dbuf, order, NONE) // attributes
		binary.Write(dbuf, order, int32(reflect.ValueOf(data).Len()))
		binary.Write(dbuf, order, data)
	case []float32:
		binary.Write(dbuf, order, int8(8))
		binary.Write(dbuf, order, NONE) // attributes
		binary.Write(dbuf, order, int32(reflect.ValueOf(data).Len()))
		binary.Write(dbuf, order, data)
	case []float64:
		binary.Write(dbuf, order, int8(9))
		binary.Write(dbuf, order, NONE) // attributes
		binary.Write(dbuf, order, int32(reflect.ValueOf(data).Len()))
		binary.Write(dbuf, order, data)
	case []byte:
		binary.Write(dbuf, order, int8(4))
		binary.Write(dbuf, order, NONE) // attributes
		binary.Write(dbuf, order, int32(reflect.ValueOf(data).Len()))
		binary.Write(dbuf, order, data)
	case Dict:
		data := data.(Dict)
		binary.Write(dbuf, order, int8(99))
		writeData(dbuf, order, data.Keys)
		writeData(dbuf, order, data.Values)
	case Table:
		data := data.(Table)
		binary.Write(dbuf, order, int8(98))
		binary.Write(dbuf, order, NONE) // attributes
		writeData(dbuf, order, Dict{data.Columns, data.Data})
	case error:
		data := data.(error)
		binary.Write(dbuf, order, int8(-128))
		binary.Write(dbuf, order, []byte(data.Error()))
		binary.Write(dbuf, order, byte(0))
	case Function:
		data := data.(Function)
		binary.Write(dbuf, order, int8(100))
		binary.Write(dbuf, order, []byte(data.Namespace))
		binary.Write(dbuf, order, byte(0))
		writeData(dbuf, order, data.Body)

	default:
		usereflect = true
	}

	if !usereflect {
		return nil
	}
	//use reflection
	dv := reflect.ValueOf(data)
	dk := dv.Kind()
	glog.V(1).Infoln(dk)
	if dk == reflect.Slice || dk == reflect.Array {
		glog.V(1).Infoln(dv.Type().Elem())
		if dv.Type().Elem().Kind() == reflect.Interface {
			glog.V(1).Infoln("Encoding generic array")

			binary.Write(dbuf, order, int8(0))
			binary.Write(dbuf, order, NONE) // attributes
			binary.Write(dbuf, order, int32(dv.Len()))
			for i := 0; i < dv.Len(); i++ {
				writeData(dbuf, order, dv.Index(i).Interface())
			}
			return nil
		}
	}
	return errors.New("unknown type")
}
func Encode(w io.Writer, msgtype int, data interface{}) (err error) {
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
