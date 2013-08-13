package kdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
)

func writeData(dbuf io.Writer, order binary.ByteOrder, data interface{}) (err error) {
	switch data.(type) {
	default:
		fmt.Println(reflect.ValueOf(data).Kind())
		return errors.New("unknown type")
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
	case []int32:
		binary.Write(dbuf, order, int8(6))
		binary.Write(dbuf, order, NONE) // attributes
		binary.Write(dbuf, order, int32(binary.Size(data)/binary.Size(int32(1))))
		binary.Write(dbuf, order, data)
	case []byte:
		binary.Write(dbuf, order, int8(4))
		binary.Write(dbuf, order, NONE) // attributes
		binary.Write(dbuf, order, int32(binary.Size(data)/binary.Size(byte(1))))
		binary.Write(dbuf, order, data)
	case Dict:
		data := data.(Dict)
		binary.Write(dbuf, order, int8(99))
		writeData(dbuf, order, data.Keys)
		writeData(dbuf, order, data.Values)
	}
	return nil
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
