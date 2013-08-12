package kdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

func Encode(w io.Writer, msgtype int, data interface{}) (err error) {
	var order = binary.LittleEndian
	dbuf := new(bytes.Buffer)
	switch data.(type) {
	default:
		return errors.New("unknown type")
	case string:
		data := data.(string)

		binary.Write(dbuf, order, int8(10))
		binary.Write(dbuf, order, NONE) // attributes
		binary.Write(dbuf, order, int32(len(data)))
		binary.Write(dbuf, order, []byte(data))
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
	}
	msglen := int32(8 + dbuf.Len())
	var header = ipcHeader{1, byte(msgtype), 0, 0, msglen}
	buf := new(bytes.Buffer)
	err = binary.Write(buf, order, header)
	err = binary.Write(buf, order, dbuf.Bytes())
	_, err = w.Write(buf.Bytes())
	return err
}
