package kdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

func Encode(w io.Writer, data interface{}) (err error) {
	switch data.(type) {
	case string:
		data := data.(string)
		var order = binary.LittleEndian
		dbuf := new(bytes.Buffer)
		binary.Write(dbuf, order, int8(10))
		binary.Write(dbuf, order, int8(0))
		binary.Write(dbuf, order, int32(len(data)))
		binary.Write(dbuf, order, []byte(data))

		msglen := int32(8 + dbuf.Len())
		var header = ipcHeader{1, 1, 0, 0, msglen}
		buf := new(bytes.Buffer)
		err = binary.Write(buf, order, header)
		err = binary.Write(buf, order, dbuf.Bytes())
		_, err = w.Write(buf.Bytes())
		return err
	}
	return errors.New("type not found")
}
