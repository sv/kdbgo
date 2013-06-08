package kdb

import (
	"bufio"
	//"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"io"
	"time"
)

var ErrBadMsg = errors.New("Bad Message")
var ErrBadHeader = errors.New("Bad header")

func makeArray(vectype int8, veclen int32) interface{} {
	switch vectype {
	case 1, 4, 10:
		return make([]byte, veclen)
	case 2:
		return make([]uuid.UUID, veclen)
	case 5:
		return make([]int16, veclen)
	case 6, 13, 14, 17, 18, 19:
		return make([]int32, veclen)
	case 16:
		return make([]time.Duration, veclen)
	case 7, 12:
		return make([]int64, veclen)
	case 8:
		return make([]float32, veclen)
	case 9, 15:
		return make([]float64, veclen)
	case 11:
		return make([]string, veclen)
	}

	return nil
}

type ipcHeader struct {
	ByteOrder   byte
	RequestType byte
	Compressed  byte
	_           byte
	MsgSize     int32
}

func (h *ipcHeader) getByteOrder() binary.ByteOrder {
	var order binary.ByteOrder
	order = binary.LittleEndian
	if h.ByteOrder == 0x00 {
		order = binary.BigEndian
	}
	return order
}
func Decode(src io.Reader) (kobj interface{}, e error) {
	var r = bufio.NewReader(src)
	var header ipcHeader
	err := binary.Read(r, binary.LittleEndian, &header)
	if err != nil {
		fmt.Println("binary.Read failed:", err)
	}
	fmt.Println("Header -> ", header)
	var order = header.getByteOrder()
	return readData(r, order)
}
func readData(r *bufio.Reader, order binary.ByteOrder) (kobj interface{}, err error) {
	var msgtype int8
	//var msglen = header.MsgSize
	binary.Read(r, order, &msgtype)
	fmt.Println("Msg Type:", msgtype)
	switch msgtype {
	case -1:
		var b byte
		binary.Read(r, order, &b)
		return b != 0x0, nil

	case -2:
		var u uuid.UUID
		binary.Read(r, order, &u)
		return u, nil

	case -4:
		var b byte
		binary.Read(r, order, &b)
		return b, nil
	case -5:
		var sh int16
		binary.Read(r, order, &sh)
		return sh, nil

	case -6:
		var i int32
		binary.Read(r, order, &i)
		return i, nil
	case -7:
		var j int64
		binary.Read(r, order, &j)
		return j, nil
	case -11:
		line, err := r.ReadBytes(0)
		if err != nil {
			return nil, err
		}
		str := string(line[:len(line)-1])

		return str, nil
	case 1, 2, 4, 5, 6, 7, 8, 9, 10, 16:
		var vecattr int8
		binary.Read(r, order, &vecattr)
		//fmt.Println("vecattr -> ", vecattr)
		var veclen int32
		err = binary.Read(r, order, &veclen)
		if err != nil {
			fmt.Println("Reading vector length failed -> %v", err)
		}
		var arr = makeArray(msgtype, veclen)
		err = binary.Read(r, order, arr)
		if err != nil {
			fmt.Println("Error during conversion -> ", err)
			return nil, err
		}
		if msgtype == 10 {
			return string(arr.([]byte)), nil
		}
		return arr, nil
	case 11:
		var vecattr int8
		binary.Read(r, order, &vecattr)
		var veclen int32
		err = binary.Read(r, order, &veclen)
		if err != nil {
			fmt.Println("Reading vector length failed -> %v", err)
		}
		var arr = makeArray(msgtype, veclen).([]string)
		for i := 0; i < int(veclen); i++ {
			line, err := r.ReadBytes(0)
			if err != nil {
				return nil, err
			}
			arr[i] = string(line[:len(line)-1])
		}
		return arr, nil
	case -128:
		line, err := r.ReadBytes(0)
		if err != nil {
			return nil, err
		}
		errmsg := string(line[:len(line)-1])
		return nil, errors.New(errmsg)
	}
	return nil, ErrBadMsg
}
