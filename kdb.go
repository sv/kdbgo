package kdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

type KDBConn struct {
	con     *net.TCPConn
	Host    string
	Port    string
	userpwd string
}

func (c *KDBConn) Close() error {
	return c.con.Close()
}

func (c *KDBConn) Cmd(cmd string) (data interface{}, err error) {
	var order = binary.LittleEndian
	cmdbuf := new(bytes.Buffer)
	binary.Write(cmdbuf, order, int8(10))
	binary.Write(cmdbuf, order, int8(0))
	binary.Write(cmdbuf, order, int32(len(cmd)))
	binary.Write(cmdbuf, order, []byte(cmd))

	msglen := int32(8 + len(cmdbuf.Bytes()))
	var header = ipcHeader{1, 1, 0, 0, msglen}
	buf := new(bytes.Buffer)
	err = binary.Write(buf, order, header)
	err = binary.Write(buf, order, cmdbuf.Bytes())
	_, err = c.con.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}
	return Decode(c.con)
}
func DialKDB(host string, port int, auth string) (*KDBConn, error) {
	tcpaddr, err := net.ResolveTCPAddr("tcp", host+":"+fmt.Sprint(port))
	if err != nil {
		return nil, err
	}
	//fmt.Println("connecting")
	conn, err := net.DialTCP("tcp", nil, tcpaddr)
	if err != nil {
		return nil, err
	}
	// handshake - assuming latest protocol
	var buf = bytes.NewBufferString(auth)
	buf.WriteByte(0)
	_, err = conn.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}
	var reply = make([]byte, 2+len(auth))
	n, err := conn.Read(reply)
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, errors.New("Authentication error" + string(reply))
	}
	kdbconn := KDBConn{conn, host, string(port), auth}
	return &kdbconn, nil
}
