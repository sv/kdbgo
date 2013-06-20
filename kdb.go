package kdb

import (
	"bytes"
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

func (c *KDBConn) Call(cmd string, args ...interface{}) (data interface{}, err error) {
	err = Encode(c.con, SYNC, cmd)
	if err != nil {
		return nil, err
	}
	return Decode(c.con)
}

func (c *KDBConn) AsyncCall(cmd string, args ...interface{}) (err error) {
	return Encode(c.con, ASYNC, cmd)
}

func (c *KDBConn) Response(data interface{}) (err error) {
	return Encode(c.con, RESPONSE, data)
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
