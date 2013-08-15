package kdb

import (
	"bytes"
	"errors"
	"fmt"
	"bufio"
	"net"
	"time"
)

type KDBConn struct {
	con     *net.TCPConn
	rbuf	*bufio.Reader
	Host    string
	Port    string
	userpwd string
}

func (c *KDBConn) Close() error {
	return c.con.Close()
}

func (c *KDBConn) Call(cmd string, args ...interface{}) (data interface{}, err error) {
	var sending interface{}
	if len(args) == 0 {
		sending = cmd
	} else {
		sending = append([]interface{}{cmd}, args)
	}
	err = Encode(c.con, SYNC, sending)
	if err != nil {
		return nil, err
	}
	return Decode(c.rbuf)
}

func (c *KDBConn) AsyncCall(cmd string, args ...interface{}) (err error) {
	var sending interface{}
	if len(args) == 0 {
		sending = cmd
	} else {
		sending = append([]interface{}{cmd}, args)
	}
	return Encode(c.con, ASYNC, sending)
}

func (c *KDBConn) Response(data interface{}) (err error) {
	return Encode(c.con, RESPONSE, data)
}

func DialKDB(host string, port int, auth string) (*KDBConn, error) {
	var timeout time.Duration
	return DialKDBTimeout(host, port, auth, timeout)
}

// 0 - v2.5, no compression, no timestamp, no timespan, no uuid
// 1..2 - v2.6-2.8, compression, timestamp, timespan
// 3 - v3.0, compression, timestamp, timespan, uuid
//
func DialKDBTimeout(host string, port int, auth string, timeout time.Duration) (*KDBConn, error) {
	conn, err := net.Dial("tcp", host+":"+fmt.Sprint(port))
	if err != nil {
		return nil, err
	}
	c := conn.(*net.TCPConn)
	// handshake - assuming latest protocol
	var buf = bytes.NewBufferString(auth)
	// capabilities
	// 3 - uuid/etc
	buf.WriteByte(3)
	buf.WriteByte(0)
	_, err = c.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}
	var reply = make([]byte, 2+len(auth))
	n, err := c.Read(reply)
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, errors.New("Authentication error. Max supported version - " + string(reply[0]))
	}
	kdbconn := KDBConn{c, bufio.NewReader(c),host, string(port), auth}
	return &kdbconn, nil
}
