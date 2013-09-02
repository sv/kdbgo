package kdb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"io"
	"net"
	"time"
)

// 0 - v2.5, no compression, no timestamp, no timespan, no uuid
// 1..2 - v2.6-2.8, compression, timestamp, timespan
// 3 - v3.0, compression, timestamp, timespan, uuid
//

type KDBConn struct {
	con     *net.TCPConn
	rbuf    *bufio.Reader
	Host    string
	Port    string
	userpwd string
}

// Close connection to the server
func (c *KDBConn) Close() error {
	return c.con.Close()
}

// process clients requests
func HandleClientConnection(conn net.Conn) {
	glog.V(1).Infoln("client connected")
	c := conn.(*net.TCPConn)
	var cred = make([]byte, 100)
	n, err := c.Read(cred)
	if err != nil {
		conn.Close()
		return
	}
	glog.V(1).Infoln("capabilities:", n, cred[:n])
	c.Write(cred[n-2 : n-1])
	rbuf := bufio.NewReader(conn)
	glog.V(1).Infoln("authenticated")
	i := 0
	for {
		d, msgtype, err := Decode(rbuf)

		if err == io.EOF {
			conn.Close()
			glog.V(1).Infoln("Connection closed")
			return
		}
		if msgtype == SYNC {
			Encode(conn, RESPONSE, ErrSyncRequest)
		} else {
			Encode(conn, RESPONSE, d)
		}
		i++
		glog.V(1).Infoln("msgnum#", i)
	}
}

// Make synchronous call to server similar to h(func;arg1;arg2;...)
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
	data, _, err = Decode(c.rbuf)
	return data, err
}

// Make asynchronous request to server
func (c *KDBConn) AsyncCall(cmd string, args ...interface{}) (err error) {
	var sending interface{}
	if len(args) == 0 {
		sending = cmd
	} else {
		sending = append([]interface{}{cmd}, args)
	}
	return Encode(c.con, ASYNC, sending)
}

// Send response to asynchronous request
func (c *KDBConn) Response(data interface{}) (err error) {
	return Encode(c.con, RESPONSE, data)
}

// Connect to host:port using supplies user:password. Wait until connected
func DialKDB(host string, port int, auth string) (*KDBConn, error) {
	var timeout time.Duration
	return DialKDBTimeout(host, port, auth, timeout)
}

// Connect to host:port using supplied user:password. Wait timeout for connection
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
	fmt.Println(reply)
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, errors.New("Authentication error. Max supported version - " + string(reply[0]))
	}
	kdbconn := KDBConn{c, bufio.NewReader(c), host, string(port), auth}
	return &kdbconn, nil
}
