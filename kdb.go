package kdb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// 0 - v2.5, no compression, no timestamp, no timespan, no uuid
// 1..2 - v2.6-2.8, compression, timestamp, timespan
// 3 - v3.0, compression, timestamp, timespan, uuid
//

// KDBConn establishes connection and communicates using Q IPC protocol
type KDBConn struct {
	con     *net.TCPConn
	rbuf    *bufio.Reader
	Host    string
	Port    string
	userpwd string
}

// Close connection to the server
func (c *KDBConn) Close() error {
	if c.ok() {
		return c.con.Close()
	}
	return errors.New("Closed connection")
}

func (c *KDBConn) ok() bool {
	return c.con != nil
}

// process clients requests
func HandleClientConnection(conn net.Conn) {
	c := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)
	var cred = make([]byte, 100)
	n, err := c.Read(cred)
	if err != nil {
		conn.Close()
		return
	}
	c.Write(cred[n-2 : n-1])
	rbuf := bufio.NewReaderSize(conn, 4*1024*1024)
	i := 0
	for {
		_, msgtype, err := Decode(rbuf)

		if err == io.EOF {
			conn.Close()
			return
		}
		if msgtype == SYNC {
			Encode(conn, RESPONSE, Error(ErrSyncRequest))
		}
		// don't respond
		i++
	}
}

// Make synchronous call to server similar to h(func;arg1;arg2;...)
func (c *KDBConn) Call(cmd string, args ...*K) (data *K, err error) {
	if !c.ok() {
		return nil, errors.New("Closed connection")
	}
	var sending *K
	var cmdK = &K{KC, NONE, cmd}
	if len(args) == 0 {
		sending = cmdK
	} else {
		sending = &K{K0, NONE, append([]*K{cmdK}, args...)}
	}
	err = Encode(c.con, SYNC, sending)
	if err != nil {
		return nil, err
	}
	data, _, err = Decode(c.rbuf)
	return data, err
}

// Make asynchronous request to server
func (c *KDBConn) AsyncCall(cmd string, args ...*K) (err error) {
	if !c.ok() {
		return errors.New("Closed connection")
	}
	var sending *K
	var cmdK = &K{KC, NONE, cmd}
	if len(args) == 0 {
		sending = cmdK
	} else {
		sending = &K{K0, NONE, append([]*K{cmdK}, args...)}
	}
	return Encode(c.con, ASYNC, sending)
}

// Send response to asynchronous request
func (c *KDBConn) Response(data *K) (err error) {
	return Encode(c.con, RESPONSE, data)
}

// Read complete message from connection
func (c *KDBConn) ReadMessage() (data *K, msgtype int, e error) {
	return Decode(c.rbuf)
}

// Write data in Q IPC format
func (c *KDBConn) WriteMessage(msgtype int, data *K) (err error) {
	return Encode(c.con, msgtype, data)
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
		c.Close()
		return nil, err
	}
	var reply = make([]byte, 2+len(auth))
	n, err := c.Read(reply)
	if err != nil {
		c.Close()
		return nil, err
	}
	if n != 1 {
		c.Close()
		return nil, errors.New("Authentication error. Max supported version - " + string(reply[0]))
	}
	_ = c.SetKeepAlive(true) // care if keepalive is failed to be set?
	kdbconn := KDBConn{c, bufio.NewReader(c), host, string(port), auth}
	return &kdbconn, nil
}
