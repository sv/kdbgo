package kdb

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"time"
)

// 0 - v2.5, no compression, no timestamp, no timespan, no uuid
// 1..2 - v2.6-2.8, compression, timestamp, timespan
// 3 - v3.0, compression, timestamp, timespan, uuid
//

// Conn represents connection and communicates using Q IPC protocol
type Conn struct {
	con     net.Conn
	rbuf    *bufio.Reader
	network string
	address string
	auth    string
}

var ErrConnClosed = errors.New("Closed connection")

// Close connection to the server
func (c *Conn) Close() error {
	if c.ok() {
		return c.con.Close()
	}
	return ErrConnClosed
}

func (c *Conn) ok() bool {
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

// Call performs synchronous call to kdb+ similar to h(func;arg1;arg2;...)
func (c *Conn) Call(cmd string, args ...*K) (data *K, err error) {
	if !c.ok() {
		return nil, ErrConnClosed
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

// AsyncCall performs asynchronous call to kdb+
func (c *Conn) AsyncCall(cmd string, args ...*K) (err error) {
	if !c.ok() {
		return ErrConnClosed
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

// Response sends response to asynchronous call
func (c *Conn) Response(data *K) (err error) {
	return Encode(c.con, RESPONSE, data)
}

// ReadMessage reads complete message from connection
func (c *Conn) ReadMessage() (data *K, msgtype ReqType, e error) {
	return Decode(c.rbuf)
}

// WriteMessage sends data in Q IPC format
func (c *Conn) WriteMessage(msgtype ReqType, data *K) (err error) {
	return Encode(c.con, msgtype, data)
}

// Dial connects to host:port using supplied user:password. Wait until connected
func Dial(network, address string) (*Conn, error) {
	var timeout time.Duration
	return DialTimeout(network, address, timeout)
}

func kdbHandshake(c net.Conn, auth string) error {
	// handshake - assuming latest protocol
	var buf = bytes.NewBufferString(auth)
	// capabilities
	// 3 - uuid/etc
	buf.WriteByte(3)
	buf.WriteByte(0)
	_, err := c.Write(buf.Bytes())
	if err != nil {
		c.Close()
		return err
	}
	var reply = make([]byte, 2+len(auth))
	n, err := c.Read(reply)
	if err != nil {
		c.Close()
		return err
	}
	if n != 1 {
		c.Close()
		return errors.New("Authentication error. Max supported version - " + string(reply[0]))
	}
	return nil
}

func parseAddress(address string) (dial string, auth string, err error) {
	hp := strings.IndexByte(address, ':')
	if hp == -1 {
		// just port of filename without auth
		return address, "", nil
	}
	hp = strings.IndexByte(address[hp+1:], ':')
	if hp == -1 {
		// only port specified - no auth
		return address, "", nil
	}
	fmt.Printf("dial=%s, auth=%s\n", address[:hp], address[hp+1:])
	return address[:hp], address[hp+1:], nil
}

// DialTLS connects to host:port using TLS with cfg provided
func DialTLS(network, address string, cfg *tls.Config) (*Conn, error) {
	dial, auth, err := parseAddress(address)
	if err != nil {
		return nil, err
	}
	c, err := tls.Dial("tcp", dial, cfg)
	if err != nil {
		return nil, err
	}
	err = kdbHandshake(c, auth)
	if err != nil {
		return nil, err
	}
	kdbconn := Conn{c, bufio.NewReader(c), network, dial, auth}
	return &kdbconn, nil
}

// DialUnix connects to port using unix domain sockets. host parameter is ignored.
func DialUnix(network, address string) (*Conn, error) {
	dial, auth, err := parseAddress(address)
	if err != nil {
		return nil, err
	}
	if s := strings.IndexByte(dial, ':'); s != -1 {
		dial = dial[s+1:]
	}
	dial = "/tmp/kx." + dial
	if runtime.GOOS == "linux" {
		dial = "@" + dial
	}
	c, err := net.Dial("unix", dial)
	if err != nil {
		return nil, err
	}

	err = kdbHandshake(c, auth)
	if err != nil {
		return nil, err
	}

	kdbconn := Conn{c, bufio.NewReader(c), network, dial, auth}
	return &kdbconn, nil
}

// DialTimeout connects to host:port using supplied user:password. Wait timeout for connection
func DialTimeout(network, address string, timeout time.Duration) (*Conn, error) {
	if network == "" {
		network = "tcp"
	}
	dial, auth, err := parseAddress(address)
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial(network, dial)
	if err != nil {
		return nil, err
	}
	err = kdbHandshake(conn, auth)
	if err != nil {
		return nil, err
	}
	if c, ok := conn.(*net.TCPConn); ok {
		_ = c.SetKeepAlive(true) // care if keepalive is failed to be set?
	}
	kdbconn := Conn{conn, bufio.NewReader(conn), network, dial, auth}
	return &kdbconn, nil
}
