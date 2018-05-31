package kdb

import (
	"bufio"
	"net"
	"strings"
)

// Listen and serve client requests
func ListenAndServe(addr string, handler func(*K, KDBConn) error) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		// TODO: Need to handle possible errors explicitly
		if err != nil {
			continue
		}
		go serve(conn, handler)
	}
}

// Serve a single client connecton
func serve(conn net.Conn, handler func(*K, KDBConn) error) {
	c := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)
	var cred = make([]byte, 100)
	n, err := c.Read(cred)
	if err != nil {
		conn.Close()
		return
	}
	auth := string(cred[:n-2])
	rbuf := bufio.NewReader(conn)
	addr := strings.Split(conn.RemoteAddr().String(), ":")
	kdbconn := KDBConn{conn, rbuf, addr[0], addr[1], auth}

	// TODO: Keep mode in kdb conn struct, properly determine mode
	c.Write([]byte{3})

	id := 0
	for {
		id++
		data, msgtype, err := Decode(kdbconn.rbuf)
		if err == UnsupportedType {
			if msgtype == SYNC {
				Encode(kdbconn.con, RESPONSE, Error(err))
				continue
			}
		} else if err != nil {
			conn.Close()
			return
		}
		if msgtype == SYNC {
			handler(data, kdbconn)
		}
	}
}

// Example echo handler
func EchoHandler(data *K, conn KDBConn) error {
	return Encode(conn.Conn(), RESPONSE, data)
}
