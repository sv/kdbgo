# Go driver for kdb+ #

[![Build Status](https://travis-ci.org/sv/kdbgo.svg?branch=master)](https://travis-ci.org/sv/kdbgo)

This is an implementation of kdb+ driver native in Go. It implements Q IPC protocol.

Can be used both as a client(Go program connects to kdb+ process) and as a server(kdb+ connects to Go program).
In server mode no execution capabilities are available.

## For documentations and examples see [godoc](https://godoc.org/github.com/sv/kdbgo)