# Q driver in Go #

[![Build Status](https://travis-ci.org/sv/kdbgo.svg?branch=master)](https://travis-ci.org/sv/kdbgo)

This is an implementation of kdb+ driver native in Go. It implements Q IPC protocol.

Can be used both as a client(Go program connects to kdb+ process) and as a server(kdb+ connects to Go program).
In server mode no execution capabilities are available.


## TODO ##
- add unix sockets
- add tls
- compression
