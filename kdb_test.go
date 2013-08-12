package kdb

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

var testHost = "localhost"
var testPort = 1234

func TestConn(t *testing.T) {
	con, err := DialKDB(testHost, testPort, "")
	if err != nil {
		t.Error("Failed to connect.", err)
	}
	err = con.Close()
	if err != nil {
		t.Error("Failed to close connection.", err)
	}
}

func TestConnTimeout(t *testing.T) {
	timeout := time.Second
	con, err := DialKDBTimeout(testHost, testPort, "", timeout)
	if err != nil {
		t.Error("Failed to connect with timeout.", timeout, err)
	}
	err = con.Close()
	if err != nil {
		t.Error("Failed to close connection.", err)
	}
}

func TestSyncCall(t *testing.T) {
	con, _ := DialKDB(testHost, testPort, "")
	res, err := con.Call("show `testreq;`test")
	fmt.Println("Result:", res, err)
	if res.(string) != "test" {
		t.Error("Unexpected result:", res)
	}
}

func TestAsyncCall(t *testing.T) {
	con, _ := DialKDB(testHost, testPort, "")
	err := con.AsyncCall("show `asynctest;asynctest:1b")
	if err != nil {
		t.Error("Async call failed", err)
	}
	// check result
	res, err := con.Call("asynctest")
	fmt.Println("Result:", res, err)
	if !res.(bool) {
		t.Error("Unexpected result:", res)
	}
}

func TestResponse(t *testing.T) {
	con, _ := DialKDB(testHost, testPort, "")
	err := con.Response("show `response;1 2 3")
	if err != nil {
		t.Error("Sending response failed", err)
	}
}

func BenchmarkTradeRead(b *testing.B) {
	con, err := DialKDB("localhost", 1234, "")
	fmt.Println("KDB connection", con, err)
	res, err := con.Call("test")
	fmt.Println("Result:", reflect.TypeOf(res), err)
}
