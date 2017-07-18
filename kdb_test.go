package kdb

import (
	"fmt"
	//"reflect"
	"crypto/tls"
	"testing"
	"time"
)

var testHost = "localhost"
var testPort = 1234

func TestConn(t *testing.T) {
	con, err := DialKDB(testHost, testPort, "")
	if err != nil {
		t.Fatalf("Failed to connect to test instance: %s", err)
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
		t.Fatalf("Failed to connect with timeout(%s). Error: %s", timeout, err)
	}
	err = con.Close()
	if err != nil {
		t.Error("Failed to close connection.", err)
	}
}

func TestConnUnix(t *testing.T) {
	con, err := DialUnix(testHost, testPort, "")
	if err != nil {
		t.Fatalf("Failed to connect to test instance via UDS: %s", err)
	}
	err = con.Close()
	if err != nil {
		t.Error("Failed to close connection on UDS.", err)
	}
}

func TestConnTLS(t *testing.T) {
	con, err := DialTLS(testHost, testPort, "", &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("Failed to connect to test instance via UDS: %s", err)
	}
	err = con.Close()
	if err != nil {
		t.Error("Failed to close connection on UDS.", err)
	}
}

func TestSyncCall(t *testing.T) {
	con, err := DialKDB(testHost, testPort, "")
	if err != nil {
		t.Fatalf("Failed to connect to test instance: %s", err)
	}
	fmt.Println("Testing sync function call")
	_, _ = con.Call("show `testreq;(.q;.Q;.h;.o);1000000#0i")
	/*fmt.Println("Result:", res, err)
	if res.(string) != "test" {
		t.Error("Unexpected result:", res)
	}*/
}

func TestSyncCallCompress(t *testing.T) {
	con, err := DialKDB(testHost, testPort, "")
	if err != nil {
		t.Fatalf("Failed to connect to test instance: %s", err)
	}
	vec := make([]int64, 25000000)
	for i := range vec {
		vec[i] = int64(i)
	}
	fmt.Println("Testing sync function call with large data compression")
	res, _ := con.Call("sum", &K{KJ, NONE, vec})
	fmt.Println("Result:", res, err)
	if res != nil && res.Data.(int64) != 499999500000 {
		t.Error("Unexpected result:", res, 499999500000)
	}
}

func TestSyncCallUnix(t *testing.T) {
	con, err := DialUnix(testHost, testPort, "")
	if err != nil {
		t.Fatalf("Failed to connect to test instance via UDS: %s", err)
	}
	fmt.Println("Testing sync function call via UDS")
	_, _ = con.Call("show `testreqUnix;(.q;.Q;.h;.o);1000000#0i")
	/*fmt.Println("Result:", res, err)
	if res.(string) != "test" {
		t.Error("Unexpected result:", res)
	}*/
}

func TestSyncCallTLS(t *testing.T) {
	con, err := DialTLS(testHost, testPort, "", &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("Failed to connect to test instance via UDS: %s", err)
	}
	fmt.Println("Testing sync function call via TLS")
	_, _ = con.Call("show `testreqTLS;(.q;.Q;.h;.o);1000000#0i")
	/*fmt.Println("Result:", res, err)
	if res.(string) != "test" {
		t.Error("Unexpected result:", res)
	}*/
}

func TestAsyncCall(t *testing.T) {
	con, err := DialKDB(testHost, testPort, "")
	if err != nil {
		t.Fatalf("Failed to connect to test instance: %s", err)
	}
	err = con.AsyncCall("show `asynctest;asynctest:1b")
	if err != nil {
		t.Error("Async call failed", err)
	}
	// check result
	res, err := con.Call("asynctest")
	fmt.Println("Result:", res, err)
	if !res.Data.(bool) {
		t.Error("Unexpected result:", res)
	}
}

/*
func TestAsyncCall2(t *testing.T) {
	con, _ := DialKDB(testHost, testPort, "")
	fmt.Println("Testing async function call with parameters")
	err := con.AsyncCall("show", Table{[]string{"a", "b"}, []interface{}{[]int32{2}, []int32{3}}})
	err = con.AsyncCall("app")
	if err != nil {
		t.Error("Async call2  failed", err)
	}
	// check result
	res, err := con.Call("1b")
	fmt.Println("Result:", res, err)
	if !res.(bool) {
		t.Error("Unexpected result:", res)
	}
}
*/

func TestResponse(t *testing.T) {
	con, err := DialKDB(testHost, testPort, "")
	if err != nil {
		t.Fatalf("Failed to connect to test instance: %s", err)
	}
	err = con.Response(&K{KC, NONE, "show `response;1 2 3"})
	if err != nil {
		t.Error("Sending response failed", err)
	}
}

func BenchmarkTradeRead(b *testing.B) {
	con, err := DialKDB("localhost", 1234, "")
	if err != nil {
		b.Fatalf("Failed to connect to test instance: %s", err)
	}
	con.Call("testdata:([]time:10?.z.p;sym:10?`8;price:100+10?1f;size:10?10)")
	//fmt.Println("KDB connection", con, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = con.Call("10#testdata")
		//fmt.Println("Result:", reflect.TypeOf(res), err)
	}
}
