package kdb

import (
	"fmt"
	//"reflect"
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
