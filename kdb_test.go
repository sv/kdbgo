package kdb

import (
	"fmt"
	//"reflect"
	"crypto/tls"
	//"io"
	"bytes"
	"log"
	"os"
	"strconv"
	//"io/ioutil"
	"os/exec"
	"testing"
	"time"
)

var testHost = "localhost"
var testPort = 0

func TestMain(m *testing.M) {
	fmt.Println("Starting q process on random port")
	_, err := exec.LookPath("q")
	if err != nil {
		log.Fatal("installing q is in your future")
	}
	cmd := exec.Command("q", "-p", "0W", "-q")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Fatal("Failed to connect stdin", err)
	}
	stdin.Write([]byte(".z.pi:.z.pg:.z.ps:{value 0N!x};system\"p\"\n"))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal("Failed to connect stdout", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Fatal(err)
	}
	err = cmd.Start()
	if err != nil {
		log.Fatal("Failed to start q", err)
	}
	buf := make([]byte, 16)
	n, _ := stdout.Read(buf)
	testPort, err = strconv.Atoi(string(buf[:bytes.IndexByte(buf, 'i')]))
	if err != nil {
		fmt.Println("Failed to setup listening port", string(buf[:n]), err)
		cmd.Process.Kill()
		os.Exit(1)
	}
	fmt.Println("Listening port is ", testPort)
	go func() {
		for {
			buf := make([]byte, 256)
			n, err := stderr.Read(buf)
			fmt.Println("Q stderr output:", string(buf[:n]))
			if err != nil {
				fmt.Println("Q stderr error:", err)
				return
			}
		}
	}()
	go func() {
		for {
			buf := make([]byte, 256)
			n, err := stdout.Read(buf)
			fmt.Println("Q stdout output:", string(buf[:n]))
			if err != nil {
				fmt.Println("Q stdout error:", err)
				return
			}
		}
	}()
	//stdin.Close()
	res := m.Run()
	stdin.Write([]byte("show `exiting_process;\nexit 0\n"))
	cmd.Wait()

	os.Exit(res)
}

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
		t.Fatalf("Failed to connect to test instance via TLS: %s", err)
	}
	err = con.Close()
	if err != nil {
		t.Error("Failed to close connection on TLS.", err)
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
	sum := 0
	for i := range vec {
		vec[i] = int64(i)
		sum += i
	}
	fmt.Println("Testing sync function call with large data compression")
	res, _ := con.Call("sum", &K{KJ, NONE, vec})
	fmt.Println("Result:", res, err)
	if res != nil && res.Data.(int64) != int64(sum) {
		t.Error("Unexpected result:", res, sum)
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
		t.Fatalf("Failed to connect to test instance via TLS: %s", err)
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
		t.Fatal("Async call failed", err)
	}
	// check result
	res, err := con.Call("asynctest")
	fmt.Println("Result:", res, err)
	if err != nil {
		t.Fatal("Fetching result error", err)
	}
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
	con, err := DialKDB(testHost, testPort, "")
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
