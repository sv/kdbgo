package kdb

import (
	"fmt"
	"reflect"
	"testing"
)

func TestConn(t *testing.T) {
	con, err := DialKDB("localhost", 1234, "")
	fmt.Println("KDB connection", con, err)
	res, err := con.Call("`test")
	fmt.Println("Result:", res, err)
	if res.(string) != "test" {
		t.Fail()
	}
	err = con.Close()

}

func BenchmarkTradeRead(b *testing.B) {
	con, err := DialKDB("localhost", 1234, "")
	fmt.Println("KDB connection", con, err)
	b.ResetTimer()
	res, err := con.Call("test")
	fmt.Println("Result:", reflect.TypeOf(res), err)
}
