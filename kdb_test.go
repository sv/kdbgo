package kdb

import (
	"fmt"
	"testing"
)

func TestConn(t *testing.T) {
	con, err := DialKDB("localhost", 1234, "")
	fmt.Println("KDB connection", con, err)
	res, err := con.Cmd("`test")
	fmt.Println("Result:", res, err)
	if res.(string) != "test" {
		t.Fail()
	}
	err = con.Close()

}

func BenchmarkTradeRead(b *testing.B) {
	con, err := DialKDB("localhost", 1234, "")
	fmt.Println("KDB connection", con, err)
	//b.ResetTimer()
	_, err = con.Cmd("([]sym:1000000#`5;price:1000000?100f;size:1000000?20)")
	//fmt.Println("Result:", res, err)
}
