package kdb_test

import (
	"fmt"

	"bitbucket.org/svidyuk/kdbgo"
)

func ExampleKDBConn_Call() {
	con, err := kdb.DialKDB("localhost", 1234, "")
	if err != nil {
		fmt.Println("Failed to connect:", err)
		return
	}

	res, err := con.Call("til", int32(10))
	if err != nil {
		fmt.Println("Query failed:", err)
	}
	fmt.Println("Result:", res)
}
