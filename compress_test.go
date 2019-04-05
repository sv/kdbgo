package kdb

import (
	"bufio"
	"bytes"
	//"fmt"
	"math/rand"
	"reflect"
	"testing"
)

// -18!2000#1b
var bytes2KTrue = []byte{0x01, 0x00, 0x01, 0x00, 0x26, 0x00, 0x00, 0x00, 0xde, 0x07, 0x00, 0x00, 0x00, 0x01, 0x00, 0xd0, 0x07, 0x00, 0x00, 0x01, 0x01, 0xff, 0x00, 0xff, 0x00, 0xff, 0x00, 0xff, 0x00, 0xff, 0x00, 0xff, 0x00, 0xff, 0x00, 0xff, 0x00, 0xc5}

func TestCompress(t *testing.T) {
	true2K := make([]bool, 2000)
	for i := 0; i < len(true2K); i++ {
		true2K[i] = true
	}
	buf := new(bytes.Buffer)
	_ = Encode(buf, ASYNC, &K{KB, NONE, true2K})
	bc := buf.Bytes()
	if !bytes.Equal(bc, bytes2KTrue) {
		t.Errorf("Compress failed expected/got: \n%v\n%v\n", bytes2KTrue, bc)
	}
}

func TestUncompress(t *testing.T) {
	true2K := make([]bool, 2000)
	for i := 0; i < len(true2K); i++ {
		true2K[i] = true
	}
	buf := new(bytes.Buffer)
	_ = Encode(buf, ASYNC, &K{KB, NONE, true2K})
	uc2 := Uncompress(bytes2KTrue[8:])
	uc1 := Uncompress(buf.Bytes()[8:])
	if !bytes.Equal(uc1, uc2) {
		t.Errorf("Uncompress failed expected/got: \n%v\n%v\n", buf.Bytes(), bytes2KTrue)
	}
}

func generateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return nil, err
	}

	return b, nil
}

func TestCompressRoundtrip(t *testing.T) {
	true2K := make([]bool, 2000)
	for i := 0; i < len(true2K); i++ {
		true2K[i] = true
	}
	k1 := &K{KB, NONE, true2K}
	buf := new(bytes.Buffer)
	Encode(buf, ASYNC, k1)
	k2, _, _ := Decode(bufio.NewReader(buf))
	if !reflect.DeepEqual(k1, k2) {
		t.Errorf("Roundtrip failed expected/got: \n%v\n%v\n", k1, k2)
	}
}

func BenchmarkUncompress(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Uncompress(bytes2KTrue[8:])
	}
}
