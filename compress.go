package kdb

import (
	"bytes"
	"encoding/binary"
)

// Compress b using Q IPC compression
func Compress(b []byte) (dst []byte) {
	if len(b) <= 17 {
		return b
	}
	i := byte(0)
	//j := int32(len(b))
	f, h0, h := int32(0), int32(0), int32(0)
	g := false
	dst = make([]byte, len(b)/2)
	lenbuf := make([]byte, 4)
	c := 12
	d := c
	e := len(dst)
	p := 0
	q, r, s0 := int32(0), int32(0), int32(0)
	s := int32(8)
	t := int32(len(b))
	a := make([]int32, 256)
	copy(dst[:4], b[:4])
	dst[2] = 1
	binary.LittleEndian.PutUint32(lenbuf, uint32(len(b)))
	copy(dst[8:], lenbuf)
	for ; s < t; i *= 2 {
		if 0 == i {
			if d > e-17 {
				return b
			}
			i = 1
			dst[c] = byte(f)
			c = d
			d++
			f = 0
		}

		g = (s > t-3)
		if !g {
			h = int32(0xff & (b[s] ^ b[s+1]))
			p = int(a[h])
			g = (0 == p) || (0 != (b[s] ^ b[p]))
		}

		if 0 < s0 {
			a[h0] = s0
			s0 = 0
		}
		if g {
			h0 = h
			s0 = s
			dst[d] = b[s]
			d++
			s++
		} else {
			a[h] = s
			f |= int32(i)
			p += 2
			s += 2
			r = s
			q = min32(s+255, t)
			for ; b[p] == b[s] && s+1 < q; s++ {
				p++
			}
			dst[d] = byte(h)
			d++
			dst[d] = byte(s - r)
			d++
		}
	}
	dst[c] = byte(f)
	binary.LittleEndian.PutUint32(lenbuf, uint32(d))
	copy(dst[4:], lenbuf)
	return dst[:d:d]
}

func min32(a, b int32) int32 {
	if a > b {
		return b
	}
	return a
}

// Uncompress byte array compressed with Q IPC compression
func Uncompress(b []byte) (dst []byte) {
	if len(b) < 4+1 {
		return b
	}
	n, r, f, s := int32(0), int32(0), int32(0), int32(8)
	p := s
	i := int16(0)
	var usize int32
	binary.Read(bytes.NewReader(b[0:4]), binary.LittleEndian, &usize)
	dst = make([]byte, usize)
	d := int32(4)
	aa := make([]int32, 256)
	for int(s) < len(dst) {
		if i == 0 {
			f = 0xff & int32(b[d])
			d++
			i = 1
		}
		if (f & int32(i)) != 0 {
			r = aa[0xff&int32(b[d])]
			d++
			dst[s] = dst[r]
			s++
			r++
			dst[s] = dst[r]
			s++
			r++
			n = 0xff & int32(b[d])
			d++
			for m := int32(0); m < n; m++ {
				dst[s+m] = dst[r+m]
			}
		} else {
			dst[s] = b[d]
			s++
			d++
		}
		for p < s-1 {
			aa[(0xff&int32(dst[p]))^(0xff&int32(dst[p+1]))] = p
			p++
		}
		if (f & int32(i)) != 0 {
			s += n
			p = s
		}
		i *= 2
		if i == 256 {
			i = 0
		}
	}
	return dst
}
