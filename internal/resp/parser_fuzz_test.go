package resp

import (
	"bufio"
	"bytes"
	"testing"
)

func FuzzRESPParser(f *testing.F) {
	f.Add([]byte("*1\r\n$4\r\nPING\r\n"))
	f.Add([]byte("PING\r\n"))
	f.Add([]byte("SET \"a b\" x\r\n"))
	f.Add([]byte("+OK\r\n-ERR x\r\n:1\r\n$-1\r\n*-1\r\n"))
	f.Add([]byte("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"))
	f.Add([]byte(""))
	f.Add([]byte("*1000000000\r\n"))
	f.Add([]byte("$999999999\r\n"))
	f.Add([]byte("*1\r\n$4\r\nPING"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64<<10 {
			data = data[:64<<10]
		}
		lim := Limits{
			MaxBulkBytes: 4096,
			MaxArrayLen:  32,
			MaxLineBytes: 1024,
			MaxDepth:     8,
		}
		r := bufio.NewReader(bytes.NewReader(data))
		for i := 0; i < 8; i++ {
			if _, err := ReadValue(r, lim); err != nil {
				break
			}
		}
		_, _ = ReadCommand(bufio.NewReader(bytes.NewReader(data)), lim)
	})
}
