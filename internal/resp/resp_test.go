package resp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

type chunkReader struct {
	data []byte
	n    int
	off  int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.off >= len(c.data) {
		return 0, io.EOF
	}
	n := c.n
	if n <= 0 {
		n = 1
	}
	if n > len(p) {
		n = len(p)
	}
	if c.off+n > len(c.data) {
		n = len(c.data) - c.off
	}
	copy(p, c.data[c.off:c.off+n])
	c.off += n
	return n, nil
}

func readValues(t *testing.T, raw string, chunk int, lim Limits) []Value {
	t.Helper()
	r := bufio.NewReader(&chunkReader{data: []byte(raw), n: chunk})
	var out []Value
	for {
		v, err := ReadValue(r, lim)
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("ReadValue(%q): %v", raw, err)
		}
		out = append(out, v)
	}
}

func TestWriterReaderRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteSimple("OK"); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteError("ERR no"); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteInt(-7); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteNull(); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBulk("a\r\nb"); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteNullArray(); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteArray(2); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBulk("x"); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteInt(3); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteCommand("GET", "name"); err != nil {
		t.Fatal(err)
	}

	got := readValues(t, buf.String(), 1, DefaultLimits())
	if len(got) != 8 {
		t.Fatalf("got %d values", len(got))
	}
	if got[0].Type != TypeSimple || got[0].Str != "OK" {
		t.Fatalf("simple: %+v", got[0])
	}
	if got[1].Type != TypeError || got[1].Str != "ERR no" {
		t.Fatalf("error: %+v", got[1])
	}
	if got[2].Type != TypeInt || got[2].Int != -7 {
		t.Fatalf("int: %+v", got[2])
	}
	if got[3].Type != TypeBulk || !got[3].Null {
		t.Fatalf("null bulk: %+v", got[3])
	}
	if got[4].Type != TypeBulk || got[4].Str != "a\r\nb" {
		t.Fatalf("bulk: %+v", got[4])
	}
	if got[5].Type != TypeArray || !got[5].Null {
		t.Fatalf("null array: %+v", got[5])
	}
	if got[6].Type != TypeArray || len(got[6].Array) != 2 || got[6].Array[0].Str != "x" || got[6].Array[1].Int != 3 {
		t.Fatalf("array: %+v", got[6])
	}
	if got[7].Type != TypeArray || len(got[7].Array) != 2 || got[7].Array[0].Str != "GET" || got[7].Array[1].Str != "name" {
		t.Fatalf("command: %+v", got[7])
	}
}

func TestParserPartialReadsAndPipeline(t *testing.T) {
	raw := "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n$0\r\n\r\n*-1\r\n"
	got := readValues(t, raw, 1, DefaultLimits())
	if len(got) != 4 {
		t.Fatalf("got %d values, want 4", len(got))
	}
	if len(got[0].Array) != 1 || got[0].Array[0].Str != "PING" {
		t.Fatalf("ping: %+v", got[0])
	}
	if len(got[1].Array) != 2 || got[1].Array[0].Str != "GET" || got[1].Array[1].Str != "key" {
		t.Fatalf("get: %+v", got[1])
	}
	if got[2].Type != TypeBulk || got[2].Null || got[2].Str != "" {
		t.Fatalf("empty bulk: %+v", got[2])
	}
	if !got[3].Null || got[3].Type != TypeArray {
		t.Fatalf("null array: %+v", got[3])
	}

	r := bufio.NewReader(&chunkReader{data: []byte(raw), n: 3})
	args, err := ReadCommand(r, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(args, " ") != "PING" {
		t.Fatalf("args %v", args)
	}
	args, err = ReadCommand(r, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(args, " ") != "GET key" {
		t.Fatalf("args %v", args)
	}
}

func TestParserRejectsDeclaredLengthsBeforeAlloc(t *testing.T) {
	lim := Limits{MaxBulkBytes: 8, MaxArrayLen: 4, MaxLineBytes: 64, MaxDepth: 8}
	cases := []string{
		"$100\r\n",
		"$-2\r\n",
		"$nope\r\n",
		"*100\r\n",
		"*-2\r\n",
		"*nope\r\n",
		":nope\r\n",
		"+\n",
		"@nope\r\n",
	}
	for _, raw := range cases {
		start := time.Now()
		_, err := ReadValue(bufio.NewReader(strings.NewReader(raw)), lim)
		if time.Since(start) > time.Second {
			t.Fatalf("%q took too long, likely allocating", raw)
		}
		var pe *ProtocolError
		if !errors.As(err, &pe) {
			t.Fatalf("%q: got %v, want ProtocolError", raw, err)
		}
	}

	start := time.Now()
	_, err := ReadValue(bufio.NewReader(strings.NewReader("*1000000000\r\n")), DefaultLimits())
	if time.Since(start) > time.Second {
		t.Fatal("huge array length took too long")
	}
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("huge array: %v", err)
	}
	if !strings.Contains(pe.Message, "multibulk") {
		t.Fatalf("message %q", pe.Message)
	}
}

func TestParserNestedDepthAndIncomplete(t *testing.T) {
	var deep strings.Builder
	for i := 0; i < 40; i++ {
		deep.WriteString("*1\r\n")
	}
	deep.WriteString("+OK\r\n")
	_, err := ReadValue(bufio.NewReader(strings.NewReader(deep.String())), DefaultLimits())
	var pe *ProtocolError
	if !errors.As(err, &pe) || !strings.Contains(pe.Message, "nested") {
		t.Fatalf("deep nest: %v", err)
	}

	_, err = ReadValue(bufio.NewReader(strings.NewReader("*1\r\n$3\r\nPI")), DefaultLimits())
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("incomplete: %v", err)
	}

	v, err := ReadValue(bufio.NewReader(strings.NewReader("*1\r\n*1\r\n$3\r\nfoo\r\n")), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Array) != 1 || len(v.Array[0].Array) != 1 || v.Array[0].Array[0].Str != "foo" {
		t.Fatalf("nested: %+v", v)
	}
}

func TestParserNeverPanics(t *testing.T) {
	samples := []string{
		"",
		"*",
		"$",
		"*-1\r\n",
		"$-1\r\n",
		":\r\n",
		"+ok\r\n",
		"*1\r\n$-1\r\n",
		"*1\r\n$3\r\nfo",
		"\r\n",
		"*0\r\n",
		"$4\r\nPING",
		"*1\r\n+PING\r\n",
		string([]byte{0xff, 0x00, 0x01}),
	}
	for _, raw := range samples {
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					t.Fatalf("panic on %q: %v", raw, rec)
				}
			}()
			r := bufio.NewReader(strings.NewReader(raw))
			_, _ = ReadValue(r, DefaultLimits())
			r = bufio.NewReader(strings.NewReader(raw))
			_, _ = ReadCommand(r, DefaultLimits())
		}()
	}
}

func TestInlineCommands(t *testing.T) {
	cases := []struct {
		line string
		args []string
		err  bool
	}{
		{line: "PING", args: []string{"PING"}},
		{line: "SET mykey myvalue", args: []string{"SET", "mykey", "myvalue"}},
		{line: `SET "my key" hello`, args: []string{"SET", "my key", "hello"}},
		{line: `SET 'a b' c`, args: []string{"SET", "a b", "c"}},
		{line: `SET "a\"b" c`, args: []string{"SET", `a"b`, "c"}},
		{line: `SET foo"bar"baz x`, args: []string{"SET", "foobarbaz", "x"}},
		{line: `ECHO \x41`, args: []string{"ECHO", "A"}},
		{line: `"unterminated`, err: true},
		{line: "   \t  ", err: true},
	}
	for _, tc := range cases {
		args, err := splitInline(tc.line)
		if tc.err {
			if err == nil {
				t.Fatalf("%q: want error", tc.line)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%q: %v", tc.line, err)
		}
		if strings.Join(args, "|") != strings.Join(tc.args, "|") {
			t.Fatalf("%q: got %q want %q", tc.line, args, tc.args)
		}
	}

	r := bufio.NewReader(strings.NewReader("PING\r\nSET a b\r\n"))
	args, err := ReadCommand(r, DefaultLimits())
	if err != nil || len(args) != 1 || args[0] != "PING" {
		t.Fatalf("inline ping: %v %v", args, err)
	}
	args, err = ReadCommand(r, DefaultLimits())
	if err != nil || strings.Join(args, " ") != "SET a b" {
		t.Fatalf("inline set: %v %v", args, err)
	}

	_, err = ReadCommand(bufio.NewReader(strings.NewReader("*1\r\n+PING\r\n")), DefaultLimits())
	var pe *ProtocolError
	if !errors.As(err, &pe) || !strings.Contains(pe.Message, "bulk") {
		t.Fatalf("simple string in array: %v", err)
	}
}

func TestGlob(t *testing.T) {
	cases := []struct {
		pattern string
		name    string
		want    bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"user:*", "user:1", true},
		{"user:*", "other", false},
		{"user:?", "user:1", true},
		{"user:?", "user:12", false},
		{"h[ae]llo", "hello", true},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hxllo", false},
		{"h[a-c]llo", "hbllo", true},
		{"h[a-c]llo", "hdllo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{`\*`, "*", true},
		{`\*`, "a", false},
		{"a*a*a*a*b", "aaab", false},
		{"a*a*a*a*b", "axaaxaab", true},
		{"User:*", "user:1", false},
	}
	for _, tc := range cases {
		if got := matchGlob(tc.pattern, tc.name); got != tc.want {
			t.Errorf("matchGlob(%q, %q) = %v, want %v", tc.pattern, tc.name, got, tc.want)
		}
	}

	var pattern strings.Builder
	for i := 0; i < 80; i++ {
		pattern.WriteString("a*")
	}
	pattern.WriteByte('b')
	if matchGlob(pattern.String(), strings.Repeat("a", 80)) {
		t.Fatal("expected no match")
	}
	if !matchGlob(pattern.String(), strings.Repeat("a", 80)+"b") {
		t.Fatal("expected match")
	}
}
