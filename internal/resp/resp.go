// Package resp is a Redis RESP2 parser, writer, and TCP server for TinyCache.
// The server reaches the cache only through the cache package's public API.
package resp

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
)

const (
	// DefaultMaxBulkBytes is the largest bulk string a client may declare.
	// The parser rejects a bigger length before it allocates.
	DefaultMaxBulkBytes = 32 << 20
	// DefaultMaxArrayLen matches Redis's multibulk cap (1024*1024).
	DefaultMaxArrayLen = 1 << 20
	// DefaultMaxLineBytes bounds simple strings, integers, and inline commands.
	DefaultMaxLineBytes = 64 << 10
	// DefaultMaxDepth bounds nested arrays so a crafted payload cannot overflow the stack.
	DefaultMaxDepth = 32
)

// Type is a RESP2 value kind.
type Type byte

const (
	TypeSimple Type = '+'
	TypeError  Type = '-'
	TypeInt    Type = ':'
	TypeBulk   Type = '$'
	TypeArray  Type = '*'
)

// Value is one RESP2 value. Null is set for null bulk strings and null arrays.
type Value struct {
	Type  Type
	Str   string
	Int   int64
	Null  bool
	Array []Value
}

// Limits caps allocations driven by client-declared sizes.
// Zero fields are replaced with the defaults.
type Limits struct {
	MaxBulkBytes int
	MaxArrayLen  int
	MaxLineBytes int
	MaxDepth     int
}

// DefaultLimits returns the production caps.
func DefaultLimits() Limits {
	return Limits{}.normalize()
}

func (l Limits) normalize() Limits {
	if l.MaxBulkBytes <= 0 {
		l.MaxBulkBytes = DefaultMaxBulkBytes
	}
	if l.MaxArrayLen <= 0 {
		l.MaxArrayLen = DefaultMaxArrayLen
	}
	if l.MaxLineBytes <= 0 {
		l.MaxLineBytes = DefaultMaxLineBytes
	}
	if l.MaxDepth <= 0 {
		l.MaxDepth = DefaultMaxDepth
	}
	return l
}

// ProtocolError is a malformed RESP frame. The message is the reply body
// without the leading '-', ready to write as -ERR ....
type ProtocolError struct {
	Message string
}

func (e *ProtocolError) Error() string { return e.Message }

func proto(message string) error {
	return &ProtocolError{Message: message}
}

var errLineTooLong = errors.New("resp: line too long")

// ErrEmptyCommand is a blank inline line. The server skips it.
var ErrEmptyCommand = errors.New("resp: empty command")

// ReadValue reads one RESP2 value. Short reads from r are fine; the call
// blocks until the value is complete or r returns an error.
func ReadValue(r *bufio.Reader, lim Limits) (Value, error) {
	return readValue(r, lim.normalize(), 1)
}

func readValue(r *bufio.Reader, lim Limits, depth int) (Value, error) {
	if depth > lim.MaxDepth {
		return Value{}, proto("ERR Protocol error: too many nested arrays")
	}
	kind, err := r.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch kind {
	case '+':
		line, err := readLine(r, lim.MaxLineBytes)
		if err != nil {
			return Value{}, lineErr(err)
		}
		return Value{Type: TypeSimple, Str: string(line)}, nil
	case '-':
		line, err := readLine(r, lim.MaxLineBytes)
		if err != nil {
			return Value{}, lineErr(err)
		}
		return Value{Type: TypeError, Str: string(line)}, nil
	case ':':
		line, err := readLine(r, lim.MaxLineBytes)
		if err != nil {
			return Value{}, lineErr(err)
		}
		n, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return Value{}, proto("ERR Protocol error: invalid integer")
		}
		return Value{Type: TypeInt, Int: n}, nil
	case '$':
		return readBulk(r, lim)
	case '*':
		return readArray(r, lim, depth)
	default:
		return Value{}, proto("ERR Protocol error: unknown protocol byte")
	}
}

func lineErr(err error) error {
	if errors.Is(err, errLineTooLong) {
		return proto("ERR Protocol error: line too long")
	}
	return err
}

func readBulk(r *bufio.Reader, lim Limits) (Value, error) {
	line, err := readLine(r, lim.MaxLineBytes)
	if err != nil {
		return Value{}, lineErr(err)
	}
	n, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil || n < -1 {
		return Value{}, proto("ERR Protocol error: invalid bulk length")
	}
	if n == -1 {
		return Value{Type: TypeBulk, Null: true}, nil
	}
	if n > int64(lim.MaxBulkBytes) || n > int64(^uint(0)>>1) {
		return Value{}, proto("ERR Protocol error: invalid bulk length")
	}
	buf := make([]byte, int(n))
	if _, err := io.ReadFull(r, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Value{}, io.ErrUnexpectedEOF
		}
		return Value{}, err
	}
	if err := expectCRLF(r); err != nil {
		return Value{}, err
	}
	return Value{Type: TypeBulk, Str: string(buf)}, nil
}

func readArray(r *bufio.Reader, lim Limits, depth int) (Value, error) {
	line, err := readLine(r, lim.MaxLineBytes)
	if err != nil {
		return Value{}, lineErr(err)
	}
	n, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil || n < -1 {
		return Value{}, proto("ERR Protocol error: invalid multibulk length")
	}
	if n == -1 {
		return Value{Type: TypeArray, Null: true}, nil
	}
	if n > int64(lim.MaxArrayLen) || n > int64(^uint(0)>>1) {
		return Value{}, proto("ERR Protocol error: invalid multibulk length")
	}
	arr := make([]Value, int(n))
	for i := range arr {
		el, err := readValue(r, lim, depth+1)
		if err != nil {
			return Value{}, err
		}
		arr[i] = el
	}
	return Value{Type: TypeArray, Array: arr}, nil
}

// ReadCommand reads one client command. A leading '*' is a RESP array of bulk
// strings. Anything else is one inline line, the telnet form redis-cli does
// not use but Redis still accepts.
func ReadCommand(r *bufio.Reader, lim Limits) ([]string, error) {
	lim = lim.normalize()
	b, err := r.Peek(1)
	if err != nil {
		return nil, err
	}
	if b[0] == '*' {
		v, err := readValue(r, lim, 1)
		if err != nil {
			return nil, err
		}
		return argsFromValue(v)
	}
	line, err := readLine(r, lim.MaxLineBytes)
	if err != nil {
		if errors.Is(err, errLineTooLong) {
			return nil, proto("ERR Protocol error: too big inline request")
		}
		return nil, err
	}
	return splitInline(string(line))
}

func argsFromValue(v Value) ([]string, error) {
	if v.Type != TypeArray || v.Null || len(v.Array) == 0 {
		return nil, proto("ERR Protocol error: invalid multibulk length")
	}
	args := make([]string, len(v.Array))
	for i, el := range v.Array {
		if el.Type != TypeBulk || el.Null {
			return nil, proto("ERR Protocol error: expected bulk string")
		}
		args[i] = el.Str
	}
	return args, nil
}

func readLine(r *bufio.Reader, max int) ([]byte, error) {
	if max <= 0 {
		max = DefaultMaxLineBytes
	}
	var buf []byte
	for {
		frag, err := r.ReadSlice('\n')
		if len(frag) > 0 {
			if len(buf) > max || len(frag) > max-len(buf) {
				return nil, errLineTooLong
			}
			// Copy out of the bufio buffer. ReadSlice's slice is invalid after the next read.
			buf = append(buf, frag...)
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(buf) == 0 {
					return nil, io.EOF
				}
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		break
	}
	if len(buf) < 2 || buf[len(buf)-2] != '\r' {
		return nil, proto("ERR Protocol error: expected CRLF")
	}
	return buf[:len(buf)-2], nil
}

func expectCRLF(r *bufio.Reader) error {
	b := make([]byte, 2)
	if _, err := io.ReadFull(r, b); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	if b[0] != '\r' || b[1] != '\n' {
		return proto("ERR Protocol error: expected CRLF")
	}
	return nil
}

// Writer encodes RESP2 values. It does not buffer; pair it with a bufio.Writer
// and flush from the server when a pipeline drains.
type Writer struct {
	w   io.Writer
	num []byte
	one [1]byte
}

// NewWriter writes RESP2 frames to w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// WriteCommand writes args as a RESP array of bulk strings.
func (w *Writer) WriteCommand(args ...string) error {
	if err := w.WriteArray(len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if err := w.WriteBulk(arg); err != nil {
			return err
		}
	}
	return nil
}

// WriteSimple writes a simple string. s must not contain CR or LF.
func (w *Writer) WriteSimple(s string) error {
	if strings.ContainsAny(s, "\r\n") {
		return errors.New("resp: simple string contains CR or LF")
	}
	if err := w.writeByte('+'); err != nil {
		return err
	}
	if err := writeString(w.w, s); err != nil {
		return err
	}
	return w.crlf()
}

// WriteError writes an error reply. msg is the body, usually starting with "ERR ".
// CR and LF in msg are replaced so the frame stays one line.
func (w *Writer) WriteError(msg string) error {
	msg = strings.ReplaceAll(msg, "\r", " ")
	msg = strings.ReplaceAll(msg, "\n", " ")
	if err := w.writeByte('-'); err != nil {
		return err
	}
	if err := writeString(w.w, msg); err != nil {
		return err
	}
	return w.crlf()
}

// WriteInt writes an integer.
func (w *Writer) WriteInt(n int64) error {
	if err := w.writeByte(':'); err != nil {
		return err
	}
	if err := w.writeInt(n); err != nil {
		return err
	}
	return w.crlf()
}

// WriteBulk writes a bulk string.
func (w *Writer) WriteBulk(s string) error {
	if err := w.writeByte('$'); err != nil {
		return err
	}
	if err := w.writeInt(int64(len(s))); err != nil {
		return err
	}
	if err := w.crlf(); err != nil {
		return err
	}
	if err := writeString(w.w, s); err != nil {
		return err
	}
	return w.crlf()
}

// WriteNull writes a null bulk string ($-1).
func (w *Writer) WriteNull() error {
	return writeAll(w.w, nullBulk)
}

// WriteNullArray writes a null array (*-1).
func (w *Writer) WriteNullArray() error {
	return writeAll(w.w, nullArray)
}

// WriteArray writes an array header. The caller writes n elements next.
func (w *Writer) WriteArray(n int) error {
	if err := w.writeByte('*'); err != nil {
		return err
	}
	if err := w.writeInt(int64(n)); err != nil {
		return err
	}
	return w.crlf()
}

// WriteBulkSlice writes an array of bulk strings.
func (w *Writer) WriteBulkSlice(ss []string) error {
	if err := w.WriteArray(len(ss)); err != nil {
		return err
	}
	for _, s := range ss {
		if err := w.WriteBulk(s); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeByte(b byte) error {
	w.one[0] = b
	return writeAll(w.w, w.one[:1])
}

func (w *Writer) writeInt(n int64) error {
	w.num = strconv.AppendInt(w.num[:0], n, 10)
	return writeAll(w.w, w.num)
}

func (w *Writer) crlf() error {
	return writeAll(w.w, crlfBytes)
}

var (
	crlfBytes = []byte("\r\n")
	nullBulk  = []byte("$-1\r\n")
	nullArray = []byte("*-1\r\n")
)

func writeString(w io.Writer, s string) error {
	if sw, ok := w.(interface{ WriteString(string) (int, error) }); ok {
		_, err := sw.WriteString(s)
		return err
	}
	return writeAll(w, []byte(s))
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
