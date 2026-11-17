// Package resp implements RESP2, the Redis Serialization Protocol.
//
// RESP is a tiny, line-oriented format. Every value starts with a type byte:
//
//	+OK\r\n                      simple string
//	-ERR something\r\n           error
//	:42\r\n                      integer
//	$5\r\nhello\r\n              bulk string ($-1\r\n is null)
//	*2\r\n$3\r\nGET\r\n$1\r\nk\r\n   array (*-1\r\n is a null array)
//
// Clients send commands as arrays of bulk strings. For hand-typing over telnet,
// Redis also accepts "inline" commands: a plain line of space-separated words.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Kind is the RESP type byte.
type Kind byte

const (
	SimpleString Kind = '+'
	Error        Kind = '-'
	Integer      Kind = ':'
	BulkString   Kind = '$'
	Array        Kind = '*'
	Map          Kind = '%' // RESP3 only; sent as a flat array to RESP2 clients
)

// Value is any RESP value. Null bulk strings and null arrays set Null.
type Value struct {
	Kind  Kind
	Str   string
	Int   int64
	Elems []Value
	Null  bool
}

func OK() Value                { return Simple("OK") }
func Simple(s string) Value    { return Value{Kind: SimpleString, Str: s} }
func Int(n int64) Value        { return Value{Kind: Integer, Int: n} }
func Bulk(s string) Value      { return Value{Kind: BulkString, Str: s} }
func NullBulk() Value          { return Value{Kind: BulkString, Null: true} }
func NullArray() Value         { return Value{Kind: Array, Null: true} }
func Arr(elems ...Value) Value { return Value{Kind: Array, Elems: elems} }

// Err builds an error reply. By convention the first word is an error code
// such as ERR or WRONGTYPE.
func Err(format string, args ...any) Value {
	return Value{Kind: Error, Str: fmt.Sprintf(format, args...)}
}

// MapOf builds a map from alternating keys and values.
func MapOf(kv ...Value) Value { return Value{Kind: Map, Elems: kv} }

// BulkArray turns a slice of strings into an array of bulk strings.
func BulkArray(items []string) Value {
	elems := make([]Value, len(items))
	for i, s := range items {
		elems[i] = Bulk(s)
	}
	return Arr(elems...)
}

func (v Value) IsError() bool { return v.Kind == Error }

// AppendTo appends the RESP2 wire encoding of v to buf.
func (v Value) AppendTo(buf []byte) []byte { return v.AppendProto(buf, 2) }

// AppendProto encodes v for a client speaking RESP2 or RESP3. The differences
// that matter here: RESP3 has a single null type ("_") and a real map type.
func (v Value) AppendProto(buf []byte, proto int) []byte {
	if proto == 3 && v.Null {
		return append(buf, "_\r\n"...)
	}
	switch v.Kind {
	case SimpleString, Error:
		buf = append(buf, byte(v.Kind))
		buf = append(buf, v.Str...)
	case Integer:
		buf = append(buf, ':')
		buf = strconv.AppendInt(buf, v.Int, 10)
	case BulkString:
		if v.Null {
			return append(buf, "$-1\r\n"...)
		}
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(v.Str)), 10)
		buf = append(buf, "\r\n"...)
		buf = append(buf, v.Str...)
	case Array:
		if v.Null {
			return append(buf, "*-1\r\n"...)
		}
		buf = append(buf, '*')
		buf = strconv.AppendInt(buf, int64(len(v.Elems)), 10)
		buf = append(buf, "\r\n"...)
		for _, e := range v.Elems {
			buf = e.AppendProto(buf, proto)
		}
		return buf
	case Map:
		n := len(v.Elems)
		if proto == 3 {
			buf = append(buf, '%')
			n /= 2
		} else {
			buf = append(buf, '*')
		}
		buf = strconv.AppendInt(buf, int64(n), 10)
		buf = append(buf, "\r\n"...)
		for _, e := range v.Elems {
			buf = e.AppendProto(buf, proto)
		}
		return buf
	}
	return append(buf, "\r\n"...)
}

// EncodeCommand encodes a command the way clients send it.
func EncodeCommand(args ...string) []byte {
	return BulkArray(args).AppendTo(nil)
}

// String renders v the way redis-cli does, which makes test failures readable.
func (v Value) String() string {
	switch v.Kind {
	case SimpleString:
		return v.Str
	case Error:
		return "(error) " + v.Str
	case Integer:
		return "(integer) " + strconv.FormatInt(v.Int, 10)
	case BulkString:
		if v.Null {
			return "(nil)"
		}
		return strconv.Quote(v.Str)
	case Array, Map:
		if v.Null {
			return "(nil)"
		}
		if len(v.Elems) == 0 {
			return "(empty array)"
		}
		parts := make([]string, len(v.Elems))
		for i, e := range v.Elems {
			parts[i] = fmt.Sprintf("%d) %s", i+1, e)
		}
		return strings.Join(parts, "\n")
	}
	return "?"
}

// ErrProtocol is returned for malformed input; the server replies and hangs up.
var ErrProtocol = errors.New("protocol error")

const (
	maxBulkLen  = 512 << 20 // Redis's own limit
	maxArrayLen = 1 << 20
	maxInline   = 64 << 10
)

// Reader decodes RESP from a stream and tracks how many bytes it has consumed,
// which the append-only file uses to find where a truncated tail begins.
type Reader struct {
	br     *bufio.Reader
	offset int64
}

func NewReader(r io.Reader) *Reader { return &Reader{br: bufio.NewReaderSize(r, 16<<10)} }

// Buffered reports bytes already read from the network but not yet parsed.
// The server flushes replies only when this hits zero, which batches replies
// to pipelined commands into a single write.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// Offset is the number of bytes consumed by fully parsed values.
func (r *Reader) Offset() int64 { return r.offset }

// ReadCommand reads one client command, either RESP-array or inline form.
func (r *Reader) ReadCommand() ([]string, error) {
	first, err := r.br.Peek(1)
	if err != nil {
		return nil, err
	}
	if first[0] != byte(Array) {
		line, err := r.readLine(maxInline)
		if err != nil {
			return nil, err
		}
		return strings.Fields(line), nil
	}
	v, err := r.ReadValue()
	if err != nil {
		return nil, err
	}
	args := make([]string, len(v.Elems))
	for i, e := range v.Elems {
		if e.Kind != BulkString || e.Null {
			return nil, fmt.Errorf("%w: expected bulk string in command", ErrProtocol)
		}
		args[i] = e.Str
	}
	return args, nil
}

// ReadValue reads any RESP value (used by clients and tests to parse replies).
func (r *Reader) ReadValue() (Value, error) {
	line, err := r.readLine(maxInline)
	if err != nil {
		return Value{}, err
	}
	if line == "" {
		return Value{}, fmt.Errorf("%w: empty line", ErrProtocol)
	}
	kind, rest := Kind(line[0]), line[1:]
	switch kind {
	case SimpleString, Error:
		return Value{Kind: kind, Str: rest}, nil
	case Integer:
		n, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			return Value{}, fmt.Errorf("%w: bad integer %q", ErrProtocol, rest)
		}
		return Int(n), nil
	case BulkString:
		n, err := strconv.Atoi(rest)
		if err != nil || n < -1 || n > maxBulkLen {
			return Value{}, fmt.Errorf("%w: bad bulk length %q", ErrProtocol, rest)
		}
		if n == -1 {
			return NullBulk(), nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return Value{}, unexpected(err)
		}
		if buf[n] != '\r' || buf[n+1] != '\n' {
			return Value{}, fmt.Errorf("%w: bulk string not terminated by CRLF", ErrProtocol)
		}
		r.offset += int64(n + 2)
		return Bulk(string(buf[:n])), nil
	case '_':
		return NullBulk(), nil
	case Array, Map:
		n, err := strconv.Atoi(rest)
		if err != nil || n < -1 || n > maxArrayLen {
			return Value{}, fmt.Errorf("%w: bad array length %q", ErrProtocol, rest)
		}
		if n == -1 {
			return NullArray(), nil
		}
		if kind == Map {
			n *= 2
		}
		elems := make([]Value, n)
		for i := range elems {
			if elems[i], err = r.ReadValue(); err != nil {
				return Value{}, unexpected(err)
			}
		}
		return Value{Kind: kind, Elems: elems}, nil
	}
	return Value{}, fmt.Errorf("%w: unknown type byte %q", ErrProtocol, line[0])
}

func (r *Reader) readLine(limit int) (string, error) {
	var sb strings.Builder
	for {
		chunk, err := r.br.ReadSlice('\n')
		sb.Write(chunk)
		if sb.Len() > limit {
			return "", fmt.Errorf("%w: line too long", ErrProtocol)
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil {
			if sb.Len() > 0 {
				return "", unexpected(err)
			}
			return "", err
		}
		break
	}
	r.offset += int64(sb.Len())
	return strings.TrimSuffix(strings.TrimSuffix(sb.String(), "\n"), "\r"), nil
}

func unexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
