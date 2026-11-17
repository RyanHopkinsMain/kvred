package resp

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	values := []Value{
		OK(), Err("ERR boom"), Int(-42), Bulk(""), Bulk("line1\r\nline2"), NullBulk(),
		NullArray(), Arr(), Arr(Bulk("a"), Int(1), Arr(Simple("nested"))),
	}
	for _, v := range values {
		wire := v.AppendTo(nil)
		got, err := NewReader(strings.NewReader(string(wire))).ReadValue()
		if err != nil {
			t.Fatalf("%q: %v", wire, err)
		}
		if got.String() != v.String() || got.Kind != v.Kind || got.Null != v.Null {
			t.Errorf("round trip of %q: got %v, want %v", wire, got, v)
		}
	}
}

func TestEncoding(t *testing.T) {
	got := string(EncodeCommand("SET", "k", "v"))
	want := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestReadCommandFormsAndOffsets(t *testing.T) {
	input := "*2\r\n$4\r\nECHO\r\n$5\r\nhi yo\r\nPING  extra   spaces\r\n"
	r := NewReader(strings.NewReader(input))
	args, err := r.ReadCommand()
	if err != nil || strings.Join(args, "|") != "ECHO|hi yo" {
		t.Fatalf("array form: %v %v", args, err)
	}
	if r.Offset() != 25 {
		t.Fatalf("offset after first command = %d, want 25", r.Offset())
	}
	args, err = r.ReadCommand()
	if err != nil || strings.Join(args, "|") != "PING|extra|spaces" {
		t.Fatalf("inline form: %v %v", args, err)
	}
	if _, err := r.ReadCommand(); err != io.EOF {
		t.Fatalf("want EOF, got %v", err)
	}
}

func TestTruncatedInputIsUnexpectedEOF(t *testing.T) {
	for _, input := range []string{"*2\r\n$3\r\nGET\r\n", "*1\r\n$10\r\nabc", "$5\r\nab"} {
		_, err := NewReader(strings.NewReader(input)).ReadValue()
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("%q: got %v, want ErrUnexpectedEOF", input, err)
		}
	}
}

func TestProtocolErrors(t *testing.T) {
	for _, input := range []string{"*x\r\n", "$-5\r\n", "*1\r\n:1\r\n", "$3\r\nabcde\r\n", "!oops\r\n"} {
		r := NewReader(strings.NewReader(input))
		var err error
		if input[0] == '*' {
			_, err = r.ReadCommand()
		} else {
			_, err = r.ReadValue()
		}
		if !errors.Is(err, ErrProtocol) {
			t.Errorf("%q: got %v, want protocol error", input, err)
		}
	}
}
