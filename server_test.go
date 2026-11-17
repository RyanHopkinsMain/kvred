package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"kvred/db"
	"kvred/resp"
)

func start(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := New(db.New(nil), nil)
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return ln.Addr().String()
}

type client struct {
	conn net.Conn
	r    *resp.Reader
	w    *bufio.Writer
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	t.Cleanup(func() { conn.Close() })
	return &client{conn, resp.NewReader(conn), bufio.NewWriter(conn)}
}

func (c *client) send(args ...string) { c.w.Write(resp.EncodeCommand(args...)) }

func (c *client) do(t *testing.T, args ...string) string {
	t.Helper()
	c.send(args...)
	c.w.Flush()
	v, err := c.r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	return v.String()
}

func TestRoundTrip(t *testing.T) {
	c := dial(t, start(t))
	if got := c.do(t, "SET", "greeting", "hello world"); got != "OK" {
		t.Fatal(got)
	}
	if got := c.do(t, "GET", "greeting"); got != `"hello world"` {
		t.Fatal(got)
	}
}

func TestPipelining(t *testing.T) {
	c := dial(t, start(t))
	const n = 10_000
	for i := 0; i < n; i++ {
		c.send("INCR", "counter")
	}
	c.w.Flush()
	for i := 1; i <= n; i++ {
		v, err := c.r.ReadValue()
		if err != nil || v.Int != int64(i) {
			t.Fatalf("reply %d: %v %v", i, v, err)
		}
	}
}

func TestInlineCommands(t *testing.T) {
	c := dial(t, start(t))
	fmt.Fprint(c.conn, "SET x 42\r\nGET x\r\n")
	for _, want := range []string{"OK", `"42"`} {
		v, err := c.r.ReadValue()
		if err != nil || v.String() != want {
			t.Fatalf("got %v %v, want %s", v, err, want)
		}
	}
}

func TestConcurrentClientsAreAtomic(t *testing.T) {
	addr := start(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := dial(t, addr)
			for j := 0; j < 200; j++ {
				c.send("INCR", "shared")
			}
			c.w.Flush()
			for j := 0; j < 200; j++ {
				if _, err := c.r.ReadValue(); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := dial(t, addr).do(t, "GET", "shared"); got != `"4000"` {
		t.Fatalf("lost updates: %s", got)
	}
}

func TestProtocolErrorClosesConnection(t *testing.T) {
	c := dial(t, start(t))
	fmt.Fprint(c.conn, "*1\r\n$abc\r\n")
	v, err := c.r.ReadValue()
	if err != nil || !strings.HasPrefix(v.Str, "ERR Protocol error") {
		t.Fatalf("got %v %v", v, err)
	}
	if _, err := c.r.ReadValue(); err == nil {
		t.Fatal("connection should be closed")
	}
}

func TestQuit(t *testing.T) {
	c := dial(t, start(t))
	if got := c.do(t, "QUIT"); got != "OK" {
		t.Fatal(got)
	}
	if _, err := c.r.ReadValue(); err == nil {
		t.Fatal("connection should be closed after QUIT")
	}
}

func TestHelloSwitchesToRESP3(t *testing.T) {
	c := dial(t, start(t))
	c.send("HELLO", "3")
	c.send("GET", "missing")
	c.w.Flush()
	raw := make([]byte, 4096)
	time.Sleep(50 * time.Millisecond)
	n, _ := c.conn.Read(raw)
	got := string(raw[:n])
	if !strings.HasPrefix(got, "%7\r\n") || !strings.HasSuffix(got, "_\r\n") {
		t.Fatalf("expected a RESP3 map then a RESP3 null, got %q", got)
	}
	if got := c.do(t, "HELLO", "4"); !strings.Contains(got, "NOPROTO") {
		t.Fatal(got)
	}
}
