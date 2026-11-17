// Package server accepts TCP connections and speaks RESP to them.
package server

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"sync"

	"kvred/db"
	"kvred/resp"
)

type Server struct {
	db     *db.DB
	logger *log.Logger

	mu     sync.Mutex
	ln     net.Listener
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

func New(d *db.DB, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Server{db: d, logger: logger, conns: make(map[net.Conn]struct{})}
}

// Serve accepts connections until Close is called. Each client gets its own
// goroutine; the DB's mutex keeps commands atomic.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			conn.Close()
			return nil
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.wg.Done()
	}()
	r := resp.NewReader(conn)
	w := bufio.NewWriterSize(conn, 16<<10)
	var out []byte
	proto := 2 // switched to 3 by HELLO 3
	for {
		args, err := r.ReadCommand()
		if err != nil {
			if errors.Is(err, resp.ErrProtocol) {
				w.Write(resp.Err("ERR Protocol error: %s", strings.TrimPrefix(err.Error(), "protocol error: ")).AppendTo(nil))
				w.Flush()
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		if strings.EqualFold(args[0], "QUIT") {
			w.Write(resp.OK().AppendTo(nil))
			w.Flush()
			return
		}
		var reply resp.Value
		if strings.EqualFold(args[0], "HELLO") {
			reply = hello(args[1:], &proto)
		} else {
			reply = s.db.Exec(args)
		}
		out = reply.AppendProto(out[:0], proto)
		if _, err := w.Write(out); err != nil {
			return
		}
		// Pipelining: if the client already sent more commands, keep going and
		// send all the replies in one write instead of one syscall per reply.
		if r.Buffered() == 0 {
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}

// hello negotiates the protocol version. It lives here rather than in the DB
// because the chosen version belongs to the connection, not the keyspace.
func hello(args []string, proto *int) resp.Value {
	if len(args) > 0 {
		switch args[0] {
		case "2":
			*proto = 2
		case "3":
			*proto = 3
		default:
			return resp.Err("NOPROTO unsupported protocol version")
		}
	}
	return resp.MapOf(
		resp.Bulk("server"), resp.Bulk("redis"),
		resp.Bulk("version"), resp.Bulk("7.0.0"),
		resp.Bulk("proto"), resp.Int(int64(*proto)),
		resp.Bulk("id"), resp.Int(1),
		resp.Bulk("mode"), resp.Bulk("standalone"),
		resp.Bulk("role"), resp.Bulk("master"),
		resp.Bulk("modules"), resp.Arr(),
	)
}

// Close stops accepting, disconnects every client, and waits for handlers.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	var err error
	if s.ln != nil {
		err = s.ln.Close()
	}
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}
