// Package db is kvred's keyspace: an in-memory map of keys to strings or lists,
// with per-key expiry and an optional append-only file for durability.
package db

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"kvred/resp"
)

type kind int

const (
	kindString kind = iota
	kindList
)

type entry struct {
	kind     kind
	str      string
	list     deque
	expireAt int64 // unix milliseconds; 0 means no expiry
}

// DB is safe for concurrent use. Commands run one at a time under a single
// mutex, which (like Redis's single thread) makes every command atomic.
type DB struct {
	mu       sync.Mutex
	data     map[string]*entry
	volatile map[string]struct{} // keys that have a TTL, for active expiry
	now      func() time.Time
	aof      *AOF
	started  time.Time

	dirty     int64 // bumped by every mutation; tells Exec whether to log
	commands  int64
	expired   int64
	replaying bool
}

// New returns an empty database. Pass a clock for tests, or nil for time.Now.
func New(clock func() time.Time) *DB {
	if clock == nil {
		clock = time.Now
	}
	return &DB{
		data:     make(map[string]*entry),
		volatile: make(map[string]struct{}),
		now:      clock,
		started:  clock(),
	}
}

type command struct {
	arity int  // >0: exact arg count including the name; <0: at least -arity
	write bool // logged to the AOF when it changes something
	fn    func(db *DB, args []string) resp.Value
}

var commands map[string]command

// Exec runs one command (name plus arguments) and returns the reply.
func (db *DB) Exec(args []string) resp.Value {
	if len(args) == 0 {
		return resp.Err("ERR empty command")
	}
	name := strings.ToUpper(args[0])
	cmd, ok := commands[name]
	if !ok {
		return resp.Err("ERR unknown command '%s'", args[0])
	}
	if (cmd.arity > 0 && len(args) != cmd.arity) || (cmd.arity < 0 && len(args) < -cmd.arity) {
		return resp.Err("ERR wrong number of arguments for '%s' command", strings.ToLower(name))
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	db.commands++
	before := db.dirty
	reply := cmd.fn(db, args[1:])
	if cmd.write && db.dirty != before && db.aof != nil && !db.replaying {
		for _, logged := range db.propagate(name, args[1:]) {
			if err := db.aof.Append(logged); err != nil {
				return resp.Err("ERR failed to write to the append-only file: %v", err)
			}
		}
	}
	return reply
}

// propagate rewrites a command into the deterministic form stored in the AOF.
// Relative TTLs become absolute timestamps, otherwise replaying "EXPIRE k 10"
// a day later would bring a long-dead key back to life.
func (db *DB) propagate(name string, args []string) [][]string {
	expiry := func(key string) [][]string {
		if e := db.data[key]; e != nil && e.expireAt > 0 {
			return [][]string{{"PEXPIREAT", key, fmt.Sprint(e.expireAt)}}
		}
		return nil
	}
	switch name {
	case "SET": // drop EX/PX/NX/GET...; the outcome is what matters
		return append([][]string{{"SET", args[0], args[1]}}, expiry(args[0])...)
	case "SETEX", "PSETEX":
		return append([][]string{{"SET", args[0], args[2]}}, expiry(args[0])...)
	case "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT":
		if db.data[args[0]] == nil { // a TTL in the past deleted the key
			return [][]string{{"DEL", args[0]}}
		}
		return expiry(args[0])
	}
	return [][]string{append([]string{name}, args...)}
}

func (db *DB) nowMs() int64 { return db.now().UnixMilli() }

// lookup returns the live entry for key, deleting it first if it has expired.
// This "lazy expiry" means an expired key is never observable, even between
// active-expiry sweeps.
func (db *DB) lookup(key string) *entry {
	e := db.data[key]
	if e != nil && e.expireAt > 0 && e.expireAt <= db.nowMs() {
		db.remove(key)
		db.expired++
		return nil
	}
	return e
}

func (db *DB) remove(key string) {
	delete(db.data, key)
	delete(db.volatile, key)
}

func (db *DB) setExpire(key string, e *entry, at int64) {
	e.expireAt = at
	if at > 0 {
		db.volatile[key] = struct{}{}
	} else {
		delete(db.volatile, key)
	}
}

// ActiveExpireCycle is Redis's probabilistic sweep: sample up to 20 keys that
// have a TTL, delete the expired ones, and go again if more than a quarter of
// the sample had expired. Memory held by keys nobody reads gets reclaimed
// without ever scanning the whole keyspace.
func (db *DB) ActiveExpireCycle() (removed int) {
	for {
		db.mu.Lock()
		now, sampled, expired := db.nowMs(), 0, 0
		for key := range db.volatile { // Go randomises map iteration order
			if sampled == 20 {
				break
			}
			sampled++
			if db.data[key].expireAt <= now {
				db.remove(key)
				expired++
			}
		}
		db.expired += int64(expired)
		db.mu.Unlock()
		removed += expired
		if sampled == 0 || expired*4 <= sampled {
			return removed
		}
	}
}

// RunExpiry calls ActiveExpireCycle ten times a second until stop is closed.
func (db *DB) RunExpiry(stop <-chan struct{}) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			db.ActiveExpireCycle()
		}
	}
}

// snapshot returns commands that rebuild the current keyspace (for AOF rewrites).
func (db *DB) snapshot() [][]string {
	keys := make([]string, 0, len(db.data))
	for k := range db.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	now := db.nowMs()
	var out [][]string
	for _, k := range keys {
		e := db.data[k]
		if e.expireAt > 0 && e.expireAt <= now {
			continue
		}
		switch e.kind {
		case kindString:
			out = append(out, []string{"SET", k, e.str})
		case kindList:
			out = append(out, append([]string{"RPUSH", k}, e.list.Range(0, e.list.Len()-1)...))
		}
		if e.expireAt > 0 {
			out = append(out, []string{"PEXPIREAT", k, fmt.Sprint(e.expireAt)})
		}
	}
	return out
}
