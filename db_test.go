package db

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kvred/resp"
)

// fakeClock lets tests move time forward without sleeping.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestDB() (*DB, *fakeClock) {
	clock := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	return New(clock.now), clock
}

// run executes a command written as one string and returns redis-cli style output.
func run(db *DB, line string) string {
	return db.Exec(strings.Fields(line)).String()
}

func expect(t *testing.T, db *DB, script string) {
	t.Helper()
	for _, step := range strings.Split(strings.TrimSpace(script), "\n") {
		cmd, want, _ := strings.Cut(step, "=>")
		cmd, want = strings.TrimSpace(cmd), strings.TrimSpace(want)
		if got := strings.ReplaceAll(run(db, cmd), "\n", " "); got != want {
			t.Errorf("%s\n  got:  %s\n  want: %s", cmd, got, want)
		}
	}
}

func TestStrings(t *testing.T) {
	db, _ := newTestDB()
	expect(t, db, `
		GET missing           => (nil)
		SET name ada          => OK
		GET name              => "ada"
		APPEND name lovelace  => (integer) 11
		STRLEN name           => (integer) 11
		SET name x NX         => (nil)
		SET other y XX        => (nil)
		SET name grace GET    => "adalovelace"
		MSET a 1 b 2          => OK
		MGET a b nope         => 1) "1" 2) "2" 3) (nil)
		GETDEL a              => "1"
		EXISTS a b name       => (integer) 2
		DEL b name nope       => (integer) 2
		DBSIZE                => (integer) 0
		set lower case        => OK
		Get lower             => "case"
	`)
}

func TestCounters(t *testing.T) {
	db, _ := newTestDB()
	expect(t, db, `
		INCR hits                         => (integer) 1
		INCRBY hits 41                    => (integer) 42
		DECR hits                         => (integer) 41
		DECRBY hits 50                    => (integer) -9
		SET word hello                    => OK
		INCR word                         => (error) ERR value is not an integer or out of range
		SET big 9223372036854775807       => OK
		INCR big                          => (error) ERR increment or decrement would overflow
	`)
}

func TestLists(t *testing.T) {
	db, _ := newTestDB()
	expect(t, db, `
		RPUSH q a b c        => (integer) 3
		LPUSH q z y          => (integer) 5
		LRANGE q 0 -1        => 1) "y" 2) "z" 3) "a" 4) "b" 5) "c"
		LRANGE q -2 100      => 1) "b" 2) "c"
		LRANGE q 3 1         => (empty array)
		LINDEX q -1          => "c"
		LPOP q               => "y"
		RPOP q 2             => 1) "c" 2) "b"
		LLEN q               => (integer) 2
		TYPE q               => list
		GET q                => (error) WRONGTYPE Operation against a key holding the wrong kind of value
		RPOP q 5             => 1) "a" 2) "z"
		EXISTS q             => (integer) 0
		LPOP q               => (nil)
	`)
}

func TestKeysAndErrors(t *testing.T) {
	db, _ := newTestDB()
	expect(t, db, `
		MSET user:1 a user:2 b user:10 c admin:1 d  => OK
		KEYS user:?                                  => 1) "user:1" 2) "user:2"
		KEYS *:1                                     => 1) "admin:1" 2) "user:1"
		KEYS user:[^2]*                              => 1) "user:1" 2) "user:10"
		RENAME admin:1 root                          => OK
		TYPE root                                    => string
		TYPE admin:1                                 => none
		NOPE                                         => (error) ERR unknown command 'NOPE'
		GET                                          => (error) ERR wrong number of arguments for 'get' command
		SET k v EX 0                                 => (error) ERR invalid expire time in 'set' command
		SET k v BOGUS                                => (error) ERR syntax error
		PING                                         => PONG
		ECHO hello                                   => "hello"
	`)
}

func TestGlob(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "", true}, {"*", "anything", true}, {"a*c", "abbbc", true}, {"a*c", "abbb", false},
		{"h?llo", "hello", true}, {"h?llo", "hllo", false}, {"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false}, {"h[^e]llo", "hallo", true}, {"h[^e]llo", "hello", false},
		{"h[a-c]llo", "hbllo", true}, {"h\\*llo", "h*llo", true}, {"h\\*llo", "hello", false},
		{"*a*b*c*", "xxaxxbxxcxx", true}, {"*a*b*c*", "xxaxxcxxbxx", false}, {"[", "[", true},
	}
	for _, c := range cases {
		if got := globMatch(c.pattern, c.s); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", c.pattern, c.s, got, c.want)
		}
	}
}

func TestExpiry(t *testing.T) {
	db, clock := newTestDB()
	expect(t, db, `
		SET session abc EX 10    => OK
		TTL session              => (integer) 10
		PTTL session             => (integer) 10000
		SET plain x              => OK
		TTL plain                => (integer) -1
		TTL nothing              => (integer) -2
		EXPIRE plain 5           => (integer) 1
		EXPIRE nothing 5         => (integer) 0
	`)
	clock.advance(6 * time.Second)
	expect(t, db, `
		GET plain       => (nil)
		TTL session     => (integer) 4
		INCR counter    => (integer) 1
		SET session new KEEPTTL => OK
		TTL session     => (integer) 4
		PERSIST session => (integer) 1
		TTL session     => (integer) -1
		EXPIRE session -1 => (integer) 1
		EXISTS session  => (integer) 0
	`)
}

func TestActiveExpiryReclaimsUnreadKeys(t *testing.T) {
	db, clock := newTestDB()
	for i := 0; i < 1000; i++ {
		db.Exec([]string{"SET", fmt.Sprint("temp:", i), "x", "PX", "100"})
	}
	db.Exec([]string{"SET", "forever", "x"})
	clock.advance(time.Second)
	removed := db.ActiveExpireCycle()
	if removed != 1000 || run(db, "DBSIZE") != "(integer) 1" {
		t.Fatalf("removed %d keys; DBSIZE now %s", removed, run(db, "DBSIZE"))
	}
}

func TestAOFReplayRestoresState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	db, clock := newTestDB()
	aof, err := OpenAOF(path, FsyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	db.AttachAOF(aof)
	for _, cmd := range []string{
		"SET a 1", "INCRBY a 10", "RPUSH l x y z", "LPOP l", "SET gone soon EX 1",
		"SET kept later EX 3600", "SET nx-miss v NX", "SET nx-miss w NX", "DEL nobody", "GET a",
	} {
		db.Exec(strings.Fields(cmd))
	}
	aof.Close()

	// Read-only commands and no-op writes (DEL of a missing key, the second SET NX)
	// must not be logged.
	data, _ := os.ReadFile(path)
	if strings.Contains(string(data), "nobody") || strings.Count(string(data), "nx-miss") != 1 {
		t.Fatalf("AOF logged no-op commands:\n%q", data)
	}
	// Relative TTLs must be stored as absolute times.
	if strings.Contains(string(data), "EX\r\n") || !strings.Contains(string(data), "PEXPIREAT") {
		t.Fatalf("AOF kept a relative TTL:\n%q", data)
	}

	// Restart five seconds later: "gone" must stay gone; "kept" keeps its deadline.
	clock.advance(5 * time.Second)
	restored := New(clock.now)
	n, truncated, err := restored.LoadAOF(path)
	if err != nil || truncated || n == 0 {
		t.Fatalf("LoadAOF: n=%d truncated=%v err=%v", n, truncated, err)
	}
	expect(t, restored, `
		GET a          => "11"
		LRANGE l 0 -1  => 1) "y" 2) "z"
		GET gone       => (nil)
		TTL kept       => (integer) 3595
		GET nx-miss    => "v"
	`)
}

func TestAOFTruncatedTailIsRepaired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	full := string(resp.EncodeCommand("SET", "a", "1")) + string(resp.EncodeCommand("SET", "b", "2"))
	partial := full + "*3\r\n$3\r\nSET\r\n$1\r\nc" // crash mid-write
	os.WriteFile(path, []byte(partial), 0o644)

	db, _ := newTestDB()
	n, truncated, err := db.LoadAOF(path)
	if err != nil || !truncated || n != 2 {
		t.Fatalf("n=%d truncated=%v err=%v", n, truncated, err)
	}
	if data, _ := os.ReadFile(path); string(data) != full {
		t.Fatalf("file not truncated to last good command: %q", data)
	}
}

func TestAOFRewriteCompactsLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	db, clock := newTestDB()
	aof, _ := OpenAOF(path, FsyncNo)
	db.AttachAOF(aof)
	for i := 0; i < 500; i++ {
		db.Exec([]string{"INCR", "n"})
	}
	db.Exec(strings.Fields("RPUSH l a b"))
	db.Exec(strings.Fields("SET t v EX 100"))
	before, _ := os.Stat(path)
	if got := run(db, "BGREWRITEAOF"); !strings.Contains(got, "rewriting started") {
		t.Fatal(got)
	}
	db.Exec(strings.Fields("SET after rewrite")) // appends still work afterwards
	aof.Close()
	after, _ := os.Stat(path)
	if after.Size() >= before.Size()/10 {
		t.Fatalf("rewrite didn't shrink the log: %d -> %d bytes", before.Size(), after.Size())
	}
	restored := New(clock.now)
	if _, _, err := restored.LoadAOF(path); err != nil {
		t.Fatal(err)
	}
	expect(t, restored, `
		GET n         => "500"
		LRANGE l 0 -1 => 1) "a" 2) "b"
		TTL t         => (integer) 100
		GET after     => "rewrite"
	`)
}

func BenchmarkSetGet(b *testing.B) {
	db := New(nil)
	set, get := []string{"SET", "key", "value"}, []string{"GET", "key"}
	for i := 0; i < b.N; i++ {
		db.Exec(set)
		db.Exec(get)
	}
}
