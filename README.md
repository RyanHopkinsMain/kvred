# kvred

**A Redis-compatible key-value server in Go, with no dependencies beyond the standard library.**

kvred speaks the real Redis wire protocol, so the existing tools just work:
`redis-cli`, `redis-benchmark`, and client libraries like redis-py (in both RESP2 and
RESP3 mode). It has strings, counters, lists, key expiry, pipelining, and an
append-only file that survives `kill -9`.

```text
$ go run ./cmd/kvred -aof appendonly.aof
kvred 2026/11/16 14:50:32 ready to accept connections on 127.0.0.1:6379

$ redis-cli
127.0.0.1:6379> SET greeting "hello, world"
OK
127.0.0.1:6379> INCRBY visits 42
(integer) 42
127.0.0.1:6379> SET session abc123 EX 60
OK
127.0.0.1:6379> TTL session
(integer) 60
127.0.0.1:6379> RPUSH queue job1 job2 job3
(integer) 3
127.0.0.1:6379> LPOP queue
"job1"
127.0.0.1:6379> KEYS *
1) "greeting"
2) "queue"
3) "session"
4) "visits"
127.0.0.1:6379> GET queue
(error) WRONGTYPE Operation against a key holding the wrong kind of value
```

## Performance

Measured with the real `redis-benchmark` (50 clients, one laptop-class container):

| Test | Throughput |
| --- | --- |
| `SET` | ~95,000 req/s |
| `GET` | ~100,000 req/s |
| `LPUSH` | ~90,000 req/s |
| `SET`, pipelined ×16 | ~360,000 req/s |
| `GET`, pipelined ×16 | ~970,000 req/s |

Crash recovery: after `kill -9` with 900,000 commands in the log, startup replay
took **0.65 s**, and `BGREWRITEAOF` compacted the 37 MB log to **97 bytes**.

## How it works

```
cmd/kvred/   flags, startup replay, signal handling
server/      TCP accept loop, one goroutine per client, pipelining, HELLO
db/          the keyspace, ~45 commands, expiry, AOF, glob matching
resp/        RESP2/RESP3 encoder and streaming parser
```

**Protocol** ([`resp/resp.go`](resp/resp.go)). RESP is a line-based format. Each
value starts with a type byte (`+` simple string, `-` error, `:` integer, `$` bulk
string, `*` array). Commands arrive as arrays of bulk strings, or as plain
"inline" lines, so you can also just `telnet` in and type `SET a 1`.

**Pipelining** ([`server/server.go`](server/server.go)). Clients can send many
commands without waiting for replies. The server only flushes its output buffer
when the input buffer is empty, so a burst of 16 commands gets back one `write()`
syscall instead of 16. That's where the pipelined numbers above come from.

**Atomicity.** Every command runs under one mutex. That's the Go equivalent of
Redis's single-threaded event loop: `INCR` from 20 concurrent clients never loses
an update (there's a test for exactly that).

**Expiry** ([`db/db.go`](db/db.go)) uses Redis's two-part strategy:
- *Lazy*: every read checks the key's deadline first, so an expired key is never visible.
- *Active*: ten times a second, sample 20 keys that have a TTL and delete the
  expired ones. If more than 25% were expired, go again right away. Memory gets
  reclaimed without ever scanning the whole keyspace.

**Durability** ([`db/aof.go`](db/aof.go)). Each write command that actually
changed something is appended to the log in RESP form. Three details matter:
- **Relative TTLs are logged as absolute times.** `EXPIRE k 10` is stored as
  `PEXPIREAT k <unix-ms>`. Otherwise replaying the log tomorrow would bring the
  key back to life for another 10 seconds.
- **No-ops aren't logged.** A `SET NX` that didn't set, or a `DEL` of a missing
  key, leaves no trace.
- **Torn writes are repaired.** If the process died halfway through writing a
  command, startup drops the partial tail, truncates the file to the last
  complete command, and carries on (like Redis's `aof-load-truncated yes`).

`BGREWRITEAOF` writes the minimal command set for the current state to a temp
file, fsyncs it, and atomically renames it over the old log. (Unlike Redis, the
rewrite runs in the foreground; it's fast at this scale.)

**A bug the benchmark found.** Lists started out as Go slices, and `LPUSH`
prepended by copying the whole slice: O(n) per push. `redis-benchmark` showed
LPUSH at ~1,500 req/s while everything else ran at ~90,000. Lists are now a
growable ring buffer ([`db/deque.go`](db/deque.go)) with O(1) pushes and pops at
both ends. A randomized test checks it against a plain slice.

## Supported commands

| Group | Commands |
| --- | --- |
| Strings | `GET SET` (with `EX PX NX XX GET KEEPTTL`) `SETEX PSETEX GETDEL MGET MSET APPEND STRLEN INCR DECR INCRBY DECRBY` |
| Lists | `LPUSH RPUSH LPOP RPOP` (with count) `LLEN LRANGE LINDEX` |
| Keys | `DEL UNLINK EXISTS TYPE KEYS RENAME EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL PERSIST` |
| Server | `PING ECHO HELLO CLIENT CONFIG GET INFO DBSIZE FLUSHALL FLUSHDB SELECT 0 BGREWRITEAOF QUIT` |

`KEYS` supports Redis glob syntax: `*`, `?`, `[abc]`, `[a-z]`, `[^x]`, `\` escapes.

## Running

```bash
go run ./cmd/kvred                                    # in-memory only
go run ./cmd/kvred -aof appendonly.aof                # durable, fsync every second
go run ./cmd/kvred -aof data.aof -appendfsync always  # fsync every write
go run ./cmd/kvred -port 6380 -bind 0.0.0.0
```

## Tests

```bash
go test -race ./...
```

The tests cover command semantics against Redis's documented behaviour, expiry
with a fake clock (no sleeps), AOF replay, truncation repair and rewrite, a
10,000-command pipeline, 20 concurrent clients incrementing one counter, protocol
errors, and RESP3 negotiation.

## Not implemented

Hashes, sets and sorted sets, transactions (`MULTI`/`EXEC`), pub/sub,
replication, RDB snapshots, and `AUTH`.

## License

MIT
