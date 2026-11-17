package db

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"kvred/resp"
)

var (
	errWrongType = resp.Err("WRONGTYPE Operation against a key holding the wrong kind of value")
	errNotInt    = resp.Err("ERR value is not an integer or out of range")
	errSyntax    = resp.Err("ERR syntax error")
)

func init() {
	commands = map[string]command{
		// connection & server
		"PING":         {-1, false, cmdPing},
		"ECHO":         {2, false, func(_ *DB, a []string) resp.Value { return resp.Bulk(a[0]) }},
		"SELECT":       {2, false, cmdSelect},
		"COMMAND":      {-1, false, func(*DB, []string) resp.Value { return resp.Arr() }},
		"CONFIG":       {-2, false, cmdConfig},
		"CLIENT":       {-2, false, cmdClient},
		"INFO":         {-1, false, cmdInfo},
		"DBSIZE":       {1, false, cmdDBSize},
		"FLUSHALL":     {-1, true, cmdFlushAll},
		"FLUSHDB":      {-1, true, cmdFlushAll},
		"BGREWRITEAOF": {1, false, cmdRewriteAOF},
		// keyspace
		"DEL":       {-2, true, cmdDel},
		"UNLINK":    {-2, true, cmdDel},
		"EXISTS":    {-2, false, cmdExists},
		"TYPE":      {2, false, cmdType},
		"KEYS":      {2, false, cmdKeys},
		"RENAME":    {3, true, cmdRename},
		"EXPIRE":    {3, true, expireCmd(time.Second, false)},
		"PEXPIRE":   {3, true, expireCmd(time.Millisecond, false)},
		"EXPIREAT":  {3, true, expireCmd(time.Second, true)},
		"PEXPIREAT": {3, true, expireCmd(time.Millisecond, true)},
		"TTL":       {2, false, ttlCmd(time.Second)},
		"PTTL":      {2, false, ttlCmd(time.Millisecond)},
		"PERSIST":   {2, true, cmdPersist},
		// strings
		"GET":    {2, false, cmdGet},
		"SET":    {-3, true, cmdSet},
		"SETEX":  {4, true, setexCmd("setex", time.Second)},
		"PSETEX": {4, true, setexCmd("psetex", time.Millisecond)},
		"GETDEL": {2, true, cmdGetDel},
		"MGET":   {-2, false, cmdMGet},
		"MSET":   {-3, true, cmdMSet},
		"APPEND": {3, true, cmdAppend},
		"STRLEN": {2, false, cmdStrlen},
		"INCR":   {2, true, incrCmd(1, false)},
		"DECR":   {2, true, incrCmd(-1, false)},
		"INCRBY": {3, true, incrCmd(1, true)},
		"DECRBY": {3, true, incrCmd(-1, true)},
		// lists
		"LPUSH":  {-3, true, pushCmd(true)},
		"RPUSH":  {-3, true, pushCmd(false)},
		"LPOP":   {-2, true, popCmd(true)},
		"RPOP":   {-2, true, popCmd(false)},
		"LLEN":   {2, false, cmdLLen},
		"LRANGE": {4, false, cmdLRange},
		"LINDEX": {3, false, cmdLIndex},
	}
}

// ---------------------------------------------------------------- server

func cmdPing(_ *DB, args []string) resp.Value {
	switch len(args) {
	case 0:
		return resp.Simple("PONG")
	case 1:
		return resp.Bulk(args[0])
	}
	return resp.Err("ERR wrong number of arguments for 'ping' command")
}

// cmdClient accepts the CLIENT subcommands clients send during setup.
func cmdClient(_ *DB, args []string) resp.Value {
	switch strings.ToUpper(args[0]) {
	case "SETNAME", "SETINFO":
		return resp.OK()
	case "GETNAME":
		return resp.NullBulk()
	case "ID":
		return resp.Int(1)
	}
	return resp.Err("ERR unknown subcommand '%s'", args[0])
}

// cmdConfig supports CONFIG GET for the handful of settings tools ask about.
func cmdConfig(db *DB, args []string) resp.Value {
	if strings.ToUpper(args[0]) != "GET" || len(args) != 2 {
		return resp.Err("ERR kvred only supports CONFIG GET <pattern>")
	}
	appendonly := map[bool]string{true: "yes", false: "no"}[db.aof != nil]
	settings := [][2]string{{"appendonly", appendonly}, {"databases", "1"}, {"save", ""}}
	var out []resp.Value
	for _, kv := range settings {
		if globMatch(strings.ToLower(args[1]), kv[0]) {
			out = append(out, resp.Bulk(kv[0]), resp.Bulk(kv[1]))
		}
	}
	return resp.MapOf(out...)
}

func cmdSelect(_ *DB, args []string) resp.Value {
	if args[0] != "0" {
		return resp.Err("ERR kvred has a single database; only SELECT 0 is supported")
	}
	return resp.OK()
}

func cmdInfo(db *DB, _ []string) resp.Value {
	var sb strings.Builder
	fmt.Fprintf(&sb, "# Server\r\nredis_version:7.0.0-kvred\r\nkvred_version:0.1.0\r\n")
	fmt.Fprintf(&sb, "uptime_in_seconds:%d\r\n\r\n", int(db.now().Sub(db.started).Seconds()))
	fmt.Fprintf(&sb, "# Persistence\r\naof_enabled:%d\r\n\r\n", btoi(db.aof != nil))
	fmt.Fprintf(&sb, "# Stats\r\ntotal_commands_processed:%d\r\nexpired_keys:%d\r\n\r\n", db.commands, db.expired)
	fmt.Fprintf(&sb, "# Keyspace\r\ndb0:keys=%d,expires=%d\r\n", len(db.data), len(db.volatile))
	return resp.Bulk(sb.String())
}

func cmdDBSize(db *DB, _ []string) resp.Value { return resp.Int(int64(len(db.data))) }

func cmdFlushAll(db *DB, _ []string) resp.Value {
	db.data = make(map[string]*entry)
	db.volatile = make(map[string]struct{})
	db.dirty++
	return resp.OK()
}

func cmdRewriteAOF(db *DB, _ []string) resp.Value {
	if db.aof == nil {
		return resp.Err("ERR append-only file is not enabled")
	}
	if err := db.aof.Rewrite(db.snapshot()); err != nil {
		return resp.Err("ERR rewrite failed: %v", err)
	}
	return resp.Simple("Background append only file rewriting started")
}

// ---------------------------------------------------------------- keyspace

func cmdDel(db *DB, args []string) resp.Value {
	n := 0
	for _, k := range args {
		if db.lookup(k) != nil {
			db.remove(k)
			n++
		}
	}
	db.dirty += int64(n)
	return resp.Int(int64(n))
}

func cmdExists(db *DB, args []string) resp.Value {
	n := 0
	for _, k := range args {
		if db.lookup(k) != nil {
			n++
		}
	}
	return resp.Int(int64(n))
}

func cmdType(db *DB, args []string) resp.Value {
	e := db.lookup(args[0])
	switch {
	case e == nil:
		return resp.Simple("none")
	case e.kind == kindList:
		return resp.Simple("list")
	}
	return resp.Simple("string")
}

func cmdKeys(db *DB, args []string) resp.Value {
	var keys []string
	for k := range db.data {
		if globMatch(args[0], k) && db.lookup(k) != nil {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return resp.BulkArray(keys)
}

func cmdRename(db *DB, args []string) resp.Value {
	e := db.lookup(args[0])
	if e == nil {
		return resp.Err("ERR no such key")
	}
	db.remove(args[0])
	db.data[args[1]] = e
	delete(db.volatile, args[1])
	db.setExpire(args[1], e, e.expireAt)
	db.dirty++
	return resp.OK()
}

func expireCmd(unit time.Duration, absolute bool) func(*DB, []string) resp.Value {
	return func(db *DB, args []string) resp.Value {
		n, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return errNotInt
		}
		e := db.lookup(args[0])
		if e == nil {
			return resp.Int(0)
		}
		at := n * unit.Milliseconds()
		if !absolute {
			at += db.nowMs()
		}
		if at <= db.nowMs() {
			db.remove(args[0]) // a TTL in the past deletes the key immediately
		} else {
			db.setExpire(args[0], e, at)
		}
		db.dirty++
		return resp.Int(1)
	}
}

func ttlCmd(unit time.Duration) func(*DB, []string) resp.Value {
	return func(db *DB, args []string) resp.Value {
		e := db.lookup(args[0])
		if e == nil {
			return resp.Int(-2)
		}
		if e.expireAt == 0 {
			return resp.Int(-1)
		}
		ms := e.expireAt - db.nowMs()
		if unit == time.Second {
			return resp.Int((ms + 500) / 1000) // Redis rounds to the nearest second
		}
		return resp.Int(ms)
	}
}

func cmdPersist(db *DB, args []string) resp.Value {
	e := db.lookup(args[0])
	if e == nil || e.expireAt == 0 {
		return resp.Int(0)
	}
	db.setExpire(args[0], e, 0)
	db.dirty++
	return resp.Int(1)
}

// ---------------------------------------------------------------- strings

func (db *DB) getString(key string) (*entry, *resp.Value) {
	e := db.lookup(key)
	if e != nil && e.kind != kindString {
		return nil, &errWrongType
	}
	return e, nil
}

func cmdGet(db *DB, args []string) resp.Value {
	e, errv := db.getString(args[0])
	if errv != nil {
		return *errv
	}
	if e == nil {
		return resp.NullBulk()
	}
	return resp.Bulk(e.str)
}

// cmdSet supports SET key value [NX|XX] [GET] [EX s|PX ms|KEEPTTL].
func cmdSet(db *DB, args []string) resp.Value {
	key, value := args[0], args[1]
	var nx, xx, get, keepTTL bool
	var expireAt int64
	for i := 2; i < len(args); i++ {
		switch opt := strings.ToUpper(args[i]); opt {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GET":
			get = true
		case "KEEPTTL":
			keepTTL = true
		case "EX", "PX":
			if i+1 >= len(args) || expireAt != 0 {
				return errSyntax
			}
			n, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil || n <= 0 {
				return resp.Err("ERR invalid expire time in 'set' command")
			}
			if opt == "EX" {
				n *= 1000
			}
			expireAt = db.nowMs() + n
			i++
		default:
			return errSyntax
		}
	}
	if (nx && xx) || (keepTTL && expireAt != 0) {
		return errSyntax
	}

	old := db.lookup(key)
	var previous resp.Value
	if get {
		switch {
		case old == nil:
			previous = resp.NullBulk()
		case old.kind != kindString:
			return errWrongType
		default:
			previous = resp.Bulk(old.str)
		}
	}
	if (nx && old != nil) || (xx && old == nil) {
		if get {
			return previous
		}
		return resp.NullBulk()
	}
	e := &entry{kind: kindString, str: value}
	if keepTTL && old != nil {
		expireAt = old.expireAt
	}
	db.data[key] = e
	db.setExpire(key, e, expireAt)
	db.dirty++
	if get {
		return previous
	}
	return resp.OK()
}

func setexCmd(name string, unit time.Duration) func(*DB, []string) resp.Value {
	return func(db *DB, args []string) resp.Value {
		n, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || n <= 0 {
			return resp.Err("ERR invalid expire time in '%s' command", name)
		}
		e := &entry{kind: kindString, str: args[2]}
		db.data[args[0]] = e
		db.setExpire(args[0], e, db.nowMs()+n*unit.Milliseconds())
		db.dirty++
		return resp.OK()
	}
}

func cmdGetDel(db *DB, args []string) resp.Value {
	reply := cmdGet(db, args)
	if reply.Kind == resp.BulkString && !reply.Null {
		db.remove(args[0])
		db.dirty++
	}
	return reply
}

func cmdMGet(db *DB, args []string) resp.Value {
	out := make([]resp.Value, len(args))
	for i, k := range args {
		if e := db.lookup(k); e != nil && e.kind == kindString {
			out[i] = resp.Bulk(e.str)
		} else {
			out[i] = resp.NullBulk() // MGET returns nil for wrong types rather than failing
		}
	}
	return resp.Arr(out...)
}

func cmdMSet(db *DB, args []string) resp.Value {
	if len(args)%2 != 0 {
		return resp.Err("ERR wrong number of arguments for 'mset' command")
	}
	for i := 0; i < len(args); i += 2 {
		db.remove(args[i])
		db.data[args[i]] = &entry{kind: kindString, str: args[i+1]}
	}
	db.dirty++
	return resp.OK()
}

func cmdAppend(db *DB, args []string) resp.Value {
	e, errv := db.getString(args[0])
	if errv != nil {
		return *errv
	}
	if e == nil {
		e = &entry{kind: kindString}
		db.data[args[0]] = e
	}
	e.str += args[1]
	db.dirty++
	return resp.Int(int64(len(e.str)))
}

func cmdStrlen(db *DB, args []string) resp.Value {
	e, errv := db.getString(args[0])
	if errv != nil {
		return *errv
	}
	if e == nil {
		return resp.Int(0)
	}
	return resp.Int(int64(len(e.str)))
}

func incrCmd(sign int64, hasArg bool) func(*DB, []string) resp.Value {
	return func(db *DB, args []string) resp.Value {
		delta := sign
		if hasArg {
			n, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil || (sign < 0 && n == math.MinInt64) {
				return errNotInt
			}
			delta = sign * n
		}
		e, errv := db.getString(args[0])
		if errv != nil {
			return *errv
		}
		var current int64
		if e != nil {
			n, err := strconv.ParseInt(e.str, 10, 64)
			if err != nil {
				return errNotInt
			}
			current = n
		}
		if (delta > 0 && current > math.MaxInt64-delta) || (delta < 0 && current < math.MinInt64-delta) {
			return resp.Err("ERR increment or decrement would overflow")
		}
		current += delta
		if e == nil {
			e = &entry{kind: kindString}
			db.data[args[0]] = e
		}
		e.str = strconv.FormatInt(current, 10)
		db.dirty++
		return resp.Int(current)
	}
}

// ---------------------------------------------------------------- lists

func (db *DB) getList(key string) (*entry, *resp.Value) {
	e := db.lookup(key)
	if e != nil && e.kind != kindList {
		return nil, &errWrongType
	}
	return e, nil
}

func pushCmd(left bool) func(*DB, []string) resp.Value {
	return func(db *DB, args []string) resp.Value {
		e, errv := db.getList(args[0])
		if errv != nil {
			return *errv
		}
		if e == nil {
			e = &entry{kind: kindList}
			db.data[args[0]] = e
		}
		for _, v := range args[1:] { // LPUSH a b c leaves the list as c b a
			if left {
				e.list.PushFront(v)
			} else {
				e.list.PushBack(v)
			}
		}
		db.dirty++
		return resp.Int(int64(e.list.Len()))
	}
}

func popCmd(left bool) func(*DB, []string) resp.Value {
	return func(db *DB, args []string) resp.Value {
		if len(args) > 2 {
			return errSyntax
		}
		count, withCount := 1, len(args) == 2
		if withCount {
			n, err := strconv.Atoi(args[1])
			if err != nil || n < 0 {
				return resp.Err("ERR value is out of range, must be positive")
			}
			count = n
		}
		e, errv := db.getList(args[0])
		if errv != nil {
			return *errv
		}
		if e == nil {
			if withCount {
				return resp.NullArray()
			}
			return resp.NullBulk()
		}
		count = min(count, e.list.Len())
		popped := make([]string, count)
		for i := range popped { // RPOP with a count returns elements tail-first
			if left {
				popped[i] = e.list.PopFront()
			} else {
				popped[i] = e.list.PopBack()
			}
		}
		if e.list.Len() == 0 {
			db.remove(args[0]) // Redis never keeps empty lists around
		}
		db.dirty++
		if withCount {
			return resp.BulkArray(popped)
		}
		return resp.Bulk(popped[0])
	}
}

func cmdLLen(db *DB, args []string) resp.Value {
	e, errv := db.getList(args[0])
	if errv != nil {
		return *errv
	}
	if e == nil {
		return resp.Int(0)
	}
	return resp.Int(int64(e.list.Len()))
}

func cmdLRange(db *DB, args []string) resp.Value {
	start, err1 := strconv.Atoi(args[1])
	stop, err2 := strconv.Atoi(args[2])
	if err1 != nil || err2 != nil {
		return errNotInt
	}
	e, errv := db.getList(args[0])
	if errv != nil {
		return *errv
	}
	if e == nil {
		return resp.Arr()
	}
	n := e.list.Len()
	if start < 0 {
		start = max(n+start, 0)
	}
	if stop < 0 {
		stop += n
	}
	stop = min(stop, n-1)
	if start > stop {
		return resp.Arr()
	}
	return resp.BulkArray(e.list.Range(start, stop))
}

func cmdLIndex(db *DB, args []string) resp.Value {
	i, err := strconv.Atoi(args[1])
	if err != nil {
		return errNotInt
	}
	e, errv := db.getList(args[0])
	if errv != nil {
		return *errv
	}
	if e == nil {
		return resp.NullBulk()
	}
	if i < 0 {
		i += e.list.Len()
	}
	if i < 0 || i >= e.list.Len() {
		return resp.NullBulk()
	}
	return resp.Bulk(e.list.At(i))
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
