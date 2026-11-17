package db

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"kvred/resp"
)

// Fsync policies, same names and trade-offs as Redis's `appendfsync`.
const (
	FsyncAlways   = "always"   // fsync after every write: safest, slowest
	FsyncEverySec = "everysec" // fsync once a second: lose at most ~1s on power loss
	FsyncNo       = "no"       // let the OS decide
)

// AOF is an append-only file: every write command, in RESP form, in order.
// Replaying it from the top rebuilds the dataset.
type AOF struct {
	mu     sync.Mutex
	path   string
	f      *os.File
	w      *bufio.Writer
	policy string
	stop   chan struct{}
	done   chan struct{}
}

func OpenAOF(path, policy string) (*AOF, error) {
	switch policy {
	case FsyncAlways, FsyncEverySec, FsyncNo:
	default:
		return nil, fmt.Errorf("unknown fsync policy %q (want always, everysec or no)", policy)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	a := &AOF{path: path, f: f, w: bufio.NewWriter(f), policy: policy,
		stop: make(chan struct{}), done: make(chan struct{})}
	if policy == FsyncEverySec {
		go a.syncEverySecond()
	} else {
		close(a.done)
	}
	return a, nil
}

func (a *AOF) Append(args []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := a.w.Write(resp.EncodeCommand(args...)); err != nil {
		return err
	}
	if err := a.w.Flush(); err != nil { // hand it to the OS right away
		return err
	}
	if a.policy == FsyncAlways {
		return a.f.Sync()
	}
	return nil
}

func (a *AOF) syncEverySecond() {
	defer close(a.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-a.stop:
			return
		case <-ticker.C:
			a.mu.Lock()
			_ = a.f.Sync()
			a.mu.Unlock()
		}
	}
}

// Rewrite atomically replaces the log with the minimal commands that rebuild
// the current state: 1,000 INCRs on one key become a single SET. It writes a
// temp file, fsyncs it, and renames it over the old log, so a crash at any
// point leaves either the old file or the new one, never a half-written mix.
func (a *AOF) Rewrite(cmds [][]string) error {
	tmpPath := a.path + ".rewrite"
	tmp, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(tmp)
	for _, cmd := range cmds {
		if _, err := w.Write(resp.EncodeCommand(cmd...)); err != nil {
			tmp.Close()
			return err
		}
	}
	if err := errors.Join(w.Flush(), tmp.Sync(), tmp.Close()); err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.w.Flush(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, a.path); err != nil {
		return err
	}
	a.f.Close()
	f, err := os.OpenFile(a.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	a.f, a.w = f, bufio.NewWriter(f)
	return nil
}

func (a *AOF) Close() error {
	close(a.stop)
	<-a.done
	a.mu.Lock()
	defer a.mu.Unlock()
	return errors.Join(a.w.Flush(), a.f.Sync(), a.f.Close())
}

// LoadAOF replays the log at path into db. If the process died mid-write, the
// last command may be cut off; like Redis's aof-load-truncated, we drop that
// partial command, truncate the file to the last complete one, and carry on.
func (db *DB) LoadAOF(path string) (commands int, truncated bool, err error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer f.Close()

	db.replaying = true
	defer func() { db.replaying = false }()
	r := resp.NewReader(f)
	var good int64
	for {
		args, err := r.ReadCommand()
		if err == io.EOF {
			return commands, false, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return commands, true, os.Truncate(path, good)
		}
		if err != nil {
			return commands, false, fmt.Errorf("append-only file corrupt at byte %d: %w", good, err)
		}
		if reply := db.Exec(args); reply.IsError() {
			return commands, false, fmt.Errorf("replaying %v: %s", args, reply.Str)
		}
		good = r.Offset()
		commands++
	}
}

// AttachAOF starts logging write commands to a.
func (db *DB) AttachAOF(a *AOF) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.aof = a
}
