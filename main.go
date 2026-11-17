// Command kvred runs a Redis-compatible key-value server.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kvred/db"
	"kvred/server"
)

func main() {
	bind := flag.String("bind", "127.0.0.1", "address to listen on")
	port := flag.Int("port", 6379, "TCP port")
	aofPath := flag.String("aof", "", "append-only file path (empty disables persistence)")
	fsync := flag.String("appendfsync", db.FsyncEverySec, "fsync policy: always, everysec or no")
	flag.Parse()

	logger := log.New(os.Stderr, "kvred ", log.LstdFlags)
	store := db.New(nil)

	var aof *db.AOF
	if *aofPath != "" {
		start := time.Now()
		n, truncated, err := store.LoadAOF(*aofPath)
		if err != nil {
			logger.Fatalf("loading %s: %v", *aofPath, err)
		}
		if truncated {
			logger.Printf("warning: %s ended mid-command (crash?); dropped the partial tail", *aofPath)
		}
		logger.Printf("replayed %d commands from %s in %v", n, *aofPath, time.Since(start).Round(time.Millisecond))
		if aof, err = db.OpenAOF(*aofPath, *fsync); err != nil {
			logger.Fatal(err)
		}
		store.AttachAOF(aof)
	}

	stop := make(chan struct{})
	go store.RunExpiry(stop)

	addr := net.JoinHostPort(*bind, fmt.Sprint(*port))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal(err)
	}
	srv := server.New(store, logger)
	logger.Printf("ready to accept connections on %s", addr)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		logger.Printf("shutting down")
		srv.Close()
	}()

	if err := srv.Serve(ln); err != nil {
		logger.Fatal(err)
	}
	close(stop)
	if aof != nil {
		if err := aof.Close(); err != nil {
			logger.Printf("closing AOF: %v", err)
		}
	}
	logger.Printf("bye")
}
