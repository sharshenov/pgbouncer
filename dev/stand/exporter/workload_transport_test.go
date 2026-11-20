// Copyright 2026 PgBouncer workload observation contributors
// SPDX-License-Identifier: ISC
package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

func postgresPacket(kind byte, body []byte) []byte {
	result := make([]byte, len(body)+5)
	result[0] = kind
	binary.BigEndian.PutUint32(result[1:5], uint32(len(body)+4))
	copy(result[5:], body)
	return result
}

// Only a local, fabricated startup exchange. It deliberately never answers SQL
// or cancellation, exercising real lib/pq I/O rather than a cooperative driver.
func stalledPostgres(t *testing.T) (string, <-chan struct{}, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var connections []net.Conn
	var workers sync.WaitGroup
	done := make(chan struct{})
	queried := make(chan struct{}, 1)
	var stopOnce sync.Once
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			connections = append(connections, conn)
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer conn.Close()
				header := make([]byte, 4)
				if _, err := io.ReadFull(conn, header); err != nil {
					return
				}
				length := binary.BigEndian.Uint32(header)
				if length < 8 || length > 65536 {
					return
				}
				body := make([]byte, length-4)
				if _, err := io.ReadFull(conn, body); err != nil {
					return
				}
				if binary.BigEndian.Uint32(body[:4]) == 196608 {
					startup := append(postgresPacket('R', []byte{0, 0, 0, 0}),
						postgresPacket('S', []byte("server_version\x0016.0\x00"))...)
					startup = append(startup, postgresPacket('S', []byte("client_encoding\x00UTF8\x00"))...)
					startup = append(startup, postgresPacket('Z', []byte{'I'})...)
					if _, err := conn.Write(startup); err != nil {
						return
					}
					queryHeader := make([]byte, 5)
					if _, err := io.ReadFull(conn, queryHeader); err != nil {
						return
					}
					queryLength := binary.BigEndian.Uint32(queryHeader[1:])
					if queryHeader[0] != 'Q' || queryLength < 4 || queryLength > 65536 {
						return
					}
					if _, err := io.CopyN(io.Discard, conn, int64(queryLength-4)); err != nil {
						return
					}
					select {
					case queried <- struct{}{}:
					default:
					}
				}
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()
	stop := func() {
		stopOnce.Do(func() {
			listener.Close()
			<-done
			mu.Lock()
			for _, conn := range connections {
				conn.Close()
			}
			mu.Unlock()
			workers.Wait()
		})
	}
	t.Cleanup(stop)
	return listener.Addr().String(), queried, stop
}

func TestWorkloadStalledSocketHasAHardDeadline(t *testing.T) {
	address, queried, stop := stalledPostgres(t)
	defer stop()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	// connect_timeout exercises lib/pq's startup deadline reset. The wrapper
	// must not allow that reset to remove the query's hard lifetime.
	db, err := newWorkloadDatabase(fmt.Sprintf(
		"host=%s port=%s user=synthetic dbname=pgbouncer sslmode=disable connect_timeout=2", host, port))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collector := newWorkloadCollector(db, log.NewNopLogger())
	collector.timeout = 200 * time.Millisecond
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)
	result := make(chan error, 1)
	started := time.Now()
	go func() {
		families, err := registry.Gather()
		if err == nil {
			ok := false
			for _, family := range families {
				if family.GetName() == "pgbouncer_workload_collection_success" {
					ok = family.Metric[0].Gauge.GetValue() == 0
				}
			}
			if !ok {
				err = fmt.Errorf("stalled socket was not reported as failed")
			}
		}
		result <- err
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("stalled SQL exceeded bounded I/O budget: %s", elapsed)
		}
		select {
		case <-queried:
		default:
			t.Fatal("fixture did not reach the deliberately stalled SQL response")
		}
	case <-time.After(2 * time.Second):
		stop()
		t.Fatal("stalled SQL ignored its context/transport deadline")
	}
}
