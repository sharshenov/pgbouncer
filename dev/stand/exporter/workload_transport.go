// Copyright 2026 PgBouncer workload observation contributors
// SPDX-License-Identifier: ISC
package main

import (
	"context"
	"database/sql"
	"net"
	"time"

	"github.com/lib/pq"
)

const workloadQueryTimeout = 3 * time.Second

// A dedicated, non-idle pool makes a connection's absolute deadline belong to
// exactly one bounded workload query. A stalled extension cannot occupy the
// stock collector's only connection. Context cancellation alone is insufficient
// because lib/pq issues a separate CancelRequest instead of closing the read.
func newWorkloadDatabase(connectionString string) (*sql.DB, error) {
	connector, err := pq.NewConnector(connectionString)
	if err != nil {
		return nil, err
	}
	connector.Dialer(workloadDialer{})
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	return db, nil
}

type workloadDialer struct{}

func (workloadDialer) Dial(network, address string) (net.Conn, error) {
	return (workloadDialer{}).DialTimeout(network, address, workloadQueryTimeout)
}

func (workloadDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	if timeout <= 0 || timeout > workloadQueryTimeout {
		timeout = workloadQueryTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return (workloadDialer{}).DialContext(ctx, network, address)
}

func (workloadDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	deadline := time.Now().Add(workloadQueryTimeout)
	if parent, ok := ctx.Deadline(); ok && parent.Before(deadline) {
		deadline = parent
	}
	conn, err := (&net.Dialer{Timeout: workloadQueryTimeout}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	bounded := &workloadConn{Conn: conn, deadline: deadline}
	if err := bounded.SetDeadline(deadline); err != nil {
		conn.Close()
		return nil, err
	}
	return bounded, nil
}

type workloadConn struct {
	net.Conn
	deadline time.Time
}

func (c *workloadConn) bounded(t time.Time) time.Time {
	if t.IsZero() || t.After(c.deadline) {
		return c.deadline
	}
	return t
}

// lib/pq clears startup deadlines when connect_timeout is set. TLS may also
// adjust deadlines: none may extend this query session's hard lifetime.
func (c *workloadConn) SetDeadline(t time.Time) error {
	return c.Conn.SetDeadline(c.bounded(t))
}
func (c *workloadConn) SetReadDeadline(t time.Time) error {
	return c.Conn.SetReadDeadline(c.bounded(t))
}
func (c *workloadConn) SetWriteDeadline(t time.Time) error {
	return c.Conn.SetWriteDeadline(c.bounded(t))
}
