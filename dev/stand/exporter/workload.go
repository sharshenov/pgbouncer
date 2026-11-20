// Copyright 2026 PgBouncer workload observation contributors
// SPDX-License-Identifier: ISC
//
// Compiled into the pinned prometheus-community/pgbouncer_exporter source.
// Reuses its descriptors, row mapping and conversions; adds a strict boundary
// only for the fork's new, non-null, non-negative observation contract.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

var includeWorkloadMetrics = kingpin.Flag(
	"pgBouncer.include-workloads",
	"Collect the fork's workload observation SQL interface; incompatible schemas fail collection.",
).Default("false").Bool()

var workloadColumns = map[string]map[string]ColumnMapping{
	"workload_stats": {
		"database":              {LABEL, "", 1, ""},
		"attribution":           {LABEL, "", 1, ""},
		"team":                  {LABEL, "", 1, ""},
		"caller":                {LABEL, "", 1, ""},
		"forwarded":             {COUNTER, "forwarded_total", 1, "Forwarded logical Query, Execute and fast-path exchanges."},
		"completed":             {COUNTER, "completed_total", 1, "Completed logical exchanges, including failed exchanges."},
		"failed":                {COUNTER, "failed_total", 1, "Failed completed exchanges, including matching preparation failures."},
		"skipped":               {COUNTER, "skipped_total", 1, "Forwarded exchanges skipped during protocol recovery."},
		"aborted":               {COUNTER, "aborted_total", 1, "Exchanges aborted by connection loss or shutdown."},
		"inflight":              {GAUGE, "inflight", 1, "Forwarded exchanges not yet reconciled, including pipeline wait."},
		"running":               {GAUGE, "running", 1, "Proxy response-order estimate of active execution exchanges."},
		"execution_us":          {COUNTER, "execution_seconds_total", 1e-6, "Observed active exchange wall time, not CPU or IO utilization."},
		"pipeline_wait_us":      {COUNTER, "pipeline_wait_seconds_total", 1e-6, "Time waiting behind earlier forwarded protocol work."},
		"pool_wait_us":          {COUNTER, "pool_wait_seconds_total", 1e-6, "Completed waits for a backend connection."},
		"transactions":          {COUNTER, "transactions_total", 1, "Reconciled backend transaction occupancy episodes."},
		"aborted_transactions":  {COUNTER, "aborted_transactions_total", 1, "Transaction occupancy episodes ended by connection loss."},
		"transactions_inflight": {GAUGE, "transactions_inflight", 1, "Backend transaction occupancy episodes currently open."},
		"transaction_us":        {COUNTER, "transaction_seconds_total", 1e-6, "Backend occupancy wall time pinned to the first observed owner."},
		"oldest_running_us":     {GAUGE, "oldest_running_seconds", 1e-6, "Age of the oldest active observed exchange."},
		"live_execution_us":     {GAUGE, "live_execution_seconds", 1e-6, "Current active elapsed time, excluding the cumulative completed counter."},
	},
	"workload_status": {
		"enabled":                   {GAUGE, "enabled", 1, "Whether the proxy observes newly forwarded operations."},
		"detail_limit_per_database": {GAUGE, "detail_limit_per_database", 1, "Configured detailed identity limit per database."},
		"allocation_failures":       {COUNTER, "allocation_failures_total", 1, "Observation allocation failures: a coverage fault."},
		"database_resets":           {COUNTER, "database_resets_total", 1, "Database observation histories reset by database removal."},
	},
}

type workloadCollector struct {
	db          *sql.DB
	mappings    map[string]MetricMapNamespace
	logger      log.Logger
	timeout     time.Duration
	lastSuccess atomic.Int64
	up          *prometheus.Desc
	last        *prometheus.Desc
}

func newWorkloadCollector(db *sql.DB, logger log.Logger) *workloadCollector {
	mappings := makeDescMap(workloadColumns, namespace, logger)
	return &workloadCollector{
		db: db, mappings: mappings, logger: logger, timeout: workloadQueryTimeout,
		up: prometheus.NewDesc("pgbouncer_workload_collection_success",
			"Both workload SQL tables were collected with compatible schemas and valid counters.", nil, nil),
		last: prometheus.NewDesc("pgbouncer_workload_last_success_timestamp_seconds",
			"Unix timestamp of the last successful workload collection; zero before the first success.", nil, nil),
	}
}

func (c *workloadCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.up
	ch <- c.last
	for _, mapping := range c.mappings {
		for _, metric := range mapping.columnMappings {
			if metric.desc != nil {
				ch <- metric.desc
			}
		}
	}
}

func (c *workloadCollector) collectTable(ctx context.Context, ch chan<- prometheus.Metric, table string) error {
	// table comes only from Collect's fixed list, never a label or user input.
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf("SHOW %s;", table))
	if err != nil {
		return err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	seen := make(map[string]bool, len(columns))
	for _, name := range columns {
		if seen[name] {
			return fmt.Errorf("duplicate workload column %q", name)
		}
		seen[name] = true
	}
	for name := range workloadColumns[table] {
		if !seen[name] {
			return fmt.Errorf("required workload column %q is missing", name)
		}
	}
	mapping := c.mappings[table]
	if table == "workload_stats" && len(mapping.labels) != 4 {
		return fmt.Errorf("workload identity contract must have four dimensions")
	}
	labels := make([]string, len(mapping.labels))
	identities := map[[4]string]bool{}
	labelIndexes := make(map[string]int, len(mapping.labels))
	for i, name := range mapping.labels {
		labelIndexes[name] = i
	}
	values := make([]sql.RawBytes, len(columns))
	scan := make([]interface{}, len(columns))
	for i, name := range columns {
		if index, ok := labelIndexes[name]; ok {
			scan[i] = &labels[index]
		} else {
			scan[i] = &values[i]
		}
	}
	count := 0
	for rows.Next() {
		if err := rows.Scan(scan...); err != nil {
			return err
		}
		count++
		if table == "workload_status" && count > 1 {
			return fmt.Errorf("workload status returned more than one row")
		}
		for _, label := range labels {
			if !utf8.ValidString(label) {
				return fmt.Errorf("invalid UTF-8 workload label")
			}
		}
		if table == "workload_stats" {
			var key [4]string
			copy(key[:], labels)
			if identities[key] {
				return fmt.Errorf("duplicate workload identity")
			}
			identities[key] = true
		}
		for i, name := range columns {
			definition, known := workloadColumns[table][name]
			if !known || definition.usage == LABEL {
				continue
			}
			if _, err := strconv.ParseUint(string(values[i]), 10, 64); err != nil {
				return fmt.Errorf("invalid unsigned workload counter %q", name)
			}
			if table == "workload_status" && name == "enabled" && string(values[i]) != "0" && string(values[i]) != "1" {
				return fmt.Errorf("workload enabled flag is not boolean")
			}
			metric := mapping.columnMappings[name]
			value, ok := metric.conversion([]byte(values[i]))
			if !ok || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
				return fmt.Errorf("invalid converted workload counter %q", name)
			}
			ch <- prometheus.MustNewConstMetric(metric.desc, metric.vtype, value, labels...)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if table == "workload_status" && count != 1 {
		return fmt.Errorf("workload status returned %d rows, expected one", count)
	}
	return nil
}

func (c *workloadCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	success := true
	for _, table := range []string{"workload_stats", "workload_status"} {
		if err := c.collectTable(ctx, ch, table); err != nil {
			level.Error(c.logger).Log("msg", "workload collection failed", "table", table, "err", err)
			success = false
		}
	}
	value := 0.0
	if success {
		value = 1
		c.lastSuccess.Store(time.Now().Unix())
	}
	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, value)
	ch <- prometheus.MustNewConstMetric(c.last, prometheus.GaugeValue, float64(c.lastSuccess.Load()))
}
