// Copyright 2026 PgBouncer workload observation contributors
// SPDX-License-Identifier: ISC
package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type testTable struct {
	columns []string
	rows    [][]driver.Value
	err     error
	block   bool
}

type testDatabase struct {
	tables map[string]testTable
	mu     sync.Mutex
	calls  []string
}

var testDatabases sync.Map

type workloadTestDriver struct{}
type workloadTestConn struct{ db *testDatabase }
type workloadTestRows struct {
	table testTable
	index int
}

func init() { sql.Register("workload-observation-test", workloadTestDriver{}) }
func (workloadTestDriver) Open(name string) (driver.Conn, error) {
	db, ok := testDatabases.Load(name)
	if !ok {
		return nil, errors.New("unknown test database")
	}
	return &workloadTestConn{db: db.(*testDatabase)}, nil
}
func (*workloadTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("test driver does not prepare statements")
}
func (*workloadTestConn) Close() error { return nil }
func (*workloadTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("test driver does not start transactions")
}
func (c *workloadTestConn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.db.mu.Lock()
	c.db.calls = append(c.db.calls, query)
	table, ok := c.db.tables[query]
	c.db.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
	if table.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if table.err != nil {
		return nil, table.err
	}
	return &workloadTestRows{table: table}, nil
}
func (r *workloadTestRows) Columns() []string { return r.table.columns }
func (*workloadTestRows) Close() error        { return nil }
func (r *workloadTestRows) Next(values []driver.Value) error {
	if r.index == len(r.table.rows) {
		return io.EOF
	}
	copy(values, r.table.rows[r.index])
	r.index++
	return nil
}

func tableFixture(table string, values map[string]driver.Value) testTable {
	columns := make([]string, 0, len(workloadColumns[table]))
	for name := range workloadColumns[table] {
		columns = append(columns, name)
	}
	sort.Strings(columns) // Exercise name-based mapping, not one lucky order.
	row := make([]driver.Value, len(columns))
	for i, name := range columns {
		if value, ok := values[name]; ok {
			row[i] = value
		} else if workloadColumns[table][name].usage == LABEL {
			row[i] = ""
		} else {
			row[i] = []byte("0")
		}
	}
	return testTable{columns: columns, rows: [][]driver.Value{row}}
}

func newTestCollector(t *testing.T, tables map[string]testTable) (*workloadCollector, *testDatabase) {
	t.Helper()
	state := &testDatabase{tables: tables}
	testDatabases.Store(t.Name(), state)
	db, err := sql.Open("workload-observation-test", t.Name())
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		db.Close()
		testDatabases.Delete(t.Name())
	})
	return newWorkloadCollector(db, log.NewNopLogger()), state
}

func fixtureTables() map[string]testTable {
	return map[string]testTable{
		"SHOW workload_stats;": tableFixture("workload_stats", map[string]driver.Value{
			"database": "primary", "attribution": "tagged", "team": "synthetic", "caller": "Jobs::Sync",
			"completed": []byte("2"), "execution_us": []byte("2500000"),
			"live_execution_us": []byte("1000000"), "running": []byte("1"),
		}),
		"SHOW workload_status;": tableFixture("workload_status", map[string]driver.Value{"enabled": []byte("1")}),
	}
}

func gather(t *testing.T, collector *workloadCollector) map[string]*dto.MetricFamily {
	t.Helper()
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]*dto.MetricFamily)
	for _, family := range families {
		result[family.GetName()] = family
	}
	return result
}

func gauge(t *testing.T, families map[string]*dto.MetricFamily, name string) float64 {
	t.Helper()
	family := families[name]
	if family == nil || family.GetType() != dto.MetricType_GAUGE || len(family.Metric) != 1 {
		t.Fatalf("missing or invalid gauge: %s", name)
	}
	return family.Metric[0].GetGauge().GetValue()
}

func TestWorkloadUnitsTypesAndBoundedLabels(t *testing.T) {
	collector, state := newTestCollector(t, fixtureTables())
	families := gather(t, collector)
	execution := families["pgbouncer_workload_stats_execution_seconds_total"]
	if execution.GetType() != dto.MetricType_COUNTER || execution.Metric[0].GetCounter().GetValue() != 2.5 {
		t.Fatal("execution time must be a seconds counter")
	}
	if families["pgbouncer_workload_stats_completed_total"].Metric[0].GetCounter().GetValue() != 2 {
		t.Fatal("operation counts must not use a time conversion factor")
	}
	if gauge(t, families, "pgbouncer_workload_stats_live_execution_seconds") != 1 {
		t.Fatal("live elapsed time must be a gauge in seconds")
	}
	labels := map[string]string{}
	for _, label := range execution.Metric[0].Label {
		labels[label.GetName()] = label.GetValue()
	}
	if len(labels) != 4 || labels["caller"] != "Jobs::Sync" || labels["database"] != "primary" {
		t.Fatalf("unexpected metric dimensions: %v", labels)
	}
	if gauge(t, families, "pgbouncer_workload_collection_success") != 1 ||
		gauge(t, families, "pgbouncer_workload_last_success_timestamp_seconds") == 0 {
		t.Fatal("successful collection must be visible")
	}
	if len(state.calls) != 2 {
		t.Fatalf("expected two read-only queries, got %v", state.calls)
	}
}

func TestWorkloadMissingSchemaFailsExplicitly(t *testing.T) {
	tables := fixtureTables()
	table := tables["SHOW workload_stats;"]
	for i, name := range table.columns {
		if name == "pool_wait_us" {
			table.columns = append(table.columns[:i], table.columns[i+1:]...)
			table.rows = nil
			break
		}
	}
	tables["SHOW workload_stats;"] = table
	collector, _ := newTestCollector(t, tables)
	families := gather(t, collector)
	if gauge(t, families, "pgbouncer_workload_collection_success") != 0 ||
		gauge(t, families, "pgbouncer_workload_last_success_timestamp_seconds") != 0 {
		t.Fatal("an incompatible schema must not appear healthy")
	}
}

func TestWorkloadDuplicateColumnsFailExplicitly(t *testing.T) {
	tables := fixtureTables()
	table := tables["SHOW workload_stats;"]
	table.columns = append(table.columns, "team")
	table.rows[0] = append(table.rows[0], "duplicate")
	tables["SHOW workload_stats;"] = table
	collector, _ := newTestCollector(t, tables)
	if gauge(t, gather(t, collector), "pgbouncer_workload_collection_success") != 0 {
		t.Fatal("ambiguous columns must fail collection")
	}
}

func TestWorkloadDuplicateIdentityFailsWithoutDuplicateMetrics(t *testing.T) {
	tables := fixtureTables()
	table := tables["SHOW workload_stats;"]
	table.rows = append(table.rows, table.rows[0])
	tables["SHOW workload_stats;"] = table
	collector, _ := newTestCollector(t, tables)
	if gauge(t, gather(t, collector), "pgbouncer_workload_collection_success") != 0 {
		t.Fatal("duplicate identities must fail workload collection without breaking stock metric gathering")
	}
}

func TestWorkloadUnknownColumnsNeverBecomeLabels(t *testing.T) {
	tables := fixtureTables()
	table := tables["SHOW workload_stats;"]
	table.columns = append(table.columns, "trace_id")
	table.rows[0] = append(table.rows[0], "synthetic-high-cardinality")
	tables["SHOW workload_stats;"] = table
	collector, _ := newTestCollector(t, tables)
	families := gather(t, collector)
	metric := families["pgbouncer_workload_stats_completed_total"].Metric[0]
	if len(metric.Label) != 4 || gauge(t, families, "pgbouncer_workload_collection_success") != 1 {
		t.Fatal("unknown columns must not change the metric dimensions")
	}
}

func TestWorkloadInvalidLabelsFailExplicitly(t *testing.T) {
	for _, value := range []driver.Value{nil, string([]byte{0xff})} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			tables := fixtureTables()
			tables["SHOW workload_stats;"] = tableFixture("workload_stats", map[string]driver.Value{"team": value})
			collector, _ := newTestCollector(t, tables)
			if gauge(t, gather(t, collector), "pgbouncer_workload_collection_success") != 0 {
				t.Fatal("invalid labels must fail collection")
			}
		})
	}
}

func TestWorkloadStatusRequiresExactlyOneRow(t *testing.T) {
	for _, count := range []int{0, 2} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			tables := fixtureTables()
			table := tables["SHOW workload_status;"]
			if count == 0 {
				table.rows = nil
			} else {
				table.rows = append(table.rows, table.rows[0])
			}
			tables["SHOW workload_status;"] = table
			collector, _ := newTestCollector(t, tables)
			if gauge(t, gather(t, collector), "pgbouncer_workload_collection_success") != 0 {
				t.Fatal("ambiguous status must fail collection")
			}
		})
	}
}

func TestWorkloadInvalidCountersFailExplicitly(t *testing.T) {
	for _, value := range []driver.Value{nil, []byte("NaN"), []byte("+Inf"), []byte("-1"), []byte("0.5"), []byte("invalid"), []byte("18446744073709551616")} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			tables := fixtureTables()
			tables["SHOW workload_stats;"] = tableFixture("workload_stats", map[string]driver.Value{"completed": value})
			collector, _ := newTestCollector(t, tables)
			if gauge(t, gather(t, collector), "pgbouncer_workload_collection_success") != 0 {
				t.Fatal("invalid counters must fail collection")
			}
		})
	}
}

func TestWorkloadEnabledFlagIsBoolean(t *testing.T) {
	tables := fixtureTables()
	tables["SHOW workload_status;"] = tableFixture("workload_status", map[string]driver.Value{"enabled": []byte("2")})
	collector, _ := newTestCollector(t, tables)
	if gauge(t, gather(t, collector), "pgbouncer_workload_collection_success") != 0 {
		t.Fatal("unknown enablement state must not appear healthy")
	}
}

func TestWorkloadEmptyStatsAndDisabledObservationAreDistinctFromFailure(t *testing.T) {
	tables := fixtureTables()
	table := tables["SHOW workload_stats;"]
	table.rows = nil
	tables["SHOW workload_stats;"] = table
	tables["SHOW workload_status;"] = tableFixture("workload_status", map[string]driver.Value{"enabled": []byte("0")})
	collector, _ := newTestCollector(t, tables)
	families := gather(t, collector)
	if gauge(t, families, "pgbouncer_workload_collection_success") != 1 ||
		gauge(t, families, "pgbouncer_workload_status_enabled") != 0 {
		t.Fatal("idle/disabled observation is not a SQL collection error")
	}
}

func TestWorkloadQueryFailureAndTimeoutAreVisible(t *testing.T) {
	for _, mode := range []string{"unsupported", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			tables := fixtureTables()
			table := tables["SHOW workload_stats;"]
			table.err = errors.New("unsupported command")
			if mode == "timeout" {
				table.err = nil
				table.block = true
			}
			tables["SHOW workload_stats;"] = table
			collector, _ := newTestCollector(t, tables)
			collector.timeout = 10 * time.Millisecond
			if gauge(t, gather(t, collector), "pgbouncer_workload_collection_success") != 0 {
				t.Fatal("failed queries must fail collection")
			}
		})
	}
}
