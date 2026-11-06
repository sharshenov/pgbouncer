# Query tag traffic control

This document describes the query tag monitoring and traffic control
feature (`src/qtags.c`, `include/qtags.h`). It covers the motivation,
the data model, the queueing mechanics and the failure cases that shaped
the implementation.

## Problem

Google Cloud SQL does not allow custom PostgreSQL extensions, so a
tenant cannot be throttled inside the database with something like
`pg_stat_activity` plus a resource governor. PgBouncer sits between the
application and the database anyway, which makes it the natural place to
answer two questions:

1. Which tenant (team, workload) is responsible for this query?
2. How much database wall time does that tenant consume, and what
   should happen when it consumes too much?

The proxy for "database load" is **server SQL wall time**: the time a
query spends in the database as observed by PgBouncer (query completion
time minus start time). This includes lock waits, I/O and CPU on the
server, and it is the quantity the database actually feels.

The design is modeled on PlanetScale Traffic Control (reverse leaky
bucket), adapted to PgBouncer's single-threaded, libevent architecture.

## Tag parsing

Tenants announce themselves by prefixing their SQL with a
SQLCommenter-style comment:

```sql
/* team=payments action=checkout */ SELECT * FROM carts ...
```

Rules implemented by `qtags_parse()`:

- Only a comment of the form `/* ... */` that consists solely of
  `key=value` or `key:value` pairs counts as a tag comment. Prose
  comments (`/* payments report query */`) and comments that appear
  after the statement (`SELECT 1 /* team=x */`) are ignored. `/*+ ... */`
  optimizer hints are skipped.
- The **tenant key** is configured with `query_tag_tenant_key`
  (e.g. `team`). A query without a value for that key is untagged.
- **Secondary keys** are configured with `query_tag_keys` (e.g.
  `action,sidekiq`). Only the first matching key is tracked. Secondary
  keys exist so that monitoring can break a tenant down by workload
  (web, queue consumer, mailer) without letting arbitrary key/value
  pairs explode the stats cardinality.
- Unknown keys (e.g. `trace_id=...`) are ignored: they never create
  stats rows or budgets. High cardinality data must not be able to
  grow the tables.
- Keys are matched case-insensitively. Values may be single-quoted;
  quotes are stripped. Keys and values are length-limited
  (`QTAG_MAX_KEY`, `QTAG_MAX_VALUE`); over-long values are truncated.

Tags are parsed from:

- the `Query` packet (simple protocol),
- the `Parse` packet (unnamed extended-protocol statements),
- the `Bind` packet (named prepared statements: the statement name is
  looked up in the prepared statement cache and its stored query text is
  parsed, so the tags of the original `Parse` travel with every
  execution).

## Data model

Two hash tables, both LRU-bounded by `tag_budget_max_entries`:

### TagStats (monitoring)

Keyed by `(database, tenant, secondary key, secondary value)`. Every
tagged query updates:

- `query_count`, `xact_count`
- `query_time`, `xact_time` (server wall time, like `SHOW STATS`)
- `wait_time` (time the query waited for a server connection)
- `budget_wait_time` (time the query spent queued by a tag budget)
- `db_time` (`query_time - wait_time`, the load proxy)
- `last_seen`

Rows are visible with `SHOW TAG_STATS` on the admin console. Stats are
purely observational: they are never used for admission decisions.

### TagBudget (control)

Keyed by tenant. Configured parameters (all of them optional, `-1`
means "inherit the global default"):

| Parameter      | Meaning                                                     |
|----------------|-------------------------------------------------------------|
| `share`        | percent of one DB-second that the bucket drains per wall second |
| `burst`        | bucket capacity in DB-seconds                               |
| `per_query_ms` | queue queries whose estimated cost exceeds this, 0 = off    |
| `concurrency`  | max in-flight queries of the tenant, 0 = off                |
| `mode`         | `off`, `warn` or `enforce`                                  |

Runtime state:

- `debt`: microseconds of DB time owed. Every finished query adds its
  server wall time (the EMA sample is capped at 1s so a single huge
  query cannot poison the average).
- `ema_cost`: exponential moving average of query cost (alpha 8). This
  is the *estimated* cost of the next query, available before it runs.
- `active`: number of in-flight queries (concurrency slots).
- counters: `queries`, `queued`, `rejected`, `warned`.

The debt is drained **lazily**: on every admission check and every
charge, the bucket computes `elapsed * share / 100` and subtracts it,
clamped at zero. There is no timer task and no background thread; a
completely idle tenant does not cost anything to maintain.

Budgets come from three sources:

1. the `[tag_budgets]` config section (permanent, survives RELOAD,
   config values win),
2. the admin console `TAG_BUDGET SET/DEL/RESET <tenant> ...` (runtime;
   admin-created budgets survive RELOAD until restart unless they are
   also in the config file),
3. implicitly, when a tagged query arrives and the global
   `tag_budget_mode` is `warn` or `enforce`: an empty budget is created
   from the global defaults.

## Admission control

For every batch of queries PgBouncer runs the decision once, at the
start of the batch (a batch is one or more packets up to the next
`ReadyForQuery`: a simple `Query`, or `Parse`/`Bind`/`Execute` of the
extended protocol). Mid-batch packets and re-deliveries (e.g. the
varcache pause on a fresh server connection) skip admission entirely;
they would deadlock against the batch's own slot otherwise.

The decision, per `qtags_admit()`:

1. **Reject** if the estimated cost alone exceeds the bucket capacity
   (`burst`) or `per_query_ms`: the query could never drain. In
   `enforce` the query is answered with a `C53000` `ErrorResponse` and
   never reaches the server; in `warn` it runs with a warning.
2. **Queue** if `debt + estimated cost > burst`: the tenant is over
   budget. The required wait is
   `(debt + cost - burst) * 100 / share` wall microseconds. In
   `warn` the query runs anyway with a `WARNING`; in `enforce` the
   client is parked.
3. **Queue** if `active >= concurrency` and the tenant is at its
   concurrency limit.
4. **Allow** otherwise: count the query, take a concurrency slot.

Charging happens when the query finishes (`qtags_account_query` /
`qtags_account_xact`, called from `server.c` next to the existing stats
accounting): the slot is released, the debt is increased by the actual
server wall time and the EMA is updated. A client that disconnects
mid-query releases its slot without charging.

### Queueing: the client must only feel latency

The explicit product requirement is: **no client-visible error for a
query that merely has to wait**. Over-budget queries are parked inside
PgBouncer and re-run when the bucket has drained:

- `park_client()` removes the client from the event loop
  (`sbuf_pause`) and appends it to a wait list. The current packet
  stays pending in the socket buffer; it is not consumed.
- Waiters carry a deadline: the computed wait, capped by
  `tag_budget_wait_timeout` (0 = unlimited). A deadline can never be
  closer than `QTAG_MIN_WAIT_USEC` (10ms); waits that resolve by an
  event (a slot freeing) are woken immediately by that event, and the
  10ms floor only keeps the timer from spinning.
- One shared libevent timer is armed for the earliest waiter deadline
  (`arm_wait_timer`). When it fires, `qtag_timer_cb` wakes the due
  waiters one by one; the flag `qtag_timer_armed` is cleared at the top
  of the callback because the one-shot event is no longer pending -
  missing this causes re-parked clients to sleep forever.
- A waiter is woken by three things: the timer, the tenant's budget
  being touched (`TAG_BUDGET SET/DEL/RESET`, config reload), or a
  concurrency slot being released for its tenant
  (`qtags_wake_waiters_for_budget`).
- `wake_client()` resumes the socket (`sbuf_continue`); the pending
  packet is re-delivered by the sbuf machinery and the whole admission
  check runs again from scratch. For packets that were fully buffered
  by the large-packet fetch path (`tag_budget_wait_buffered`), the
  packet is re-dispatched directly via `client_handle_complete_packet()`
  instead, mirroring the normal completion path.
- When the deadline passes, the client is disconnected with
  `query waited N s for the '<tenant>' tag budget, aborting`. This is
  the only timeout that produces an error, and it is off by default.

The net effect for a healthy tenant: queries run at full speed, no
queueing, no messages. The bucket only fills when the tenant's DB time
consumption outpaces its share.

### Cost estimation

Admission happens before the query runs, so the decision needs an
estimate. The EMA of past query costs is used for all three checks
(bucket overflow, per-query cap, and therefore also the wait
computation). Consequences worth knowing:

- The first query of a fresh budget is always admitted (EMA is 0).
- A tenant alternating tiny and huge queries is admitted based on the
  average, then charged for the actual cost. The debt catches up on the
  charge; the queueing delay may lag one query behind the abuse.
- The EMA sample and the charge are capped at 1 second per query; a
  single 10-minute query adds 10 minutes of debt but only 1s to the
  EMA.

## Interaction with PgBouncer internals

- `handle_client_work()` inspects the packet *before* it is forwarded.
  The hooks return a three-way result: `CONTINUE` (process normally),
  `CONSUMED` (rejected; the packet is skipped with
  `skip_possibly_completely_buffered_packet()`) and `PARKED` (the
  client was parked; return `false` so the sbuf keeps the packet
  pending). Conflating "consumed" and "parked" causes the packet to be
  forwarded to the server anyway and spins the sbuf state machine.
- `qtags_clear()` (called on every re-parse) preserves
  `active_tenant`, the client's booked slot, because the slot is
  released only when the batch completes. Wiping it leaks concurrency
  slots.
- Accounting hooks live in `server.c` (`qtags_account_query` /
  `qtags_account_xact`, next to `stats.query_time` accounting) and in
  `objects.c` (`activate_client` for pre-link wait time,
  `disconnect_client_sqlstate` for slot cleanup on disconnect).
- Prepared statement lookups use the non-disconnecting
  `get_prepared_statement_query()` helper added to `prepare.c`.

## Admin surface

```
SHOW TAG_STATS        per (db, tenant, secondary) monitoring counters
SHOW TAG_BUDGETS      per tenant budget configuration and runtime state
TAG_BUDGET SET <tenant> [share=N] [burst=S] [per_query_ms=N] ...
TAG_BUDGET DEL <tenant>
TAG_BUDGET RESET <tenant>    zero the debt and counters
```

`TAG_BUDGET SET` merges parameters: only the listed keys change. All
commands wake queued clients so that they re-check immediately.

## Configuration summary

| Setting                   | Default | Meaning                                   |
|---------------------------|---------|-------------------------------------------|
| `query_tag_tenant_key`    | (empty) | tag key that identifies the tenant        |
| `query_tag_keys`          | (empty) | comma separated secondary keys            |
| `tag_budget_mode`         | off     | global default mode (`off`/`warn`/`enforce`) |
| `tag_budget_share`        | 100     | default drain percent                     |
| `tag_budget_burst`        | 0       | default bucket capacity (DB-seconds)      |
| `tag_budget_per_query_ms` | 0       | default per-query cap                     |
| `tag_budget_concurrency`  | 0       | default concurrency cap                   |
| `tag_budget_max_entries`  | 1000    | LRU bound for budgets and stats rows      |
| `tag_budget_wait_timeout` | 0       | max queueing time per query (seconds)     |

With the feature disabled (`query_tag_tenant_key` empty or
`tag_budget_mode=off` and no budgets), the hooks are no-ops: packets
are parsed only when a tenant key is configured, and no state is kept.

## Test notes

`test/test_qtags.py` covers parsing, monitoring, the config section,
the admin commands, queueing latency, concurrency limiting, rejection,
warn mode and the wait timeout. The queueing tests assert on wall
clock, so they use `pg_sleep` durations with generous margins.
