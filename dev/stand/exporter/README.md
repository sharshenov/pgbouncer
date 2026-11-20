# Opt-in exporter extension

This component builds into the existing prometheus-community PgBouncer exporter
at pinned commit `5716a88b8dc8ce887878b2bda832ff46d79f67d2` (v0.9.0).
The added collector is ISC licensed, matching this fork. The pinned upstream
source retains its existing licenses and notices; its files are not relicensed.

`upstream.patch` only registers the opt-in collector. Existing descriptors and
unit conversions are reused, while the new SQL contract has a typed row boundary
(non-null UTF-8 labels and unsigned integral counters). Stock row parsing, metric
tables, connection pooling, and default behavior are unchanged.

The small patch uses zero-context hunks to avoid whitespace-only context lines
under the repository's whitespace checks. It is applied only after checking out
and verifying the exact pinned upstream commit; an unexpected checkout or patch
failure must stop the build, not fall back to a different source.

Enable explicitly with `--pgBouncer.include-workloads`. This requires the fork's
`SHOW WORKLOAD_STATS` and `SHOW WORKLOAD_STATUS` contract. Missing/ambiguous
columns, invalid nonnegative counters, unsupported commands, and timeouts fail
collection visibly instead of silently returning an idle workload.

The collector uses one three-second budget for both read-only queries, including
connection startup. Its separate single-connection pool retains no idle sessions:
each query owns an absolute socket deadline that lib/pq startup or TLS cannot
extend/reset. This avoids a stalled workload query occupying the stock
collector's sole pooled connection. It costs two short admin connections per
successful scrape; measure that overhead before rollout. Stock collection still
has its upstream transport behavior. The extension exposes:

- `pgbouncer_workload_stats_*`: per database/attribution/team/caller observations.
- `pgbouncer_workload_status_*`: feature state, detail limits, allocation faults,
  and database observation resets.
- `pgbouncer_workload_collection_success`: success of the additional SQL contract,
  separate from stock exporter connectivity.
- `pgbouncer_workload_last_success_timestamp_seconds`: freshness, including across
  failed collections.

Execution, transaction, pipeline-wait, and pool-wait totals are counters in
seconds. In-flight/running counts, oldest-running age, and live execution time
are gauges. **Do not apply counter-rate functions to live elapsed-time gauges.**
Sum all attribution categories for totals; detailed rows and fallbacks are
disjoint. Proxy wall time is not CPU/IO utilization.

The stand dashboard deliberately suppresses consumption after any recorded
observation allocation fault, and displays that lifetime fault count explicitly.
That is a conservative qualification gate, not a production recovery policy.
Multi-resource views retain resource identity instead of merging independent
database envelopes.

Successful collection does not imply observation is enabled. Coverage checks
must also require `pgbouncer_workload_status_enabled == 1`, freshness, and expected
targets. Do not render failed/missing collectors as zero consumption.

The fixed labels never include raw SQL, trace IDs, or arbitrary new SQL columns.
Deployment supplies process/fleet identity and resource mapping. Independent
physical resources must not be merged merely because aliases or callers match.
