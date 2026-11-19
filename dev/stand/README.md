# Synthetic PgBouncer stand

Implements the foundation of BE-1053 / BE-1054. **Synthetic data only.** This is
not a production deployment and does not copy production schemas or rows.

## Safety and topology

- Dedicated `pgobs` Compose project, isolated **internal** network. Never attach
  external networks, mount application secrets, or pass production DSNs.
- Two disposable PostgreSQL servers, two transaction-pooling proxies, two exporter
  instances, Prometheus, and an ephemeral load-generator container.
- Only proxy ports `26432` / `26433` and Prometheus `26909` are published, on
  **127.0.0.1**. PostgreSQL has no published port.
- Explicit per-service CPU/memory limits. Databases use named volumes; state and
  generated stand-only credentials live in ignored `.state/`. Do not publish it.
- The load driver accepts no database destination. Its only targets are the
  stand's service names, on the isolated network. No remote execution identity
  belongs in this repository or tickets.
- One stand per Docker daemon for now. Check port availability before starting.
  Never use global Docker prune or modify unrelated containers.

## Run

Docker with Compose v2 and Python 3 are required. Run commands from a checkout:

```sh
python3 dev/stand/stand.py build --variant upstream --revision UPSTREAM_COMMIT
python3 dev/stand/stand.py up --variant upstream
python3 dev/stand/stand.py run --profile smoke --duration 30 --clients 16
python3 dev/stand/stand.py run --profile mixed --duration 300 --clients 256 --rate 10000
python3 dev/stand/stand.py run --profile throughput --duration 300 --clients 256 --rate 40000 --processes 4
python3 dev/stand/stand.py status
python3 dev/stand/stand.py down    # retains only this project's data
```

The initial checkout is upstream `ac3bb6d748cefc9470c2c66e061549a59a072ce3`.
Dependency images are pinned in `images.lock.json`. To refresh deliberately,
remove that lock and run `python3 dev/stand/stand.py lock`, then review the diff.
Build that exact source separately with `--source` once candidate changes exist.
Use `--variant candidate` for feature source; `--variant legacy` builds release
1.22.1 at its pinned commit. Switch only after `down`. Each result records the
source digest, build system, immutable local image ID, and dependency digests.

Build and execute upstream tests in isolated containers:

```sh
python3 dev/stand/stand.py test --build-system autoconf
python3 dev/stand/stand.py test --build-system meson
```

These use no host network, privileges, or firewall modifications. Tests requiring
those facilities may be skipped; record skips explicitly.

`down --destroy-data` deletes **only this Compose project's** databases and metrics.
Never use it until results needed for comparison have been retained.

## Workloads and interpretation

`throughput` measures indexed point reads, `mixed` adds updates, short explicit
transactions and slow work, and `occupancy` creates controlled active backend
occupancy. Names and tags are fabricated. Prepared statement reuse is exercised.

`--rate` is **operations/s**, not SQL messages/s. A mixed transaction uses four
SQL messages. Results report both counts. Rate zero is closed-loop saturation;
nonzero rates schedule arrivals and shed missed arrivals rather than queue
unbounded work in the driver. Reports include latency histogram upper bounds,
scheduling lag, dropped arrivals, errors, notices, and achieved throughput.
Histogram precision is approximately 10%; it is not a per-query exact percentile.
Use up to four generator processes to avoid a single Python interpreter becoming
the throughput ceiling. They share the load container's CPU/memory limits, split
the requested clients/rate, and synchronize their measurement start. Histograms
are merged by bucket counts, never by averaging individual percentiles.

The driver is not a complete protocol/fault qualification harness. COPY,
fragmentation, cancellation, lock contention, TLS, prepared-cache reassignment,
and exact accounting need the separate compatibility tests.

See [workload-targets.md](workload-targets.md) for synthetic scale and proposed
qualification criteria. Internal monitoring evidence belongs in the private
tracking issue, not this repository. A successful smoke run is not production-scale
qualification, and a synthetic point-read benchmark is not a database replica.
