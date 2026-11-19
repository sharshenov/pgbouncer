# Synthetic workload targets

This stand qualifies proxy behavior; it does not reproduce a production database.
Internal monitoring evidence is recorded privately in BE-1054, not published in
this repository. Never add production SQL samples, identifiers, or credentials.

## Independent scale axes

- Ramp point-read traffic through 5k, 15k, 25k, and 40k SQL executions/s total.
  Count SQL executions separately from driver operations and protocol packets.
- Independently test 256, 1,024, and 3,500 connected clients.
- Generate active-backend occupancy and pool queues independently of throughput
  using transactions, controlled idle intervals, sleeps, and lock contention.
  Sleeps simulate occupancy, not CPU/IO cost.
- Keep healthy callers alongside an overloaded caller. Include long-running
  work, unknown tags, 100/1,000/10,000 callers, and large/fragmented packets.
- Use multiple independent resources to verify resource separation.

The default topology and driver are the starting point, not complete coverage
of all these scenarios. Expand them with the compatibility and control tests.

## Proposed measurement gates

These are engineering defaults, not measured production facts or business SLOs.
Refine them against measured variance before qualification:

- No observation-induced protocol errors, notices, or admission delay.
- At most 5% throughput regression and 10% CPU-per-completed-request increase
  against the same-version feature-disabled baseline.
- At most 10% p99 regression or 0.2 ms absolute increase, whichever is larger.
- Ten-minute warmup, three paired ten-minute measurements, two-hour intermediate
  soak, then twelve-hour qualification soak with failure scenarios.
- Bounded memory/cardinality, no leaked claims, stable completed-work
  reconciliation, and continued administrative access under pressure.

Record generator CPU, missed arrival deadlines, errors, throughput, and latency
alongside proxy results. An undersized generator or noisy environment makes a
comparison inconclusive; it does not establish that a target passed.

Soak duration alone is not sufficient. Observation and local-control readiness
must receive separate verdicts, neither implying fleet-wide enforcement safety.
