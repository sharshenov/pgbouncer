# Workload observation implementation

The current increment adds passive observation to the canonical outstanding
request lifecycle and exposes `SHOW WORKLOAD_STATS` / `SHOW WORKLOAD_STATUS`.
It does not implement admission control. The stand and original prototype are
separate. Qualification, collection integration, and the full protocol/failure
matrix remain work in progress.

## Tag boundary

`workload_parse_tags` accepts an available SQL prefix and an explicit indication
of whether that prefix contains the complete SQL string. It never allocates,
waits for another packet, changes SQL, or reads beyond the supplied range.
Inspection is capped at 4,096 bytes.

- Recognize only leading SQL comments and the fixed, lower-case `team` and
  `caller` keys used by the application. Other dimensions do not create identity.
- Ignore prose, optimizer hints, and nested block comments. Do not interpret
  apparent tags in SQL literals or trailing comments.
- Decode percent escapes once, validate UTF-8, and reject control characters.
  Literal `+` is not a form-encoded space.
- Retain at most 128 decoded team bytes and 512 decoded caller bytes. Longer
  values are explicitly unsupported; no identity is silently truncated.
- Reject duplicate recognized keys, including across leading tag comments.
  A malformed tag list cannot partially update attribution.
- Return distinct valid, missing, malformed, and unsupported outcomes. Missing
  attribution can retain one known dimension. Other failure outcomes clear both.
- If a fragment ends before classification can be completed, report unsupported
  attribution rather than forcing full-packet buffering or inheriting old tags.

The fixed bounds are implementation limits, not cardinality limits or a budget
identity scheme. Stable policy IDs and bounded statistics storage are separate
boundaries. Raw SQL and unrelated tags must not become metric labels.

## Validation

`workload_tags_test` runs through both Make and Meson. It covers application-shaped
encoding, Unicode, malformed/duplicate keys, prose and SQL literal isolation,
partial prefixes, bounded scanning, and exact versus oversized identities.

## Accounting ownership

Each forwarded simple Query or extended Execute request owns an immutable stats
row reference until its canonical response, error recovery, or server close.
Parsing/Binds update statement/portal metadata, never an already forwarded
operation's owner. Named statements are resolved through the prepared module's
own cache lookup, not another module's hash macros.

Preparation and execution are correlated by unique statement/Bind tokens, not
aggregate-row equality. Sync epochs fence speculative unnamed metadata: ignored
packets after an error cannot overwrite it, while valid metadata after Sync is
preserved. Epoch state survives disabling observation; disabled traffic releases
the optional metadata cache without releasing outstanding execution claims.

The proxy cannot know individual executor start times. It assigns active wall
time serially in response order, starting the first forwarded execution unit
when related preparation is at the queue frontier, and later units when earlier
units or unrelated protocol barriers finish. This includes response
transfer/proxy effects and may include preceding Parse/Bind work. Pending units
remain visible as in-flight, but their pipeline wait is accounted separately.
These are proxy observations, not CPU measurements or exact server execution
latency distributions. A cached last-blocking request avoids rescanning long
metadata prefixes on every response.

Simple multi-statement SQL is one exchange, not several counted statements.
The first observed operation pins backend transaction ownership until idle
ReadyForQuery. An untagged COMMIT cannot move that occupancy to another caller.
COPY temporarily transfers the observation into its server-side protocol phase
when the normal outstanding request is drained. Extended COPY finishes at its
CommandComplete; a simple multi-statement Query remains one operation through
ReadyForQuery.
Completed and aborted execution time is added directly; it is never calculated
by subtracting asynchronously updated cumulative pool-wait counters.

Named portals, Execute-only continuations, and unavailable/oversized attribution
prefixes use an unsupported aggregate. Named prepared statements with unnamed
portals are supported when canonical PgBouncer tracking is enabled. This
coverage boundary does not reject, delay, replay, or rewrite unsupported SQL.

Rows persist without LRU eviction for the lifetime of a configured database.
Four fixed fallback rows preserve missing, malformed, unsupported, and overflow
traffic. Detailed cardinality limits apply per database; lowering a limit does
not evict live rows or history. Allocation failure is visible as a coverage fault.
Database deletion resets that database's observation history and increments a
process-level reset counter. No policy ID, debt, or enforcement state is modeled
by these observation rows.

The initial observation contract excludes replication, authentication queries,
and PgBouncer-generated internal protocol work from application-operation counts.
It also does not charge an operation that PostgreSQL skipped as executed work.
Those boundaries must be considered when comparing with database-wide metrics.
