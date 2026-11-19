# Workload observation implementation

The initial increment implements **tag parsing only**. It does not connect the
parser to protocol handling, expose workload metrics, or implement admission
control. The stand and original prototype are separate.

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
