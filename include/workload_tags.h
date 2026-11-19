/*
 * PgBouncer - Lightweight connection pooler for PostgreSQL.
 *
 * Copyright (c) 2026 PgBouncer contributors
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef WORKLOAD_TAGS_H
#define WORKLOAD_TAGS_H

#include <usual/base.h>

#define WORKLOAD_TEAM_MAX 128
#define WORKLOAD_CALLER_MAX 512
#define WORKLOAD_PREFIX_MAX 4096

enum WorkloadTagStatus {
	WORKLOAD_TAGS_VALID,
	WORKLOAD_TAGS_MISSING,
	WORKLOAD_TAGS_MALFORMED,
	WORKLOAD_TAGS_UNSUPPORTED,
};

struct WorkloadTags {
	char team[WORKLOAD_TEAM_MAX + 1];
	char caller[WORKLOAD_CALLER_MAX + 1];
};

/*
 * Inspect only a bounded leading SQL prefix, without allocations or requiring
 * packet buffering. complete means length covers the entire SQL string, not
 * merely all currently received bytes. SQL need not be NUL-terminated.
 *
 * Only lower-case team/caller keys are recognized. Values are percent-decoded
 * UTF-8; '+' stays literal. Overlong values are unsupported, never truncated.
 * Missing attribution may retain one known dimension; malformed/unsupported
 * attribution always returns an empty set.
 */
enum WorkloadTagStatus workload_parse_tags(const char *sql, size_t length,
					   bool complete, struct WorkloadTags *tags);

#endif
