/*
 * PgBouncer - Lightweight connection pooler for PostgreSQL.
 *
 * Copyright (c) 2025 PgBouncer contributors
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

/**
 * Query tag monitoring and traffic control.
 *
 * Applications can attach tags to SQL statements as SQL comments
 * (SQLCommenter style, e.g. "/\* team=payments action=checkout *\ /").
 * PgBouncer parses the tags out of the query text, aggregates the time
 * spent in the database per tag combination (SHOW TAG_STATS) and can
 * enforce a reverse leaky bucket budget per tenant (SHOW TAG_BUDGETS,
 * TAG_BUDGET command).
 *
 * Enforcement queues the query inside PgBouncer until the budget has
 * room, so an over budget tenant only experiences higher latency and
 * never an error. See dev/query-tag-traffic-control.md for the design.
 *
 * The implementation is split over three files:
 *
 * - src/qtags.c: tag parsing, packet dispatch and per tenant stats
 * - src/qtags_budget.c: the budget store, admission and wait queue
 * - src/qtags_admin.c: [tag_budgets] config parsing and the admin console
 */
#ifndef QTAGS_H
#define QTAGS_H

#include "util.h"

/* Maximum lengths for parsed tag keys and values (truncated beyond this). */
#define QTAG_MAX_KEY 32
#define QTAG_MAX_VALUE 64

/*
 * Tags parsed from the query that is currently in flight on a client
 * socket. Fixed size to keep the hot path allocation free.
 */
struct QTagSet {
	char tenant[QTAG_MAX_VALUE + 1];	/* value of the tenant tag */
	char sec_key[QTAG_MAX_KEY + 1];		/* first matching secondary key */
	char sec_value[QTAG_MAX_VALUE + 1];	/* value of the secondary tag */
	char active_tenant[QTAG_MAX_VALUE + 1];	/* tenant holding a concurrency slot */
};

/* Admission decision for a query */
enum QTagDecision {
	QTAG_ALLOW = 0,
	QTAG_WARN = 1,		/* over budget, run anyway with a notice */
	QTAG_WAIT = 2,		/* over budget, queue until the bucket drains */
	QTAG_REJECT = 3,	/* query can never fit the budget, error it */
};

/* result of the packet inspection hooks */
enum QTagPacketAction {
	QTAG_PKT_CONTINUE = 0,	/* process the packet normally */
	QTAG_PKT_CONSUMED,	/* packet rejected, caller must consume it */
	QTAG_PKT_PARKED,	/* client queued by budget, packet still pending */
};

/* Enforcement mode (tag_budget_mode config parameter and per budget) */
enum QTagBudgetMode {
	QTAG_MODE_OFF = 0,
	QTAG_MODE_WARN = 1,
	QTAG_MODE_ENFORCE = 2,
};

/* Lookup table for the tag_budget_mode config parameter */
extern const struct CfLookup tag_budget_mode_map[];

void qtags_init(void);
void qtags_config_changed(void);

/* is query tag parsing configured? */
bool qtags_feature_enabled(void);

/*
 * Parse tags from the packet text and run the budget check. Returns
 * what the caller should do with the packet (src/qtags.c).
 */
enum QTagPacketAction qtags_process_packet(PgSocket *client, PktHdr *pkt);

/* does this packet type carry query text that we parse tags from? */
bool qtags_inspect_needed(int pkt_type);

/* accounting, called from the server response path (src/qtags.c) */
void qtags_account_query(PgSocket *client, usec_t total);
void qtags_account_xact(PgSocket *client, usec_t total);
void qtags_account_wait(PgSocket *client, usec_t waited);

/* admin console (src/qtags.c and src/qtags_admin.c) */
bool qtags_show_tag_stats(PgSocket *client);
bool qtags_show_tag_budgets(PgSocket *client);
bool qtags_admin_budget_cmd(PgSocket *admin, const char *arg);

/*
 * Internal interface between the budget engine and the other qtags
 * files. Everything below is shared state, do not use outside src/.
 */

/* A leaky bucket budget for one tenant tag value. */
struct TagBudget {
	UT_hash_handle hh;
	struct List lru;		/* in tag_budget_lru, most recent first */

	char tenant[QTAG_MAX_VALUE + 1];

	/* limits, -1 = inherit from the global config */
	int share_pct;			/* % of DB time per second */
	int burst_sec;			/* bucket size in DB time seconds, 0 = no bucket */
	int per_query_ms;		/* queue queries estimated above this, 0 = off */
	int concurrency;		/* max in-flight queries, 0 = off */
	int mode;			/* enum QTagBudgetMode, -1 = inherit */

	bool from_config;		/* listed in the [tag_budgets] section */
	bool from_admin;		/* created/modified via the admin console */
	bool dead;			/* config reload bookkeeping */

	/* runtime state */
	usec_t debt;			/* us of DB time owed */
	usec_t last_update;		/* clock of the last lazy drain */
	uint64_t ema_cost;		/* EMA of query cost in us */
	uint64_t queries;		/* queries admitted without waiting */
	uint64_t active;		/* in-flight queries */
	uint64_t queued_count;		/* queries that had to wait */
	uint64_t rejected_count;	/* queries rejected with an error */
	uint64_t warned_count;		/* queries that ran with a warning */
};

extern struct TagBudget *tag_budget_map;
extern struct StatList tag_budget_lru;

struct TagBudget *qtags_find_budget(const char *tenant);

/* resolve a budget parameter: per budget value, else the global config */
int qtags_budget_share_pct(struct TagBudget *b);
int qtags_budget_burst_sec(struct TagBudget *b);
int qtags_budget_per_query_ms(struct TagBudget *b);
int qtags_budget_concurrency(struct TagBudget *b);
int qtags_budget_mode(struct TagBudget *b);

/* lazily drain the bucket: share_pct % of one DB-second per wall second */
void qtags_budget_drain(struct TagBudget *b, usec_t now);

/*
 * Run the admission check for the batch starting on the client. Returns
 * the decision and, for QTAG_WAIT, how many microseconds the query has
 * to wait for its bucket to drain (in *wait_needed).
 */
enum QTagDecision qtags_budget_admit(PgSocket *client, usec_t *wait_needed);

/*
 * Charge a finished batch to the tenant that holds its concurrency
 * slot (or to its tags when none was taken), then free the slot.
 */
void qtags_budget_account_query(PgSocket *client, usec_t cost);

/* the client connection is going away: give back slot and waiter state */
void qtags_client_release(PgSocket *client);

/*
 * Queue the client until its budget has room. The current packet stays
 * pending in the sbuf and is re-delivered on wake. Returns
 * QTAG_PKT_PARKED ("do not touch the sbuf") or QTAG_PKT_CONSUMED when
 * parking failed and the client was disconnected.
 */
enum QTagPacketAction qtags_budget_park(PgSocket *client, usec_t wait_us);

/* wake every queued client; used when budgets change */
void qtags_budget_wake_waiters(void);

/* [tag_budgets] config section and reload bookkeeping (src/qtags_admin.c) */
bool qtags_parse_tag_budget(void *base, const char *tenant, const char *params);
void qtags_config_dead(bool flag);
void qtags_config_sweep(void);

#endif
