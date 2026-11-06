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
	QTAG_PKT_CONSUMED,	/* packet handled (rejected), do not forward */
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

/* packet inspection, returns a QTagPacketAction */
enum QTagPacketAction qtags_handle_query_packet(PgSocket *client, PktHdr *pkt);
enum QTagPacketAction qtags_handle_parse_packet(PgSocket *client, PktHdr *pkt);
enum QTagPacketAction qtags_handle_bind_packet(PgSocket *client, PktHdr *pkt);
bool qtags_inspect_needed(int pkt_type);

/* accounting, called from the server response path */
void qtags_account_query(PgSocket *client, usec_t total);
void qtags_account_xact(PgSocket *client, usec_t total);
void qtags_account_wait(PgSocket *client, usec_t waited);
void qtags_account_budget_wait(PgSocket *client, usec_t waited);
void qtags_client_release(PgSocket *client);

void qtags_clear(PgSocket *client);
void qtags_parse(PgSocket *client, const char *query);

/* config file [tag_budgets] section and reload bookkeeping */
bool qtags_budget_from_config(const char *tenant, const char *params);
void qtags_config_dead(bool flag);
void qtags_config_sweep(void);

/* admin console */
bool qtags_admin_budget_cmd(PgSocket *admin, const char *arg);
bool qtags_show_tag_stats(PgSocket *client);
bool qtags_show_tag_budgets(PgSocket *client);

#endif
