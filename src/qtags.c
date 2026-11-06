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

/*
 * Query tag parsing and per tenant stats.
 *
 * A "reverse leaky bucket" models a budget as a bucket of debt: every
 * query that runs adds the database wall time it consumed to the bucket,
 * and the bucket drains at a configured rate (the tenant's share). A
 * query that would overflow the bucket is queued inside PgBouncer until
 * the bucket has drained enough, so the client only experiences higher
 * latency. This file parses the tags, turns the parsed tags into an
 * admission decision via the budget engine (src/qtags_budget.c) and
 * keeps the per tenant stats. The [tag_budgets] config section and the
 * admin console live in src/qtags_admin.c.
 *
 * See dev/query-tag-traffic-control.md for the full design.
 */

#include "bouncer.h"

#include <usual/hashtab-impl.h>


/*
 * Benchmarking showed that HASH_BER is one of the fastest hash functions
 * for our usecases (same choice as prepare.c).
 */
#undef HASH_FUNCTION
#define HASH_FUNCTION HASH_BER

/* number of secondary tag keys that can be configured */
#define QTAG_MAX_SECONDARY_KEYS 16

/*
 * Aggregated stats for one (database, tenant, secondary tag) combination.
 * Times are cumulative microseconds, same units as SHOW STATS.
 */

/* size of the packed (db, tenant, sec_key, sec_value) hash key */
#define QTAG_STATS_KEYLEN \
	((MAX_DBNAME + 1) + (QTAG_MAX_VALUE + 1) + (QTAG_MAX_KEY + 1) + (QTAG_MAX_VALUE + 1))

struct TagStats {
	UT_hash_handle hh;
	struct List lru;		/* in tag_stats_lru, most recent first */

	/*
	 * key: db, tenant, sec_key, sec_value NUL-joined into one packed
	 * byte string. The key must live in a packed buffer, because uthash
	 * compares the raw bytes at the stored key pointer.
	 */
	char hkey[QTAG_STATS_KEYLEN];
	size_t hkeylen;

	/* values */
	uint64_t query_count;
	uint64_t query_time;		/* us, includes pool wait */
	uint64_t wait_time;		/* us queued for a server */
	uint64_t budget_wait_time;	/* us queued by the tag budget */
	uint64_t xact_count;
	uint64_t xact_time;		/* us */
	uint64_t last_seen;		/* unix seconds of last completed query */
};

static struct TagStats *tag_stats_map;

static STATLIST(tag_stats_lru);

/* parsed secondary keys from cf_query_tag_keys */
static char secondary_keys[QTAG_MAX_SECONDARY_KEYS][QTAG_MAX_KEY + 1];
static int secondary_key_count;

static void qtags_clear_tags(PgSocket *client);
static void qtags_parse(PgSocket *client, const char *query);
static enum QTagPacketAction admit_packet(PgSocket *client, PktHdr *pkt);
static void send_budget_warning(PgSocket *client);
static void send_budget_rejection(PgSocket *client);
static void qtags_account_budget_wait(PgSocket *client, usec_t waited);
static void tag_stats_lru_touch(struct TagStats *s);

bool qtags_feature_enabled(void)
{
	return cf_query_tag_tenant_key[0] != 0;
}

void qtags_init(void)
{
	qtags_config_changed();
}

/* re-parse the secondary key list after a config (re)load */
void qtags_config_changed(void)
{
	const char *p = cf_query_tag_keys;

	secondary_key_count = 0;
	while (p && *p && secondary_key_count < QTAG_MAX_SECONDARY_KEYS) {
		const char *end = strchr(p, ',');
		size_t len = end ? (size_t)(end - p) : strlen(p);

		while (len > 0 && (*p == ' ' || *p == '\t')) {
			p++;
			len--;
		}
		while (len > 0 && (p[len - 1] == ' ' || p[len - 1] == '\t'))
			len--;
		if (len > 0) {
			int n = MIN((int)len, QTAG_MAX_KEY);
			char *dst = secondary_keys[secondary_key_count];

			memcpy(dst, p, n);
			dst[n] = 0;
			secondary_key_count++;
		}
		if (!end)
			break;
		p = end + 1;
	}
}

/* ------------------------------ tag parsing ------------------------------ */

static bool tag_key_char(char c)
{
	return isalnum((unsigned char)c) || c == '_' || c == '-' || c == '.';
}

static bool tag_value_char(char c)
{
	return isalnum((unsigned char)c) || strchr("-_.:/@%+~", c) != NULL;
}

/* store a recognized tag */
static void record_tag(const char *key, size_t key_len, const char *val, size_t val_len,
		       struct QTagSet *tags)
{
	int i;

	if (!tags->tenant[0] && strlen(cf_query_tag_tenant_key) == key_len
	    && strncasecmp(key, cf_query_tag_tenant_key, key_len) == 0) {
		int n = MIN((int)val_len, QTAG_MAX_VALUE);

		memcpy(tags->tenant, val, n);
		tags->tenant[n] = 0;
		return;
	}

	if (tags->sec_key[0])
		return;

	for (i = 0; i < secondary_key_count; i++) {
		if (strlen(secondary_keys[i]) == key_len
		    && strncasecmp(key, secondary_keys[i], key_len) == 0) {
			int nk = MIN((int)key_len, QTAG_MAX_KEY);
			int nv = MIN((int)val_len, QTAG_MAX_VALUE);

			/* store the canonical config spelling of the key */
			memcpy(tags->sec_key, secondary_keys[i], nk);
			tags->sec_key[nk] = 0;
			memcpy(tags->sec_value, val, nv);
			tags->sec_value[nv] = 0;
			return;
		}
	}
}

/* fill in tags that are still empty from earlier comments */
static void merge_tags(const struct QTagSet *from, struct QTagSet *into)
{
	if (from->tenant[0] && !into->tenant[0]) {
		memcpy(into->tenant, from->tenant, sizeof(into->tenant));
		memcpy(into->sec_key, from->sec_key, sizeof(into->sec_key));
		memcpy(into->sec_value, from->sec_value, sizeof(into->sec_value));
	} else if (from->sec_key[0] && !into->sec_key[0]) {
		memcpy(into->sec_key, from->sec_key, sizeof(into->sec_key));
		memcpy(into->sec_value, from->sec_value, sizeof(into->sec_value));
	}
}

/*
 * Try to parse the body of a C-style block comment (between the markers)
 * as a tag comment. Returns true when the comment consisted solely of
 * key=value or key:value pairs (SQLCommenter / marginalia style), so
 * prose comments never get mistaken for tags. The parsed tags are
 * collected in a scratch set; the caller commits them only when the
 * whole comment parsed, so a comment that stops being a tag list
 * partway through records nothing.
 */
static bool parse_tag_comment(const char *p, const char *end, struct QTagSet *tags)
{
	struct QTagSet parsed;

	if (p < end && *p == '+')	/* planner hint comment, not tags */
		return false;

	memset(&parsed, 0, sizeof(parsed));

	while (p < end) {
		char key[QTAG_MAX_KEY + 1];
		char val[QTAG_MAX_VALUE + 1];
		char sep;
		size_t len = 0;

		/* skip separators */
		while (p < end && (*p == ',' || isspace((unsigned char)*p)))
			p++;
		if (p == end)
			break;

		/* key */
		while (p < end && tag_key_char(*p)) {
			if (len < QTAG_MAX_KEY)
				key[len] = *p;
			len++;
			p++;
		}
		key[MIN((int)len, QTAG_MAX_KEY)] = 0;
		if (len == 0 || len > QTAG_MAX_KEY || p == end)
			return false;
		sep = *p++;
		if (sep != '=' && sep != ':')
			return false;

		/* value: single quoted, or a run of safe characters */
		if (p < end && *p == '\'') {
			size_t vlen = 0;

			p++;
			while (p < end && *p != '\'') {
				if (*p == '*' && p + 1 < end && p[1] == '/')
					return false;
				if (vlen < QTAG_MAX_VALUE)
					val[vlen] = *p;
				vlen++;
				p++;
			}
			if (p == end)	/* unterminated quote */
				return false;
			p++;	/* closing quote */
			val[MIN((int)vlen, QTAG_MAX_VALUE)] = 0;
		} else {
			size_t vlen = 0;

			while (p < end && tag_value_char(*p)) {
				if (vlen < QTAG_MAX_VALUE)
					val[vlen] = *p;
				vlen++;
				p++;
			}
			if (vlen == 0 || vlen > QTAG_MAX_VALUE)
				return false;
			val[vlen] = 0;
		}

		record_tag(key, MIN((int)len, QTAG_MAX_KEY), val, strlen(val), &parsed);
	}
	merge_tags(&parsed, tags);
	return true;
}

/*
 * Clear the parsed tags, but keep active_tenant: the concurrency slot is
 * held until the batch completes, even when a re-delivered packet
 * re-parses tags on the way (e.g. the varcache pause/resume on a fresh
 * server connection).
 */
static void qtags_clear_tags(PgSocket *client)
{
	char active_tenant[QTAG_MAX_VALUE + 1];

	memcpy(active_tenant, client->qtags.active_tenant, sizeof(active_tenant));
	memset(&client->qtags, 0, sizeof(client->qtags));
	memcpy(client->qtags.active_tenant, active_tenant, sizeof(active_tenant));
}

/*
 * Parse tags out of a query text into client->qtags. Tags of a previous
 * query are cleared first. Only comments that precede any statement
 * text are parsed (SQLCommenter places them at the start), and only
 * comments that consist entirely of key=value pairs count. Unknown keys
 * are ignored, so high cardinality tags like trace_id create no state.
 */
static void qtags_parse(PgSocket *client, const char *query)
{
	const char *p = query;
	const char *scan = query;	/* start of the not-yet-scanned text */
	bool leading = true;

	qtags_clear_tags(client);
	if (!qtags_feature_enabled() || !query)
		return;

	while (leading && (p = strchr(p, '/')) != NULL) {
		const char *inner, *end, *q;

		if (p[1] != '*') {
			p++;
			continue;
		}
		inner = p + 2;
		end = strstr(inner, "*/");
		if (!end)
			break;	/* unterminated comment, no more tags */

		/* a comment only counts while no statement text has appeared */
		for (q = scan; q < p; q++) {
			if (!isspace((unsigned char)*q)) {
				leading = false;
				break;
			}
		}
		if (leading)
			parse_tag_comment(inner, end, &client->qtags);
		p = end + 2;
		scan = p;
	}
}

/* --------------------------- packet dispatch ----------------------------- */

/* the packet types whose text we parse tags from */
bool qtags_inspect_needed(int pkt_type)
{
	if (!qtags_feature_enabled())
		return false;
	return pkt_type == PqMsg_Query || pkt_type == PqMsg_Parse;
}

/*
 * Parse the query text of the packet and admit it to the tenant's
 * budget. Returns what the caller should do with the packet:
 *
 * - QTAG_PKT_CONTINUE: process the packet normally
 * - QTAG_PKT_CONSUMED: the query was rejected, the caller must consume
 *   the packet so that it never reaches the server
 * - QTAG_PKT_PARKED: the client was queued by its budget and the packet
 *   stays pending in the sbuf for re-delivery on wake
 */
enum QTagPacketAction qtags_process_packet(PgSocket *client, PktHdr *pkt)
{
	if (!qtags_feature_enabled() || client->pool->db->admin)
		return QTAG_PKT_CONTINUE;

	switch (pkt->type) {
	case PqMsg_Query: {
		const char *query;
		bool ok;

		ok = mbuf_get_string(&pkt->data, &query);
		pkt_rewind_v3(pkt);
		if (!ok)
			return QTAG_PKT_CONTINUE;
		qtags_parse(client, query);
		break;
	}

	case PqMsg_Parse: {
		const char *name, *query;
		bool ok;

		ok = mbuf_get_string(&pkt->data, &name);
		if (ok)
			ok = mbuf_get_string(&pkt->data, &query);
		pkt_rewind_v3(pkt);
		if (!ok)
			return QTAG_PKT_CONTINUE;
		qtags_parse(client, query);
		break;
	}

	case PqMsg_Bind: {
		/*
		 * For named statements the tags have to come from the
		 * stored query text, because the Parse packet only flies
		 * by once. Unnamed statements keep the tags of the Parse
		 * packet that is still in flight on this client.
		 */
		const char *portal, *stmt, *query;
		bool ok;

		if (!is_prepared_statements_enabled(client))
			break;

		/*
		 * With prepared statements enabled the caller fully buffers
		 * Bind packets before dispatch, so this is defensive. The
		 * batch was already admitted on the Parse packet's tags, so
		 * skip admission instead of re-running it on stale state.
		 */
		if (incomplete_pkt(pkt))
			return QTAG_PKT_CONTINUE;

		ok = mbuf_get_string(&pkt->data, &portal);
		if (ok)
			ok = mbuf_get_string(&pkt->data, &stmt);
		pkt_rewind_v3(pkt);
		if (!ok || !stmt[0])
			break;

		query = get_prepared_statement_query(client, stmt);
		if (query) {
			qtags_parse(client, query);
		} else {
			/*
			 * Binding an unknown statement is a protocol error
			 * that the server will report; keep the tags of a
			 * previous query off the books.
			 */
			qtags_clear_tags(client);
		}
		break;
	}

	default:
		return QTAG_PKT_CONTINUE;
	}

	return admit_packet(client, pkt);
}

/*
 * Run the budget check for the batch starting on the client and act on
 * the decision.
 */
static enum QTagPacketAction admit_packet(PgSocket *client, PktHdr *pkt)
{
	enum QTagDecision decision;
	usec_t wait_needed = 0;

	decision = qtags_budget_admit(client, &wait_needed);

	/*
	 * Every decision but WAIT ends the queueing episode: account the
	 * time the batch spent waiting for its budget, wherever the batch
	 * ends up (allowed, warned, or rejected).
	 */
	if (client->qtag_wait_since && decision != QTAG_WAIT) {
		usec_t waited = get_cached_time() - client->qtag_wait_since;

		client->qtag_wait_since = 0;
		qtags_account_budget_wait(client, waited);
	}

	switch (decision) {
	case QTAG_ALLOW:
		return QTAG_PKT_CONTINUE;

	case QTAG_WARN:
		send_budget_warning(client);
		return QTAG_PKT_CONTINUE;

	case QTAG_REJECT:
		send_budget_rejection(client);

		/*
		 * A simple protocol query outside a transaction owns the
		 * protocol state until the next ReadyForQuery, so the
		 * exchange can be completed here. In all other cases the
		 * transaction state of the linked server decides when
		 * ReadyForQuery comes, only the error is injected.
		 */
		if (pkt->type == PqMsg_Query && !client->xact_start && !client->link) {
			bool res;

			SEND_ReadyForQuery(res, client);
			(void)res;	/* a failed send surfaces as a disconnect */
		}
		return QTAG_PKT_CONSUMED;

	case QTAG_WAIT:
		/* the packet stays pending, the client is queued */
		return qtags_budget_park(client, wait_needed);
	}
	return QTAG_PKT_CONTINUE;
}

static void send_budget_warning(PgSocket *client)
{
	char msg[160];
	bool res;

	snprintf(msg, sizeof(msg),
		 "query tag budget reached for tenant '%s', running anyway",
		 client->qtags.tenant);
	SEND_generic(res, client, PqMsg_NoticeResponse, "cscscsc",
		     'S', "WARNING", 'C', "C53000", 'M', msg, 0);
	(void)res;	/* a failed send surfaces as a client disconnect */
}

static void send_budget_rejection(PgSocket *client)
{
	char msg[160];
	bool res;

	snprintf(msg, sizeof(msg),
		 "query rejected: tag budget for tenant '%s' does not allow "
		 "this query", client->qtags.tenant);
	slog_warning(client, "rejecting query: %s", msg);
	SEND_generic(res, client, PqMsg_ErrorResponse, "cscscsc",
		     'S', "ERROR", 'C', "C53000", 'M', msg, 0);
	(void)res;	/* a failed send surfaces as a client disconnect */
}

/* ---------------------------- stats accounting --------------------------- */

/*
 * Build the composite hash key (db, tenant, sec_key, sec_value) in buf.
 * The TagStats buffers are laid out identically, so the same byte string
 * is used for HASH_ADD.
 */
static size_t stats_key(char *buf, size_t buflen, const char *db, const struct QTagSet *tags)
{
	size_t len;

	if (strlen(db) + 1 + strlen(tags->tenant) + 1 + strlen(tags->sec_key) + 1
	    + strlen(tags->sec_value) + 1 > buflen)
		return 0;

	len = strlcpy(buf, db, buflen) + 1;
	len += strlcpy(buf + len, tags->tenant, buflen - len) + 1;
	len += strlcpy(buf + len, tags->sec_key, buflen - len) + 1;
	len += strlcpy(buf + len, tags->sec_value, buflen - len) + 1;
	return len;
}

static void tag_stats_lru_touch(struct TagStats *s)
{
	statlist_remove(&tag_stats_lru, &s->lru);
	statlist_prepend(&tag_stats_lru, &s->lru);
}

static struct TagStats *get_stats_entry(PgSocket *client)
{
	struct TagStats *s;
	char key[QTAG_STATS_KEYLEN];
	size_t len;

	if (!qtags_feature_enabled())
		return NULL;
	if (!client->pool || !client->pool->db || client->pool->db->admin)
		return NULL;
	if (!client->qtags.tenant[0])
		return NULL;

	len = stats_key(key, sizeof(key), client->pool->db->name, &client->qtags);
	if (len == 0)
		return NULL;

	HASH_FIND(hh, tag_stats_map, key, len, s);
	if (s) {
		tag_stats_lru_touch(s);
		return s;
	}

	if (cf_tag_stats_max > 0 && HASH_COUNT(tag_stats_map) >= (unsigned)cf_tag_stats_max) {
		struct List *item = statlist_last(&tag_stats_lru);

		if (item) {
			struct TagStats *old = container_of(item, struct TagStats, lru);

			HASH_DELETE(hh, tag_stats_map, old);
			statlist_remove(&tag_stats_lru, &old->lru);
			free(old);
		}
	}

	s = calloc(1, sizeof(*s));
	if (!s)
		return NULL;
	memcpy(s->hkey, key, len);
	s->hkeylen = len;
	HASH_ADD_KEYPTR(hh, tag_stats_map, s->hkey, s->hkeylen, s);
	statlist_prepend(&tag_stats_lru, &s->lru);
	return s;
}

void qtags_account_query(PgSocket *client, usec_t total)
{
	struct TagStats *s = get_stats_entry(client);

	if (s) {
		s->query_count++;
		s->query_time += total;
		s->last_seen = get_cached_time() / USEC;
	}

	qtags_budget_account_query(client, total);
}

void qtags_account_xact(PgSocket *client, usec_t total)
{
	struct TagStats *s = get_stats_entry(client);

	if (s) {
		s->xact_count++;
		s->xact_time += total;
	}
}

void qtags_account_wait(PgSocket *client, usec_t waited)
{
	struct TagStats *s = get_stats_entry(client);

	if (s)
		s->wait_time += waited;
}

static void qtags_account_budget_wait(PgSocket *client, usec_t waited)
{
	struct TagStats *s = get_stats_entry(client);

	if (s)
		s->budget_wait_time += waited;
}

/* unpack the segments of the composite key for display */
static void stats_key_split(const struct TagStats *s, const char **db, const char **tenant,
			    const char **sec_key, const char **sec_value)
{
	*db = s->hkey;
	*tenant = *db + strlen(*db) + 1;
	*sec_key = *tenant + strlen(*tenant) + 1;
	*sec_value = *sec_key + strlen(*sec_key) + 1;
}

bool qtags_show_tag_stats(PgSocket *client)
{
	PktBuf *buf;
	struct List *item;

	buf = pktbuf_dynamic(512);
	if (!buf) {
		admin_error(client, "no mem");
		return true;
	}

	pktbuf_write_RowDescription(buf, "ssssNNNNNNNN",
				    "database", "tenant", "secondary_key", "secondary_value",
				    "query_count", "query_time", "wait_time",
				    "budget_wait_time", "db_time",
				    "xact_count", "xact_time", "last_seen");
	statlist_for_each(item, &tag_stats_lru) {
		struct TagStats *s = container_of(item, struct TagStats, lru);
		const char *db, *tenant, *sec_key, *sec_value;
		uint64_t db_time = s->query_time > s->wait_time ? s->query_time - s->wait_time : 0;

		stats_key_split(s, &db, &tenant, &sec_key, &sec_value);
		pktbuf_write_DataRow(buf, "ssssNNNNNNNN",
				     db, tenant,
				     sec_key[0] ? sec_key : "-",
				     sec_key[0] ? sec_value : "-",
				     s->query_count, s->query_time, s->wait_time,
				     s->budget_wait_time, db_time,
				     s->xact_count, s->xact_time, s->last_seen);
	}
	admin_flush(client, buf, "SHOW");
	return true;
}
