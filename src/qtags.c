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
 * Query tag parsing, per tenant stats and reverse leaky bucket budgets.
 *
 * A "reverse leaky bucket" models a budget as a bucket of debt: every
 * query that runs adds the database wall time it consumed to the bucket,
 * and the bucket drains at a configured rate (the tenant's share). A
 * query that would overflow the bucket is queued inside PgBouncer until
 * the bucket has drained enough, so the client only experiences higher
 * latency. Debt is drained lazily on read, so there is no periodic drain
 * task, and tenants that stay under their share never wait.
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

/* cap for the EMA of query cost, so one huge query does not poison it */
#define QTAG_EMA_CAP USEC	/* 1 second */
#define QTAG_EMA_ALPHA 8	/* EMA weight of a single new sample */

/*
 * Minimum queueing interval. Waits that resolve by event (a concurrency
 * slot freeing) are woken immediately by that event; this interval only
 * keeps the timer from spinning when the wait duration is zero or when
 * an event was missed.
 */
#define QTAG_MIN_WAIT_USEC (10 * 1000)

/*
 * Aggregated stats for one (database, tenant, secondary tag) combination.
 * Times are cumulative microseconds, same units as SHOW STATS.
 */
struct TagStats {
	UT_hash_handle hh;
	struct List lru;		/* in tag_stats_lru, most recent first */

	/*
	 * key: db, tenant, sec_key, sec_value NUL-joined into one packed
	 * byte string. The key must live in a packed buffer, because uthash
	 * compares the raw bytes at the stored key pointer.
	 */
	char hkey[(MAX_DBNAME + 1) + (QTAG_MAX_VALUE + 1) + (QTAG_MAX_KEY + 1) + (QTAG_MAX_VALUE + 1)];
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

/*
 * A leaky bucket budget for one tenant tag value. Configured limits use
 * -1 for "inherit from the global config", so that per budget overrides
 * can be distinguished from an explicit 0 (which disables a limit).
 */
struct TagBudget {
	UT_hash_handle hh;
	struct List lru;		/* in tag_budget_lru, most recent first */

	char tenant[QTAG_MAX_VALUE + 1];

	/* limits, -1 = inherit from global config */
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

static struct TagStats *tag_stats_map;
static struct TagBudget *tag_budget_map;

/*
 * Statically initialized lists, so that config parsing (which can touch
 * them before qtags_init runs) cannot hit uninitialized state.
 */
static STATLIST(tag_stats_lru);
static STATLIST(tag_budget_lru);

/* clients queued by the budget, waiting for their bucket to drain */
static STATLIST(tag_waiters);
static struct event qtag_timer;
static bool qtag_timer_armed;
static usec_t qtag_timer_due;

static void wake_client(PgSocket *client);
static void qtags_wake_waiters_for_budget(struct TagBudget *b);

/* parsed secondary keys from cf_query_tag_keys */
static char secondary_keys[QTAG_MAX_SECONDARY_KEYS][QTAG_MAX_KEY + 1];
static int secondary_key_count;

const struct CfLookup tag_budget_mode_map[] = {
	{ "off", QTAG_MODE_OFF },
	{ "warn", QTAG_MODE_WARN },
	{ "enforce", QTAG_MODE_ENFORCE },
	{ NULL, 0 },
};

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

/*
 * Try to parse the body of a C-style block comment (between the markers)
 * as a tag comment. Returns true when the comment consisted solely of
 * key=value or key:value pairs (SQLCommenter / marginalia style), so
 * prose comments never get mistaken for tags.
 */
static bool parse_tag_comment(const char *p, const char *end, struct QTagSet *tags)
{
	if (p < end && *p == '+')	/* planner hint comment, not tags */
		return false;

	while (p < end) {
		char key[QTAG_MAX_KEY + 1];
		char val[QTAG_MAX_VALUE + 1];
		char sep;
		size_t len = 0;

		/* skip separators */
		while (p < end && (*p == ',' || isspace((unsigned char)*p)))
			p++;
		if (p == end)
			return true;

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

		record_tag(key, MIN((int)len, QTAG_MAX_KEY), val, strlen(val), tags);
	}
	return true;
}

void qtags_clear(PgSocket *client)
{
	char active_tenant[QTAG_MAX_VALUE + 1];

	/*
	 * active_tenant survives: the concurrency slot is held until the
	 * batch completes, even when a re-delivered packet re-parses tags
	 * on the way (e.g. the varcache pause/resume on a fresh server).
	 */
	memcpy(active_tenant, client->qtags.active_tenant, sizeof(active_tenant));
	memset(&client->qtags, 0, sizeof(client->qtags));
	memcpy(client->qtags.active_tenant, active_tenant, sizeof(active_tenant));
}

/*
 * Parse tags out of a query text into client->qtags. Tags of a previous
 * query are cleared first. Unknown keys are ignored, so high cardinality
 * tags like trace_id create no state.
 */
void qtags_parse(PgSocket *client, const char *query)
{
	const char *p = query;

	qtags_clear(client);
	if (!qtags_feature_enabled() || !query)
		return;

	while ((p = strchr(p, '/')) != NULL) {
		const char *inner, *end;

		if (p[1] != '*') {
			p++;
			continue;
		}
		inner = p + 2;
		end = strstr(inner, "*/");
		if (!end)
			break;	/* unterminated comment, no more tags */
		parse_tag_comment(inner, end, &client->qtags);
		p = end + 2;
	}
}

/* ------------------------------- budgets --------------------------------- */

static void tag_budget_lru_touch(struct TagBudget *b)
{
	statlist_remove(&tag_budget_lru, &b->lru);
	statlist_prepend(&tag_budget_lru, &b->lru);
}

static void tag_stats_lru_touch(struct TagStats *s)
{
	statlist_remove(&tag_stats_lru, &s->lru);
	statlist_prepend(&tag_stats_lru, &s->lru);
}

/* last element of a statlist, for LRU eviction: see statlist_last() */

static struct TagBudget *find_budget(const char *tenant)
{
	struct TagBudget *b;

	HASH_FIND_STR(tag_budget_map, tenant, b);
	return b;
}

/* resolve a budget parameter: per budget value, else the global config */
static int budget_share_pct(struct TagBudget *b)
{
	return b->share_pct >= 0 ? b->share_pct : cf_tag_budget_share;
}

static int budget_burst_sec(struct TagBudget *b)
{
	return b->burst_sec >= 0 ? b->burst_sec : cf_tag_budget_burst;
}

static int budget_per_query_ms(struct TagBudget *b)
{
	return b->per_query_ms >= 0 ? b->per_query_ms : cf_tag_budget_per_query_ms;
}

static int budget_concurrency(struct TagBudget *b)
{
	return b->concurrency >= 0 ? b->concurrency : cf_tag_budget_concurrency;
}

static int budget_mode(struct TagBudget *b)
{
	return b->mode >= 0 ? b->mode : cf_tag_budget_mode;
}

/* lazily drain the bucket: share_pct % of one DB-second per wall second */
static void budget_drain(struct TagBudget *b, usec_t now)
{
	int share = budget_share_pct(b);

	if (b->debt > 0 && b->last_update > 0 && now > b->last_update && share > 0) {
		usec_t elapsed = now - b->last_update;
		usec_t drain = elapsed * (usec_t)share / 100;

		b->debt = b->debt > drain ? b->debt - drain : 0;
	}
	b->last_update = now;
}

/*
 * Create an empty runtime bucket for a tenant, evicting the least
 * recently used one when over the entry cap. Returns NULL on OOM.
 */
static struct TagBudget *create_budget(const char *tenant)
{
	struct TagBudget *b;

	if (cf_tag_budget_max_entries > 0
	    && HASH_COUNT(tag_budget_map) >= (unsigned)cf_tag_budget_max_entries) {
		struct List *item = statlist_last(&tag_budget_lru);

		if (item) {
			b = container_of(item, struct TagBudget, lru);
			HASH_DELETE(hh, tag_budget_map, b);
			statlist_remove(&tag_budget_lru, &b->lru);
			free(b);
		}
	}

	b = calloc(1, sizeof(*b));
	if (!b)
		return NULL;
	strlcpy(b->tenant, tenant, sizeof(b->tenant));
	b->share_pct = -1;
	b->burst_sec = -1;
	b->per_query_ms = -1;
	b->concurrency = -1;
	b->mode = -1;
	HASH_ADD_STR(tag_budget_map, tenant, b);
	statlist_prepend(&tag_budget_lru, &b->lru);
	return b;
}

/*
 * Decide whether a query carrying client->qtags may run. Returns the
 * decision and, for QTAG_WAIT, how many microseconds the query has to
 * wait for its bucket to drain (in *wait_needed).
 */
static enum QTagDecision qtags_admit(PgSocket *client, usec_t *wait_needed)
{
	struct TagBudget *b;
	usec_t now, burst_us, per_query_us, cost;
	int share, mode;

	*wait_needed = 0;

	if (client->query_start != 0) {
		/*
		 * Mid-batch query: pipelined packet, or the re-delivery of
		 * the same packet after the varcache pause. The batch
		 * already passed admission and holds its slot; re-checking
		 * would deadlock against the batch's own slot.
		 */
		return QTAG_ALLOW;
	}

	if (!qtags_feature_enabled() || !client->qtags.tenant[0])
		return QTAG_ALLOW;

	b = find_budget(client->qtags.tenant);
	if (!b) {
		/* budgets apply globally when enforcement is configured */
		if (cf_tag_budget_mode == QTAG_MODE_OFF)
			return QTAG_ALLOW;
		b = create_budget(client->qtags.tenant);
		if (!b)
			return QTAG_ALLOW;
	}
	tag_budget_lru_touch(b);

	now = get_cached_time();
	budget_drain(b, now);

	cost = b->ema_cost;
	share = budget_share_pct(b);
	mode = budget_mode(b);
	per_query_us = (usec_t)budget_per_query_ms(b) * 1000;
	burst_us = (usec_t)budget_burst_sec(b) * USEC;

	if (burst_us && cost > burst_us) {
		/* the query can never fit the bucket */
		return (mode == QTAG_MODE_ENFORCE) ? QTAG_REJECT : QTAG_WARN;
	}

	if (per_query_us && cost > per_query_us) {
		return (mode == QTAG_MODE_ENFORCE) ? QTAG_REJECT : QTAG_WARN;
	}

	if (burst_us && b->debt + cost > burst_us) {
		usec_t over = b->debt + cost - burst_us;

		if (share <= 0) {
			/* a share of 0 never drains */
			return (mode == QTAG_MODE_ENFORCE) ? QTAG_REJECT : QTAG_WARN;
		}
		*wait_needed = over * 100 / (usec_t)share;
		return (mode == QTAG_MODE_ENFORCE) ? QTAG_WAIT : QTAG_WARN;
	}

	{
		int conc = budget_concurrency(b);

		if (conc && b->active >= (uint64_t)conc) {
			*wait_needed = 0;	/* wait until a slot frees */
			return (mode == QTAG_MODE_ENFORCE) ? QTAG_WAIT : QTAG_WARN;
		}
	}

	/* within budget */
	if (client->query_start == 0) {
		/*
		 * First query of a batch: count it and take a concurrency
		 * slot. Re-delivered packets (varcache pause) see
		 * query_start != 0 and skip this.
		 */
		b->queries++;
		b->active++;
		strlcpy(client->qtags.active_tenant, b->tenant, sizeof(client->qtags.active_tenant));
	}
	return QTAG_ALLOW;
}

static void budget_charge(struct TagBudget *b, usec_t cost)
{
	usec_t ema_input;

	b->debt += cost;

	/* update the per query cost estimate */
	ema_input = MIN(cost, MAX(QTAG_EMA_CAP, (usec_t)budget_burst_sec(b) * USEC));
	if (b->ema_cost == 0)
		b->ema_cost = ema_input;
	else if (ema_input >= b->ema_cost)
		b->ema_cost += (ema_input - b->ema_cost) / QTAG_EMA_ALPHA;
	else
		b->ema_cost -= (b->ema_cost - ema_input) / QTAG_EMA_ALPHA;
}

/* charge the batch's cost to the budget that was counted as active */
static void budget_release_active(PgSocket *client, bool charge, usec_t cost)
{
	struct TagBudget *b;

	if (!client->qtags.active_tenant[0])
		return;

	b = find_budget(client->qtags.active_tenant);
	if (b) {
		if (b->active > 0)
			b->active--;
		if (charge)
			budget_charge(b, cost);
		/* a freed slot may let a queued query in */
		qtags_wake_waiters_for_budget(b);
	}
	client->qtags.active_tenant[0] = 0;
}

/* the client connection is going away: give back the concurrency slot */
void qtags_client_release(PgSocket *client)
{
	if (client->qtag_wait_deadline) {
		statlist_remove(&tag_waiters, &client->qtag_wait);
		client->qtag_wait_deadline = 0;
	}
	budget_release_active(client, false, 0);
}

/* ------------------------- wait queue machinery -------------------------- */

static void qtag_timer_cb(evutil_socket_t s, short flags, void *arg);

/* arm (or re-arm) the wake timer to the earliest deadline */
static void arm_wait_timer(usec_t deadline)
{
	struct timeval tv;
	usec_t now = get_cached_time();
	usec_t delay;

	if (qtag_timer_armed && qtag_timer_due <= deadline)
		return;		/* already firing early enough */

	if (qtag_timer_armed)
		event_del(&qtag_timer);
	event_assign(&qtag_timer, pgb_event_base, -1, 0, qtag_timer_cb, NULL);

	delay = deadline > now ? deadline - now : 0;
	tv.tv_sec = delay / USEC;
	tv.tv_usec = delay % USEC;
	if (event_add(&qtag_timer, &tv) < 0) {
		log_warning("qtags: event_add failed: %s", strerror(errno));
		return;
	}
	qtag_timer_due = deadline;
	qtag_timer_armed = true;
}

static void disarm_wait_timer(void)
{
	if (qtag_timer_armed) {
		event_del(&qtag_timer);
		qtag_timer_armed = false;
	}
}

/*
 * Resume a client whose packet is still pending in the sbuf read buffer.
 * The packet is re-parsed from scratch, so the whole admission check
 * runs again.
 */
static void wake_client(PgSocket *client)
{
	bool buffered = client->qtag_wait_buffered;

	client->qtag_wait_deadline = 0;
	client->qtag_wait_buffered = false;
	statlist_remove(&tag_waiters, &client->qtag_wait);

	if (!buffered) {
		sbuf_continue(&client->sbuf);
		return;
	}

	/*
	 * The packet was fully buffered by the fetch machinery (big packet),
	 * so the sbuf read position is already past it. Re-dispatch the
	 * buffered packet directly, mirroring the epilogue of
	 * process_pkt_callback(), and only then resume the stream.
	 */
	{
		struct CallbackState *cb = &client->packet_cb_state;
		bool res = client_handle_complete_packet(client, &cb->pkt);

		if (res) {
			cb->flag = CB_NONE;
			free_header(&cb->pkt);
			/*
			 * Do not touch the sbuf when the packet handler
			 * parked the client again (waiting for a server).
			 */
			if (client->state == CL_ACTIVE)
				sbuf_continue(&client->sbuf);
		}
	}
}

/* wake the single oldest waiter of a budget (called when a slot frees) */
static void qtags_wake_waiters_for_budget(struct TagBudget *b)
{
	struct List *item;

	statlist_for_each(item, &tag_waiters) {
		PgSocket *client = container_of(item, PgSocket, qtag_wait);

		if (strcmp(client->qtags.tenant, b->tenant) == 0) {
			wake_client(client);
			return;
		}
	}
}

/* wake every queued client; used when budgets change */
static void wake_all_waiters(void)
{
	struct StatList tmp;
	struct List *item, *next;

	if (statlist_empty(&tag_waiters))
		return;

	/* steal the waiter list so re-parks go to a fresh list */
	tmp.head = tag_waiters.head;
	tmp.cur_count = tag_waiters.cur_count;
	tmp.head.next->prev = &tmp.head;
	tmp.head.prev->next = &tmp.head;
	statlist_init(&tag_waiters, "tag_waiters");

	item = tmp.head.next;
	while (item != &tmp.head) {
		PgSocket *client = container_of(item, PgSocket, qtag_wait);

		next = item->next;
		client->qtag_wait_deadline = 0;
		client->qtag_wait_buffered = false;
		sbuf_continue(&client->sbuf);
		item = next;
	}
	disarm_wait_timer();
}

/* timer callback: wake every client whose wait is over, one at a time */
static void qtag_timer_cb(evutil_socket_t s, short flags, void *arg)
{
	usec_t now = get_cached_time();

	/* the one-shot event has fired; it is no longer pending */
	qtag_timer_armed = false;

	while (1) {
		struct List *item;
		PgSocket *due = NULL;
		usec_t earliest = 0;

		/* find the waiter with the earliest deadline */
		statlist_for_each(item, &tag_waiters) {
			PgSocket *client = container_of(item, PgSocket, qtag_wait);

			if (!due || client->qtag_wait_deadline < earliest) {
				due = client;
				earliest = client->qtag_wait_deadline;
			}
		}

		if (!due) {
			disarm_wait_timer();
			return;
		}
		if (earliest > now) {
			arm_wait_timer(earliest);
			return;
		}

		/* wake exactly one client; it may re-park or disconnect */
		wake_client(due);
	}
}

/*
 * Park a client until its budget has room. The current packet stays
 * unconsumed in the sbuf, so it is re-delivered when the client is
 * woken. Returns QTAG_PKT_PARKED on success ("do not touch the sbuf").
 */
static enum QTagPacketAction park_client(PgSocket *client, usec_t wait_us)
{
	usec_t now = get_cached_time();
	usec_t deadline = now + wait_us;

	if (deadline < now + QTAG_MIN_WAIT_USEC)
		deadline = now + QTAG_MIN_WAIT_USEC;

	if (cf_tag_budget_wait_timeout > 0) {
		usec_t max_deadline = now + cf_tag_budget_wait_timeout;

		if (deadline > max_deadline)
			deadline = max_deadline;
	}

	client->qtag_wait_buffered =
		(client->packet_cb_state.flag == CB_HANDLE_COMPLETE_PACKET);
	if (!client->qtag_wait_since)
		client->qtag_wait_since = now;
	statlist_append(&tag_waiters, &client->qtag_wait);
	client->qtag_wait_deadline = deadline;

	if (!sbuf_pause(&client->sbuf)) {
		statlist_remove(&tag_waiters, &client->qtag_wait);
		client->qtag_wait_deadline = 0;
		client->qtag_wait_buffered = false;
		disconnect_client(client, true, "query tag budget pause failed");
		return QTAG_PKT_CONSUMED;
	}

	arm_wait_timer(deadline);
	return QTAG_PKT_PARKED;
}

/* the client waited too long for its budget */
static void timeout_waiter(PgSocket *client)
{
	char msg[128];

	snprintf(msg, sizeof(msg),
		 "query waited %d s for the '%s' tag budget, aborting",
		 (int)(cf_tag_budget_wait_timeout / USEC), client->qtags.tenant);
	log_warning("disconnecting client: %s", msg);
	client->qtag_wait_deadline = 0;
	client->qtag_wait_buffered = false;
	statlist_remove(&tag_waiters, &client->qtag_wait);
	disconnect_client(client, true, "%s", msg);
}

/* --------------------------- packet inspection --------------------------- */

/* the packet types whose text we parse tags from */
bool qtags_inspect_needed(int pkt_type)
{
	if (!qtags_feature_enabled())
		return false;
	return pkt_type == PqMsg_Query || pkt_type == PqMsg_Parse;
}

/*
 * Run the budget check for the query currently parsed on the client and
 * act on it. CONTINUE means normal packet processing, CONSUMED means the
 * packet was rejected and must not be forwarded, PARKED means the client
 * was queued and the packet stays pending in the sbuf for re-delivery
 * when the client is woken.
 */
static enum QTagPacketAction admit_or_wait(PgSocket *client, PktHdr *pkt)
{
	enum QTagDecision decision;
	usec_t wait_needed = 0;
	bool res;

	(void)res;	/* SEND_generic result; errors surface via disconnect */

	decision = qtags_admit(client, &wait_needed);

	switch (decision) {
	case QTAG_ALLOW:
		if (client->qtag_wait_since) {
			/* waited for the budget: account the queueing time */
			usec_t waited = get_cached_time() - client->qtag_wait_since;

			client->qtag_wait_since = 0;
			qtags_account_budget_wait(client, waited);
		}
		return QTAG_PKT_CONTINUE;

	case QTAG_WARN: {
		char msg[160];

		snprintf(msg, sizeof(msg),
			 "query tag budget reached for tenant '%s', running anyway",
			 client->qtags.tenant);
		SEND_generic(res, client, PqMsg_NoticeResponse, "cscscsc",
			     'S', "WARNING", 'C', "C53000", 'M', msg, 0);
		return QTAG_PKT_CONTINUE;
	}

	case QTAG_REJECT: {
		char msg[160];

		if (client->qtag_wait_since) {
			client->qtag_wait_since = 0;
		}
		snprintf(msg, sizeof(msg),
			 "query rejected: tag budget for tenant '%s' does not allow "
			 "this query", client->qtags.tenant);
		slog_warning(client, "rejecting query: %s", msg);
		SEND_generic(res, client, PqMsg_ErrorResponse, "cscscsc",
			     'S', "ERROR", 'C', "C53000", 'M', msg, 0);

		/*
		 * A simple protocol query outside a transaction owns the
		 * protocol state until the next ReadyForQuery, so the
		 * exchange can be completed here. In all other cases the
		 * transaction state of the linked server decides when
		 * ReadyForQuery comes, only the error is injected.
		 */
		if (pkt->type == PqMsg_Query && !client->xact_start && !client->link) {
			SEND_ReadyForQuery(res, client);
		}

		/* consume the packet, it must not reach the server */
		skip_possibly_completely_buffered_packet(client, pkt);
		return QTAG_PKT_CONSUMED;
	}

	case QTAG_WAIT:
		/* count the queueing event and enforce the wait timeout */
		if (client->qtag_wait_since == 0) {
			struct TagBudget *b = find_budget(client->qtags.tenant);

			if (b)
				b->queued_count++;
		}
		if (client->qtag_wait_since
		    && cf_tag_budget_wait_timeout > 0
		    && get_cached_time() - client->qtag_wait_since
		       >= cf_tag_budget_wait_timeout) {
			timeout_waiter(client);
			return false;
		}
		/*
		 * Park the client. Fully buffered packets (queries larger
		 * than pkt_buf) are re-dispatched directly on wake instead.
		 */
		return park_client(client, wait_needed);
	}
	return QTAG_PKT_CONTINUE;
}

enum QTagPacketAction qtags_handle_query_packet(PgSocket *client, PktHdr *pkt)
{
	const char *query;
	bool ok;

	if (!qtags_feature_enabled() || client->pool->db->admin)
		return QTAG_PKT_CONTINUE;

	ok = mbuf_get_string(&pkt->data, &query);
	pkt_rewind_v3(pkt);
	if (!ok)
		return QTAG_PKT_CONTINUE;

	qtags_parse(client, query);
	return admit_or_wait(client, pkt);
}

enum QTagPacketAction qtags_handle_parse_packet(PgSocket *client, PktHdr *pkt)
{
	const char *name, *query;
	bool ok;

	if (!qtags_feature_enabled() || client->pool->db->admin)
		return QTAG_PKT_CONTINUE;

	ok = mbuf_get_string(&pkt->data, &name);
	if (ok)
		ok = mbuf_get_string(&pkt->data, &query);
	pkt_rewind_v3(pkt);
	if (!ok)
		return QTAG_PKT_CONTINUE;

	qtags_parse(client, query);
	return admit_or_wait(client, pkt);
}

enum QTagPacketAction qtags_handle_bind_packet(PgSocket *client, PktHdr *pkt)
{
	if (!qtags_feature_enabled() || client->pool->db->admin)
		return QTAG_PKT_CONTINUE;

	/*
	 * For named statements the tags have to come from the stored query
	 * text, because the Parse packet only flies by once. Unnamed
	 * statements keep the tags of the Parse packet that is still in
	 * flight on this client.
	 */
	if (is_prepared_statements_enabled(client) && !incomplete_pkt(pkt)) {
		const char *portal, *stmt, *query;
		bool ok;

		ok = mbuf_get_string(&pkt->data, &portal);
		if (ok)
			ok = mbuf_get_string(&pkt->data, &stmt);
		pkt_rewind_v3(pkt);
		if (ok && stmt[0]) {
			query = get_prepared_statement_query(client, stmt);
			if (query)
				qtags_parse(client, query);
		}
	}
	return admit_or_wait(client, pkt);
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

static struct TagStats *get_stats_entry(PgSocket *client)
{
	struct TagStats *s;
	char key[(MAX_DBNAME + 1) + (QTAG_MAX_VALUE + 1) + (QTAG_MAX_KEY + 1) + (QTAG_MAX_VALUE + 1)];
	size_t len;

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

	if (client->qtags.active_tenant[0]) {
		budget_release_active(client, true, total);
	} else if (client->qtags.tenant[0]) {
		struct TagBudget *b = find_budget(client->qtags.tenant);

		if (b)
			budget_charge(b, total);
	}
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

void qtags_account_budget_wait(PgSocket *client, usec_t waited)
{
	struct TagStats *s = get_stats_entry(client);

	if (s)
		s->budget_wait_time += waited;
}

/* ------------------------- config: [tag_budgets] ------------------------- */

/*
 * Parse "share=25 burst=60 mode=enforce" style parameters for a tenant
 * from the [tag_budgets] config section. Parameters are validated before
 * any of them is applied.
 */
bool qtags_budget_from_config(const char *tenant, const char *params)
{
	struct TagBudget *b;
	struct TagBudget newb;
	char *dup, *tok, *saveptr = NULL;

	if (!tenant[0] || strlen(tenant) > QTAG_MAX_VALUE) {
		log_error("invalid tenant name in [tag_budgets]: %s", tenant);
		return false;
	}

	/* start from the current values, apply only what appears in params */
	b = find_budget(tenant);
	if (b) {
		newb = *b;
	} else {
		memset(&newb, 0, sizeof(newb));
		newb.share_pct = -1;
		newb.burst_sec = -1;
		newb.per_query_ms = -1;
		newb.concurrency = -1;
		newb.mode = -1;
	}

	dup = strdup(params ? params : "");
	if (!dup)
		return false;

	for (tok = strtok_r(dup, " \t", &saveptr); tok; tok = strtok_r(NULL, " \t", &saveptr)) {
		char *eq = strchr(tok, '=');
		const char *key, *val;
		char *endp;

		if (!eq) {
			log_error("bad [tag_budgets] entry for %s: expected key=value, got '%s'",
				  tenant, tok);
			free(dup);
			return false;
		}
		*eq = 0;
		key = tok;
		val = eq + 1;

		if (strcmp(key, "share") == 0) {
			int v = strtol(val, &endp, 10);

			if (endp == val || *endp || v < 0 || v > 10000) {
				log_error("bad share '%s' for [tag_budgets] %s", val, tenant);
				free(dup);
				return false;
			}
			newb.share_pct = v;
		} else if (strcmp(key, "burst") == 0) {
			int v = strtol(val, &endp, 10);

			if (endp == val || *endp || v < 0 || v > 3600) {
				log_error("bad burst '%s' for [tag_budgets] %s", val, tenant);
				free(dup);
				return false;
			}
			newb.burst_sec = v;
		} else if (strcmp(key, "per_query_ms") == 0) {
			int v = strtol(val, &endp, 10);

			if (endp == val || *endp || v < 0 || v > 3600000) {
				log_error("bad per_query_ms '%s' for [tag_budgets] %s", val, tenant);
				free(dup);
				return false;
			}
			newb.per_query_ms = v;
		} else if (strcmp(key, "concurrency") == 0) {
			int v = strtol(val, &endp, 10);

			if (endp == val || *endp || v < 0 || v > 1000000) {
				log_error("bad concurrency '%s' for [tag_budgets] %s", val, tenant);
				free(dup);
				return false;
			}
			newb.concurrency = v;
		} else if (strcmp(key, "mode") == 0) {
			if (strcasecmp(val, "inherit") == 0) {
				newb.mode = -1;
				continue;
			}
			for (const struct CfLookup *l = tag_budget_mode_map; l->name; l++) {
				if (strcasecmp(l->name, val) == 0) {
					newb.mode = l->value;
					break;
				}
			}
			if (newb.mode < 0) {
				log_error("bad mode '%s' for [tag_budgets] %s", val, tenant);
				free(dup);
				return false;
			}
		} else {
			log_error("unknown key '%s' in [tag_budgets] for %s", key, tenant);
			free(dup);
			return false;
		}
	}
	free(dup);

	if (!b) {
		b = calloc(1, sizeof(*b));
		if (!b)
			return false;
		strlcpy(b->tenant, tenant, sizeof(b->tenant));
		HASH_ADD_STR(tag_budget_map, tenant, b);
		statlist_prepend(&tag_budget_lru, &b->lru);
	}
	b->share_pct = newb.share_pct;
	b->burst_sec = newb.burst_sec;
	b->per_query_ms = newb.per_query_ms;
	b->concurrency = newb.concurrency;
	b->mode = newb.mode;
	b->from_config = true;
	b->dead = false;

	/* limits may have changed: let queued clients re-check */
	wake_all_waiters();
	return true;
}

/* mark all config budgets dead at the start of a config reload */
void qtags_config_dead(bool flag)
{
	struct TagBudget *b, *tmp;

	HASH_ITER (hh, tag_budget_map, b, tmp) {
		if (b->from_config)
			b->dead = flag;
	}
}

/* drop config budgets that disappeared from the config file */
void qtags_config_sweep(void)
{
	struct TagBudget *b, *tmp;

	HASH_ITER (hh, tag_budget_map, b, tmp) {
		if (b->from_config && b->dead) {
			HASH_DELETE(hh, tag_budget_map, b);
			statlist_remove(&tag_budget_lru, &b->lru);
			free(b);
		}
	}
	wake_all_waiters();
}

/* ---------------------------- admin interface ---------------------------- */

/*
 * TAG_BUDGET SET <tenant> [share=N] [burst=S] [mode=M] [per_query_ms=N] [concurrency=N]
 * TAG_BUDGET DEL <tenant>
 * TAG_BUDGET RESET <tenant>
 */
bool qtags_admin_budget_cmd(PgSocket *admin, const char *arg)
{
	char action[16];
	char tenant[QTAG_MAX_VALUE + 1];
	char params[192];
	struct TagBudget *b;
	int n;

	if (!admin->admin_user)
		return admin_error(admin, "admin access needed");

	params[0] = 0;
	n = sscanf(arg, "%15s %64s %191[^\n]", action, tenant, params);
	if (n < 2)
		return admin_error(admin, "usage: TAG_BUDGET SET <tenant> [share=N] [burst=S] "
					   "[mode=off|warn|enforce] [per_query_ms=N] [concurrency=N] | "
					   "TAG_BUDGET DEL <tenant> | TAG_BUDGET RESET <tenant>");

	if (strcasecmp(action, "set") == 0) {
		bool was_config;

		b = find_budget(tenant);
		was_config = b ? b->from_config : false;

		if (!qtags_budget_from_config(tenant, params))
			return admin_error(admin, "bad budget parameters");
		/*
		 * Admin SET is a runtime change. Budgets that also live in
		 * the [tag_budgets] section keep from_config, so a RELOAD
		 * reverts the tweak to the configured values. Budgets that
		 * exist only here are marked from_admin so the reload sweep
		 * does not drop them; they still live only until restart
		 * unless added to the config file.
		 */
		b = find_budget(tenant);
		if (b) {
			b->from_config = was_config;
			b->from_admin = true;
		}
		log_info("TAG_BUDGET SET %s %s", tenant, params);
		return admin_ready(admin, "TAG_BUDGET");
	}

	if (strcasecmp(action, "del") == 0) {
		b = find_budget(tenant);
		if (!b)
			return admin_error(admin, "no budget for tenant: %s", tenant);
		HASH_DELETE(hh, tag_budget_map, b);
		statlist_remove(&tag_budget_lru, &b->lru);
		free(b);
		wake_all_waiters();
		log_info("TAG_BUDGET DEL %s", tenant);
		return admin_ready(admin, "TAG_BUDGET");
	}

	if (strcasecmp(action, "reset") == 0) {
		b = find_budget(tenant);
		if (!b)
			return admin_error(admin, "no budget for tenant: %s", tenant);
		b->debt = 0;
		b->last_update = get_cached_time();
		b->active = 0;
		wake_all_waiters();
		log_info("TAG_BUDGET RESET %s", tenant);
		return admin_ready(admin, "TAG_BUDGET");
	}

	return admin_error(admin, "unknown TAG_BUDGET action: %s", action);
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

bool qtags_show_tag_budgets(PgSocket *client)
{
	PktBuf *buf;
	struct List *item;

	buf = pktbuf_dynamic(512);
	if (!buf) {
		admin_error(client, "no mem");
		return true;
	}

	pktbuf_write_RowDescription(buf, "sNNNNsNNNNNNN",
				    "tenant", "share", "burst", "per_query_ms", "concurrency",
				    "mode", "debt", "avg_cost", "queries", "active",
				    "queued", "rejected", "warned");
	statlist_for_each(item, &tag_budget_lru) {
		struct TagBudget *b = container_of(item, struct TagBudget, lru);

		budget_drain(b, get_cached_time());
		pktbuf_write_DataRow(buf, "sNNNNsNNNNNNN",
				     b->tenant, budget_share_pct(b), budget_burst_sec(b),
				     budget_per_query_ms(b), budget_concurrency(b),
				     tag_budget_mode_map[budget_mode(b)].name,
				     b->debt, b->ema_cost, b->queries, b->active,
				     b->queued_count, b->rejected_count, b->warned_count);
	}
	admin_flush(client, buf, "SHOW");
	return true;
}
