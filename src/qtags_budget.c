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
 * The reverse leaky bucket budget engine.
 *
 * Every finished query adds the database wall time it consumed to the
 * tenant's bucket, and the bucket drains at a configured rate (the
 * tenant's share). A query that would overflow the bucket is queued
 * inside PgBouncer until the bucket has drained enough, so the client
 * only experiences higher latency. Debt is drained lazily on read, so
 * there is no periodic drain task, and tenants that stay under their
 * share never wait.
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

struct TagBudget *tag_budget_map;

STATLIST(tag_budget_lru);

/* clients queued by the budget, waiting for their bucket to drain */
static STATLIST(tag_waiters);
static struct event qtag_timer;
static bool qtag_timer_armed;
static usec_t qtag_timer_due;

static void wake_client(PgSocket *client);
static void qtags_budget_wake_tenant(struct TagBudget *b);
static enum QTagDecision over_budget_decision(PgSocket *client, struct TagBudget *b, bool can_wait);

struct TagBudget *qtags_find_budget(const char *tenant)
{
	struct TagBudget *b;

	HASH_FIND_STR(tag_budget_map, tenant, b);
	return b;
}

/* resolve a budget parameter: per budget value, else the global config */
int qtags_budget_share_pct(struct TagBudget *b)
{
	return b->share_pct >= 0 ? b->share_pct : cf_tag_budget_share;
}

int qtags_budget_burst_sec(struct TagBudget *b)
{
	return b->burst_sec >= 0 ? b->burst_sec : cf_tag_budget_burst;
}

int qtags_budget_per_query_ms(struct TagBudget *b)
{
	return b->per_query_ms >= 0 ? b->per_query_ms : cf_tag_budget_per_query_ms;
}

int qtags_budget_concurrency(struct TagBudget *b)
{
	return b->concurrency >= 0 ? b->concurrency : cf_tag_budget_concurrency;
}

int qtags_budget_mode(struct TagBudget *b)
{
	return b->mode >= 0 ? b->mode : cf_tag_budget_mode;
}

void qtags_budget_drain(struct TagBudget *b, usec_t now)
{
	int share = qtags_budget_share_pct(b);

	if (b->debt > 0 && b->last_update > 0 && now > b->last_update && share > 0) {
		usec_t elapsed = now - b->last_update;
		usec_t drain = elapsed * (usec_t)share / 100;

		b->debt = b->debt > drain ? b->debt - drain : 0;
	}
	b->last_update = now;
}

static void tag_budget_lru_touch(struct TagBudget *b)
{
	statlist_remove(&tag_budget_lru, &b->lru);
	statlist_prepend(&tag_budget_lru, &b->lru);
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
 * Decide whether the batch starting on the client may run. Returns the
 * decision and, for QTAG_WAIT, how many microseconds the query has to
 * wait for its bucket to drain (in *wait_needed).
 */
enum QTagDecision qtags_budget_admit(PgSocket *client, usec_t *wait_needed)
{
	struct TagBudget *b;
	usec_t burst_us, per_query_us, cost;
	int share;

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

	b = qtags_find_budget(client->qtags.tenant);
	if (!b) {
		/* budgets apply globally when enforcement is configured */
		if (cf_tag_budget_mode == QTAG_MODE_OFF)
			return QTAG_ALLOW;
		b = create_budget(client->qtags.tenant);
		if (!b)
			return QTAG_ALLOW;
	}
	tag_budget_lru_touch(b);

	qtags_budget_drain(b, get_cached_time());

	cost = b->ema_cost;
	share = qtags_budget_share_pct(b);
	per_query_us = (usec_t)qtags_budget_per_query_ms(b) * 1000;
	burst_us = (usec_t)qtags_budget_burst_sec(b) * USEC;

	if (burst_us && cost > burst_us) {
		/* the query can never fit the bucket */
		return over_budget_decision(client, b, false);
	}

	if (per_query_us && cost > per_query_us) {
		return over_budget_decision(client, b, false);
	}

	if (burst_us && b->debt + cost > burst_us) {
		usec_t over = b->debt + cost - burst_us;

		if (share <= 0) {
			/* a share of 0 never drains */
			return over_budget_decision(client, b, false);
		}
		*wait_needed = over * 100 / (usec_t)share;
		return over_budget_decision(client, b, true);
	}

	if (qtags_budget_concurrency(b)
	    && b->active >= (uint64_t)qtags_budget_concurrency(b)) {
		*wait_needed = 0;	/* wait until a slot frees */
		return over_budget_decision(client, b, true);
	}

	/* within budget: count the batch and take a concurrency slot */
	b->queries++;
	b->active++;
	strlcpy(client->qtags.active_tenant, b->tenant, sizeof(client->qtags.active_tenant));
	return QTAG_ALLOW;
}

/*
 * Map an over-budget finding to the decision the configured mode asks
 * for. In enforce mode the query is queued when it can drain (can_wait)
 * and rejected when it never fits the bucket; in warn mode it always
 * runs with a warning.
 */
static enum QTagDecision over_budget_decision(PgSocket *client, struct TagBudget *b, bool can_wait)
{
	if (qtags_budget_mode(b) != QTAG_MODE_ENFORCE) {
		b->warned_count++;
		return QTAG_WARN;
	}
	if (can_wait) {
		/* count a queueing episode once, not every re-park */
		if (!client->qtag_wait_since)
			b->queued_count++;
		return QTAG_WAIT;
	}
	b->rejected_count++;
	return QTAG_REJECT;
}

static void budget_charge(struct TagBudget *b, usec_t cost)
{
	usec_t ema_input;

	b->debt += cost;

	/* update the per query cost estimate */
	ema_input = MIN(cost, MAX(QTAG_EMA_CAP, (usec_t)qtags_budget_burst_sec(b) * USEC));
	if (b->ema_cost == 0)
		b->ema_cost = ema_input;
	else if (ema_input >= b->ema_cost)
		b->ema_cost += (ema_input - b->ema_cost) / QTAG_EMA_ALPHA;
	else
		b->ema_cost -= (b->ema_cost - ema_input) / QTAG_EMA_ALPHA;
}

void qtags_budget_account_query(PgSocket *client, usec_t cost)
{
	struct TagBudget *b;

	if (!qtags_feature_enabled())
		return;

	if (client->qtags.active_tenant[0]) {
		/* the batch holds a slot: give it back and charge the cost */
		b = qtags_find_budget(client->qtags.active_tenant);
		if (b) {
			if (b->active > 0)
				b->active--;
			if (cost)
				budget_charge(b, cost);
			/* a freed slot may let a queued query in */
			qtags_budget_wake_tenant(b);
		}
		client->qtags.active_tenant[0] = 0;
	} else if (cost && client->qtags.tenant[0]) {
		/*
		 * No slot was taken because the budget did not exist at
		 * admission time. Charge the cost to a budget that has
		 * since been created, so late budgets still fill up.
		 */
		b = qtags_find_budget(client->qtags.tenant);
		if (b)
			budget_charge(b, cost);
	}
}

/* the client connection is going away: give back slot and waiter state */
void qtags_client_release(PgSocket *client)
{
	if (client->qtag_wait_deadline) {
		statlist_remove(&tag_waiters, &client->qtag_wait);
		client->qtag_wait_deadline = 0;
	}
	qtags_budget_account_query(client, 0);
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

	if (buffered && !client_resume_buffered_packet(client))
		return;		/* queued again or disconnected */

	if (client->state == CL_ACTIVE)
		sbuf_continue(&client->sbuf);
}

/* wake the single oldest waiter of a budget (called when a slot frees) */
static void qtags_budget_wake_tenant(struct TagBudget *b)
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

void qtags_budget_wake_waiters(void)
{
	struct StatList tmp;
	struct List *item;

	if (statlist_empty(&tag_waiters))
		return;

	/*
	 * Steal the waiter list so that clients parking again during the
	 * walk append to a fresh list.
	 */
	tmp = tag_waiters;
	tmp.head.next->prev = &tmp.head;
	tmp.head.prev->next = &tmp.head;
	statlist_init(&tag_waiters, "tag_waiters");

	while ((item = statlist_pop(&tmp)) != NULL) {
		PgSocket *client = container_of(item, PgSocket, qtag_wait);

		client->qtag_wait_deadline = 0;
		client->qtag_wait_buffered = false;
		sbuf_continue(&client->sbuf);
	}
	disarm_wait_timer();
}

/* has the client waited longer than tag_budget_wait_timeout allows? */
static bool wait_timed_out(PgSocket *client, usec_t now)
{
	return cf_tag_budget_wait_timeout > 0
	       && client->qtag_wait_since
	       && now - client->qtag_wait_since >= cf_tag_budget_wait_timeout;
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

		/*
		 * The wait is over: disconnect clients that waited past
		 * tag_budget_wait_timeout, wake the rest. A woken client
		 * may re-park (with a fresh deadline) or disconnect.
		 */
		if (wait_timed_out(due, now))
			timeout_waiter(due);
		else
			wake_client(due);
	}
}

enum QTagPacketAction qtags_budget_park(PgSocket *client, usec_t wait_us)
{
	usec_t now = get_cached_time();
	usec_t deadline = now + wait_us;

	if (deadline < now + QTAG_MIN_WAIT_USEC)
		deadline = now + QTAG_MIN_WAIT_USEC;

	if (cf_tag_budget_wait_timeout > 0) {
		/*
		 * The deadline never moves past the end of the client's
		 * total wait; the timer disconnects it when it fires.
		 */
		usec_t max_deadline = client->qtag_wait_since + cf_tag_budget_wait_timeout;

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
