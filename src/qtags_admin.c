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
 * The control plane of the query tag budgets: the [tag_budgets] config
 * section, the TAG_BUDGET admin command and the SHOW TAG_BUDGETS
 * rendering. The buckets themselves live in src/qtags_budget.c, the
 * per tenant stats in src/qtags.c.
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

/* Lookup table for the tag_budget_mode config parameter and budgets */
const struct CfLookup tag_budget_mode_map[] = {
	{ "off", QTAG_MODE_OFF },
	{ "warn", QTAG_MODE_WARN },
	{ "enforce", QTAG_MODE_ENFORCE },
	{ NULL, 0 },
};

/* the numeric [tag_budgets] parameters and their upper bounds */
enum BudgetParam {
	PARAM_SHARE,
	PARAM_BURST,
	PARAM_PER_QUERY_MS,
	PARAM_CONCURRENCY,
};

static const struct BudgetParamDef {
	const char *key;
	int max;
} budget_param_defs[] = {
	[PARAM_SHARE]	       = { "share",        10000 },
	[PARAM_BURST]	       = { "burst",         3600 },
	[PARAM_PER_QUERY_MS]	   = { "per_query_ms", 3600000 },
	[PARAM_CONCURRENCY]    = { "concurrency", 1000000 },
};

static int find_budget_param(const char *key)
{
	int i;

	for (i = 0; i < (int)(sizeof(budget_param_defs) / sizeof(budget_param_defs[0])); i++) {
		if (strcmp(key, budget_param_defs[i].key) == 0)
			return i;
	}
	return -1;
}

/* store one numeric parameter; all limit fields are "0 = off" ints */
static bool apply_budget_param(struct TagBudget *newb, int param, const char *val)
{
	char *endp;
	int v = strtol(val, &endp, 10);

	if (endp == val || *endp || v < 0 || v > budget_param_defs[param].max)
		return false;

	switch (param) {
	case PARAM_SHARE:
		newb->share_pct = v;
		break;
	case PARAM_BURST:
		newb->burst_sec = v;
		break;
	case PARAM_PER_QUERY_MS:
		newb->per_query_ms = v;
		break;
	case PARAM_CONCURRENCY:
		newb->concurrency = v;
		break;
	}
	return true;
}

/* parse the mode= value; "inherit" resets to the global config */
static bool apply_budget_mode(struct TagBudget *newb, const char *val)
{
	const struct CfLookup *l;

	if (strcasecmp(val, "inherit") == 0) {
		newb->mode = -1;
		return true;
	}
	for (l = tag_budget_mode_map; l->name; l++) {
		if (strcasecmp(l->name, val) == 0) {
			newb->mode = l->value;
			return true;
		}
	}
	return false;
}

/*
 * Parse "share=25 burst=60 mode=enforce" style parameters for a tenant
 * from the [tag_budgets] config section. Parameters are validated before
 * any of them is applied.
 */
bool qtags_parse_tag_budget(void *base, const char *tenant, const char *params)
{
	struct TagBudget *b;
	struct TagBudget newb;
	char *dup, *tok, *saveptr = NULL;

	(void)base;	/* the budget store is global, like the other loaders */

	if (!tenant[0] || strlen(tenant) > QTAG_MAX_VALUE) {
		log_error("invalid tenant name in [tag_budgets]: %s", tenant);
		return false;
	}

	/* start from the current values, apply only what appears in params */
	b = qtags_find_budget(tenant);
	if (b) {
		newb = *b;
	} else {
		qtags_budget_defaults(&newb);
	}

	dup = strdup(params ? params : "");
	if (!dup)
		return false;

	for (tok = strtok_r(dup, " \t", &saveptr); tok; tok = strtok_r(NULL, " \t", &saveptr)) {
		char *eq = strchr(tok, '=');
		const char *key, *val;
		int param;

		if (!eq) {
			log_error("bad [tag_budgets] entry for %s: expected key=value, got '%s'",
				  tenant, tok);
			free(dup);
			return false;
		}
		*eq = 0;
		key = tok;
		val = eq + 1;

		if (strcmp(key, "mode") == 0) {
			if (!apply_budget_mode(&newb, val)) {
				log_error("bad mode '%s' for [tag_budgets] %s", val, tenant);
				free(dup);
				return false;
			}
			continue;
		}

		param = find_budget_param(key);
		if (param < 0) {
			log_error("unknown key '%s' in [tag_budgets] for %s", key, tenant);
			free(dup);
			return false;
		}
		if (!apply_budget_param(&newb, param, val)) {
			log_error("bad %s '%s' for [tag_budgets] %s", key, val, tenant);
			free(dup);
			return false;
		}
	}
	free(dup);

	if (!b) {
		b = qtags_budget_new(tenant);
		if (!b)
			return false;
	}
	b->share_pct = newb.share_pct;
	b->burst_sec = newb.burst_sec;
	b->per_query_ms = newb.per_query_ms;
	b->concurrency = newb.concurrency;
	b->mode = newb.mode;
	b->from_config = true;
	b->dead = false;
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
		if (b->from_config && b->dead)
			qtags_budget_destroy(b);
	}
	qtags_budget_wake_waiters();
}

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

		b = qtags_find_budget(tenant);
		was_config = b ? b->from_config : false;

		if (!qtags_parse_tag_budget(NULL, tenant, params))
			return admin_error(admin, "bad budget parameters");
		/*
		 * Admin SET is a runtime change. Budgets that also live in
		 * the [tag_budgets] section keep from_config, so a RELOAD
		 * reverts the tweak to the configured values. Budgets that
		 * exist only here are marked from_admin so the reload sweep
		 * does not drop them; they still live only until restart
		 * unless added to the config file.
		 */
		b = qtags_find_budget(tenant);
		if (b) {
			b->from_config = was_config;
			b->from_admin = true;
		}
		log_info("TAG_BUDGET SET %s %s", tenant, params);
		qtags_budget_wake_waiters();
		return admin_ready(admin, "TAG_BUDGET");
	}

	if (strcasecmp(action, "del") == 0) {
		b = qtags_find_budget(tenant);
		if (!b)
			return admin_error(admin, "no budget for tenant: %s", tenant);
		qtags_budget_destroy(b);
		qtags_budget_wake_waiters();
		log_info("TAG_BUDGET DEL %s", tenant);
		return admin_ready(admin, "TAG_BUDGET");
	}

	if (strcasecmp(action, "reset") == 0) {
		b = qtags_find_budget(tenant);
		if (!b)
			return admin_error(admin, "no budget for tenant: %s", tenant);
		b->debt = 0;
		b->last_update = get_cached_time();
		b->active = 0;
		qtags_budget_wake_waiters();
		log_info("TAG_BUDGET RESET %s", tenant);
		return admin_ready(admin, "TAG_BUDGET");
	}

	return admin_error(admin, "unknown TAG_BUDGET action: %s", action);
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

		qtags_budget_drain(b, get_cached_time());
		pktbuf_write_DataRow(buf, "sNNNNsNNNNNNN",
				     b->tenant, qtags_budget_share_pct(b), qtags_budget_burst_sec(b),
				     qtags_budget_per_query_ms(b), qtags_budget_concurrency(b),
				     tag_budget_mode_map[qtags_budget_mode(b)].name,
				     b->debt, b->ema_cost, b->queries, b->active,
				     b->queued_count, b->rejected_count, b->warned_count);
	}
	admin_flush(client, buf, "SHOW");
	return true;
}
