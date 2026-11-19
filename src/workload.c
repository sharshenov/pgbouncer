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

#include "bouncer.h"
#include "workload_tags.h"

#include <usual/hashtab-impl.h>

static bool uthash_alloc_failed;
#undef uthash_nonfatal_oom
#define uthash_nonfatal_oom(elt) uthash_alloc_failed = true

/*
 * Observation only: never owns SQL scheduling, protocol errors, or budget
 * policy. Existing request objects own all live execution accounting.
 */
struct WorkloadIdentity {
	struct WorkloadTags tags;
	enum WorkloadTagStatus status;
	uint64_t statement_id;
	uint64_t bind_id;
	uint64_t epoch;
};

struct WorkloadClient {
	struct WorkloadIdentity packet;
	struct WorkloadIdentity unnamed_statement;
	struct WorkloadIdentity unnamed_portal;
};

struct WorkloadRow {
	UT_hash_handle hh;
	struct WorkloadTags tags;
	const char *attribution;
	uint64_t forwarded;
	uint64_t completed;
	uint64_t failed;
	uint64_t skipped;
	uint64_t aborted;
	uint64_t inflight;
	uint64_t running;
	uint64_t execution_us;
	uint64_t pipeline_wait_us;
	uint64_t pool_wait_us;
	uint64_t transactions;
	uint64_t aborted_transactions;
	uint64_t transactions_inflight;
	uint64_t transaction_us;
	usec_t oldest_running;	/* transient snapshot, not accounting state */
	usec_t live_execution_us;	/* excludes completed execution_us */
};

enum FallbackRow {
	FALLBACK_MISSING,
	FALLBACK_MALFORMED,
	FALLBACK_UNSUPPORTED,
	FALLBACK_OVERFLOW,
	FALLBACK_COUNT,
};

struct WorkloadDatabase {
	struct WorkloadRow *details;
	struct WorkloadRow fallback[FALLBACK_COUNT];
};

static const char *fallback_names[] = {"missing", "malformed", "unsupported", "overflow"};
static STATLIST(live_workload_events);
static uint64_t allocation_failures;
static uint64_t database_resets;
static uint64_t next_protocol_id;

static void unsupported(struct WorkloadIdentity *identity)
{
	memset(identity, 0, sizeof(*identity));
	identity->status = WORKLOAD_TAGS_UNSUPPORTED;
}

static void invalidate_metadata(struct WorkloadClient *state)
{
	if (!state)
		return;
	unsupported(&state->packet);
	unsupported(&state->unnamed_statement);
	unsupported(&state->unnamed_portal);
}

static void invalidate_before(struct WorkloadClient *state, uint64_t epoch)
{
	if (!state)
		return;
	if (state->packet.epoch <= epoch)
		unsupported(&state->packet);
	if (state->unnamed_statement.epoch <= epoch)
		unsupported(&state->unnamed_statement);
	if (state->unnamed_portal.epoch <= epoch)
		unsupported(&state->unnamed_portal);
}

static struct WorkloadClient *get_client(PgSocket *client)
{
	bool new_state = false;

	if (!cf_workload_observation) {
		workload_client_free(client);
		return NULL;
	}
	if (client->pool->db->admin || client->replication)
		return NULL;
	if (!client->workload_client) {
		client->workload_client = calloc(1, sizeof(*client->workload_client));
		if (!client->workload_client) {
			allocation_failures++;
			return NULL;
		}
		invalidate_metadata(client->workload_client);
		new_state = true;
	}
	if (!client->workload_epoch)
		client->workload_epoch = ++next_protocol_id;
	if (new_state && client->link && client->link->query_failed) {
		struct List *item;
		bool sync_forwarded = false;

		statlist_for_each(item, &client->link->outstanding_requests) {
			OutstandingRequest *request = container_of(item, OutstandingRequest, node);

			if (request->type == PqMsg_Sync)
				sync_forwarded = true;
		}
		if (!sync_forwarded)
			client->workload_discard_epoch = client->workload_epoch;
	}
	return client->workload_client;
}

static struct WorkloadRow *get_row(PgDatabase *db, const struct WorkloadIdentity *identity)
{
	struct WorkloadDatabase *stats = db->workload_database;
	struct WorkloadRow *row;
	int fallback;
	size_t i;

	if (!stats) {
		stats = calloc(1, sizeof(*stats));
		if (!stats) {
			allocation_failures++;
			return NULL;
		}
		for (i = 0; i < FALLBACK_COUNT; i++)
			stats->fallback[i].attribution = fallback_names[i];
		db->workload_database = stats;
	}
	switch (identity->status) {
	case WORKLOAD_TAGS_MISSING:
		fallback = FALLBACK_MISSING;
		break;
	case WORKLOAD_TAGS_MALFORMED:
		fallback = FALLBACK_MALFORMED;
		break;
	case WORKLOAD_TAGS_UNSUPPORTED:
		fallback = FALLBACK_UNSUPPORTED;
		break;
	case WORKLOAD_TAGS_VALID:
		HASH_FIND(hh, stats->details, &identity->tags, sizeof(identity->tags), row);
		if (row)
			return row;
		if (cf_workload_stats_max > 0 && HASH_COUNT(stats->details) < (unsigned)cf_workload_stats_max) {
			row = calloc(1, sizeof(*row));
			if (row) {
				row->tags = identity->tags;
				row->attribution = "tagged";
				HASH_ADD(hh, stats->details, tags, sizeof(row->tags), row);
				if (!uthash_alloc_failed)
					return row;
				uthash_alloc_failed = false;
				free(row);
			}
			allocation_failures++;
		}
		fallback = FALLBACK_OVERFLOW;
		break;
	default:
		fallback = FALLBACK_UNSUPPORTED;
		break;
	}
	return &stats->fallback[fallback];
}

static void parse_sql(struct MBuf *data, bool complete, struct WorkloadIdentity *identity)
{
	const char *sql = (const char *)data->data + data->read_pos;
	size_t available = mbuf_avail_for_read(data);
	const char *nul;

	if (available > WORKLOAD_PREFIX_MAX) {
		available = WORKLOAD_PREFIX_MAX;
		complete = false;
	}
	nul = memchr(sql, 0, available);

	if (nul) {
		available = nul - sql;
		complete = true;
	}
	identity->status = workload_parse_tags(sql, available, complete, &identity->tags);
}

static void statement_identity(PgSocket *client, const char *name, struct WorkloadIdentity *identity)
{
	PgClientPreparedStatement *prepared;

	unsupported(identity);
	if (!name[0]) {
		*identity = client->workload_client->unnamed_statement;
		return;
	}
	if (!is_prepared_statements_enabled(client))
		return;
	prepared = find_client_prepared_statement(client, name);
	if (prepared) {
		const char *sql = prepared->ps->query_and_parameters;
		size_t length = strnlen(sql, WORKLOAD_PREFIX_MAX + 1);

		identity->status = workload_parse_tags(sql, length, length <= WORKLOAD_PREFIX_MAX, &identity->tags);
		identity->statement_id = prepared->workload_statement_id;
	}
}

void workload_inspect_packet(PgSocket *client, PktHdr *pkt)
{
	struct WorkloadClient *state = get_client(client);
	struct MBuf data = pkt->data;
	const char *name, *statement;
	char target;

	if (!state)
		return;
	if (client->workload_discard_epoch) {
		unsupported(&state->packet);
		return;
	}
	switch (pkt->type) {
	case PqMsg_FunctionCall:
		unsupported(&state->packet);
		break;
	case PqMsg_Query:
		unsupported(&state->packet);
		parse_sql(&data, !incomplete_pkt(pkt), &state->packet);
		state->packet.epoch = client->workload_epoch;
		unsupported(&state->unnamed_statement);
		unsupported(&state->unnamed_portal);
		break;
	case PqMsg_Parse:
		unsupported(&state->packet);
		if (!mbuf_get_string(&data, &name)) {
			unsupported(&state->unnamed_statement);
			unsupported(&state->unnamed_portal);
			break;
		}
		parse_sql(&data, !incomplete_pkt(pkt), &state->packet);
		state->packet.statement_id = ++next_protocol_id;
		state->packet.epoch = client->workload_epoch;
		if (!name[0])
			state->unnamed_statement = state->packet;
		break;
	case PqMsg_Bind:
		unsupported(&state->packet);
		if (!mbuf_get_string(&data, &name) || !mbuf_get_string(&data, &statement)) {
			unsupported(&state->unnamed_portal);
			break;
		}
		if (name[0])
			break;
		statement_identity(client, statement, &state->packet);
		state->packet.bind_id = ++next_protocol_id;
		state->packet.epoch = client->workload_epoch;
		state->unnamed_portal = state->packet;
		break;
	case PqMsg_Execute:
		unsupported(&state->packet);
		if (mbuf_get_string(&data, &name) && !name[0]) {
			state->packet = state->unnamed_portal;
			unsupported(&state->unnamed_portal);
		}
		break;
	case PqMsg_Close:
		if (mbuf_get_char(&data, &target) && mbuf_get_string(&data, &name) && !name[0]) {
			if (target == 'S')
				unsupported(&state->unnamed_statement);
			else if (target == 'P')
				unsupported(&state->unnamed_portal);
		}
		break;
	case PqMsg_Describe:
		unsupported(&state->packet);
		if (mbuf_get_char(&data, &target) && mbuf_get_string(&data, &name)) {
			if (target == 'S')
				statement_identity(client, name, &state->packet);
			else if (target == 'P' && !name[0])
				state->packet = state->unnamed_portal;
			state->packet.epoch = client->workload_epoch;
		}
		break;
	default:
		break;
	}
}

uint64_t workload_current_statement_id(PgSocket *client)
{
	return client->workload_client ? client->workload_client->packet.statement_id : 0;
}

static bool prepares_event(const OutstandingRequest *request, const struct WorkloadEvent *event)
{
	if (request->type != PqMsg_Parse && request->type != PqMsg_Bind && request->type != PqMsg_Describe)
		return false;
	if (request->workload_bind_id)
		return request->workload_bind_id == event->bind_id;
	return request->workload_statement_id && request->workload_statement_id == event->statement_id;
}

/*
 * Cache the last unrelated request ahead of the next execution. Recompute
 * only when that execution changes, not on every metadata response: long
 * Parse/Describe pipelines must not turn observation into a quadratic scan.
 */
static void select_frontier(PgSocket *server)
{
	struct List *item;
	struct WorkloadEvent *event = NULL;

	server->workload_next_event = NULL;
	server->workload_blocker = NULL;
	if (!server->workload_pending)
		return;
	statlist_for_each(item, &server->outstanding_requests) {
		OutstandingRequest *request = container_of(item, OutstandingRequest, node);

		if (request->workload.row) {
			event = &request->workload;
			break;
		}
	}
	server->workload_next_event = event;
	if (!event)
		return;
	statlist_for_each(item, &server->outstanding_requests) {
		OutstandingRequest *request = container_of(item, OutstandingRequest, node);

		if (&request->workload == event)
			break;
		if (!prepares_event(request, event))
			server->workload_blocker = request;
	}
}

void workload_request_add(PgSocket *client, OutstandingRequest *request)
{
	struct WorkloadEvent *event = &request->workload;

	if (client->workload_epoch) {
		request->workload_epoch = client->workload_epoch;
		if (request->action == RA_FORWARD &&
		    (request->type == PqMsg_Sync ||
		     (!client->workload_discard_epoch &&
		      (request->type == PqMsg_Query || request->type == PqMsg_FunctionCall)))) {
			client->workload_epoch = ++next_protocol_id;
			client->workload_discard_epoch = 0;
		}
	}
	if (!cf_workload_observation || !client->workload_client)
		return;
	if (request->type == PqMsg_Parse || request->type == PqMsg_Bind || request->type == PqMsg_Describe) {
		request->workload_statement_id = client->workload_client->packet.statement_id;
		request->workload_bind_id = client->workload_client->packet.bind_id;
		return;
	}
	if (request->action != RA_FORWARD)
		return;
	if (request->type != PqMsg_Query && request->type != PqMsg_Execute && request->type != PqMsg_FunctionCall)
		return;
	event->row = get_row(client->pool->db, &client->workload_client->packet);
	if (!event->row)
		return;
	event->forwarded_at = get_cached_time();
	event->statement_id = client->workload_client->packet.statement_id;
	event->bind_id = client->workload_client->packet.bind_id;
	event->extended = request->type == PqMsg_Execute;
	event->row->forwarded++;
	event->row->inflight++;
	statlist_append(&live_workload_events, &event->live_node);
	client->link->workload_pending++;
	if (client->link->workload_pending == 1)
		select_frontier(client->link);
	workload_advance(client->link);
}

void workload_advance(PgSocket *server)
{
	struct WorkloadEvent *event = server->workload_next_event;

	if (!server->workload_pending)
		return;
	if (server->workload_running || !event || server->workload_blocker)
		return;
	event->started_at = get_cached_time();
	event->row->pipeline_wait_us += event->started_at - event->forwarded_at;
	event->row->running++;
	server->workload_running = event;
	if (!server->workload_transaction.row) {
		server->workload_transaction.row = event->row;
		server->workload_transaction.started_at = get_cached_time();
		event->row->transactions_inflight++;
	}
}

static void finish_event(PgSocket *server, struct WorkloadEvent *event, enum WorkloadCompletion outcome)
{
	struct WorkloadRow *row = event->row;

	if (!row)
		return;
	if (event->started_at) {
		if (outcome == WORKLOAD_SKIPPED && !event->failed)
			row->pipeline_wait_us += get_cached_time() - event->started_at;
		else
			row->execution_us += get_cached_time() - event->started_at;
		row->running--;
		server->workload_running = NULL;
	} else {
		row->pipeline_wait_us += get_cached_time() - event->forwarded_at;
	}
	row->inflight--;
	server->workload_pending--;
	if (outcome == WORKLOAD_SKIPPED && event->failed)
		outcome = WORKLOAD_COMPLETED;
	if (outcome == WORKLOAD_COMPLETED) {
		row->completed++;
		if (event->failed)
			row->failed++;
	} else if (outcome == WORKLOAD_SKIPPED) {
		row->skipped++;
	} else {
		row->aborted++;
	}
	statlist_remove(&live_workload_events, &event->live_node);
	memset(event, 0, sizeof(*event));
}

void workload_request_finish(PgSocket *server, OutstandingRequest *request, enum WorkloadCompletion outcome)
{
	bool observed = request->workload.row != NULL;

	if (server->workload_blocker == request)
		server->workload_blocker = NULL;
	finish_event(server, &request->workload, outcome);
	if (observed)
		select_frontier(server);
}

void workload_server_error(PgSocket *server)
{
	struct List *item = statlist_first(&server->outstanding_requests);
	OutstandingRequest *request;

	if (server->link && item) {
		PgSocket *client = server->link;
		OutstandingRequest *head = container_of(item, OutstandingRequest, node);
		uint64_t failed_epoch = head->workload_epoch ? head->workload_epoch : client->workload_epoch;

		invalidate_before(client->workload_client, failed_epoch);
		if (head->type != PqMsg_Query && head->type != PqMsg_FunctionCall &&
		    client->workload_epoch == failed_epoch)
			client->workload_discard_epoch = failed_epoch;
	}
	if (server->workload_copy.row) {
		server->workload_copy.failed = true;
		return;
	}
	if (!item)
		return;
	request = container_of(item, OutstandingRequest, node);
	/* Match a specific Parse or Bind, never an aggregate statistics row. */
	if (server->workload_running &&
	    (&request->workload == server->workload_running ||
	     (request->workload_bind_id && request->workload_bind_id == server->workload_running->bind_id) ||
	     (request->type == PqMsg_Parse && !request->workload_bind_id && request->workload_statement_id &&
	      request->workload_statement_id == server->workload_running->statement_id)))
		server->workload_running->failed = true;
}

void workload_copy_start(PgSocket *server)
{
	struct WorkloadEvent *event = server->workload_running;

	if (!event || server->workload_copy.row)
		return;
	/* COPY drains its canonical request before ReadyForQuery. Transfer the
	 * observation to the server's COPY phase without releasing its claim. */
	statlist_remove(&live_workload_events, &event->live_node);
	server->workload_copy = *event;
	memset(event, 0, sizeof(*event));
	statlist_append(&live_workload_events, &server->workload_copy.live_node);
	server->workload_running = &server->workload_copy;
	server->workload_next_event = &server->workload_copy;
}

void workload_copy_finish(PgSocket *server)
{
	bool observed = server->workload_copy.row != NULL;

	finish_event(server, &server->workload_copy, WORKLOAD_COMPLETED);
	if (observed)
		select_frontier(server);
}

void workload_copy_command_complete(PgSocket *server)
{
	if (server->workload_copy.extended)
		workload_copy_finish(server);
}

void workload_server_close(PgSocket *server)
{
	struct List *item;

	statlist_for_each(item, &server->outstanding_requests) {
		OutstandingRequest *request = container_of(item, OutstandingRequest, node);

		finish_event(server, &request->workload, WORKLOAD_ABORTED);
	}
	finish_event(server, &server->workload_copy, WORKLOAD_ABORTED);
	server->workload_next_event = NULL;
	server->workload_blocker = NULL;
	workload_transaction_end(server, true);
}

void workload_transaction_end(PgSocket *server, bool aborted)
{
	struct WorkloadTransaction *transaction = &server->workload_transaction;

	if (!transaction->row)
		return;
	transaction->row->transaction_us += get_cached_time() - transaction->started_at;
	transaction->row->transactions_inflight--;
	transaction->row->transactions++;
	if (aborted)
		transaction->row->aborted_transactions++;
	memset(transaction, 0, sizeof(*transaction));
}

void workload_pool_wait(PgSocket *client, usec_t waited)
{
	struct WorkloadRow *row;

	if (!cf_workload_observation || !client->workload_client)
		return;
	row = get_row(client->pool->db, &client->workload_client->packet);
	if (row)
		row->pool_wait_us += waited;
}

void workload_client_free(PgSocket *client)
{
	free(client->workload_client);
	client->workload_client = NULL;
}

void workload_database_free(PgDatabase *db)
{
	struct WorkloadDatabase *stats = db->workload_database;
	struct WorkloadRow *row, *tmp;

	if (!stats)
		return;
	HASH_ITER(hh, stats->details, row, tmp) {
		HASH_DELETE(hh, stats->details, row);
		free(row);
	}
	free(stats);
	db->workload_database = NULL;
	database_resets++;
}

static void write_row(PktBuf *buf, PgDatabase *db, struct WorkloadRow *row, usec_t now)
{
	pktbuf_write_DataRow(buf, "ssssNNNNNNNNNNNNNNNN",
			     db->name, row->attribution, row->tags.team, row->tags.caller,
			     row->forwarded, row->completed, row->failed, row->skipped, row->aborted,
			     row->inflight, row->running, row->execution_us, row->pipeline_wait_us,
			     row->pool_wait_us, row->transactions, row->aborted_transactions,
			     row->transactions_inflight, row->transaction_us,
			     row->oldest_running ? now - row->oldest_running : 0, row->live_execution_us);
}

bool workload_show_stats(PgSocket *admin, const char *arg)
{
	PktBuf *buf = pktbuf_dynamic(1024);
	struct List *db_item, *item;
	usec_t now = get_cached_time();

	if (!buf)
		return admin_error(admin, "no mem");
	statlist_for_each(db_item, &database_list) {
		PgDatabase *db = container_of(db_item, PgDatabase, head);
		struct WorkloadDatabase *stats = db->workload_database;
		struct WorkloadRow *row, *tmp;
		size_t i;

		if (!stats)
			continue;
		HASH_ITER(hh, stats->details, row, tmp) {
			row->oldest_running = 0;
			row->live_execution_us = 0;
		}
		for (i = 0; i < FALLBACK_COUNT; i++) {
			stats->fallback[i].oldest_running = 0;
			stats->fallback[i].live_execution_us = 0;
		}
	}
	statlist_for_each(item, &live_workload_events) {
		struct WorkloadEvent *event = container_of(item, struct WorkloadEvent, live_node);

		if (event->started_at && (!event->row->oldest_running || event->started_at < event->row->oldest_running))
			event->row->oldest_running = event->started_at;
		if (event->started_at)
			event->row->live_execution_us += now - event->started_at;
	}
	pktbuf_write_RowDescription(buf, "ssssNNNNNNNNNNNNNNNN",
				    "database", "attribution", "team", "caller",
				    "forwarded", "completed", "failed", "skipped", "aborted",
				    "inflight", "running", "execution_us", "pipeline_wait_us", "pool_wait_us",
				    "transactions", "aborted_transactions", "transactions_inflight", "transaction_us",
				    "oldest_running_us", "live_execution_us");
	statlist_for_each(db_item, &database_list) {
		PgDatabase *db = container_of(db_item, PgDatabase, head);
		struct WorkloadDatabase *stats = db->workload_database;
		struct WorkloadRow *row, *tmp;
		size_t i;

		if (!stats)
			continue;
		HASH_ITER(hh, stats->details, row, tmp)
		write_row(buf, db, row, now);
		for (i = 0; i < FALLBACK_COUNT; i++)
			write_row(buf, db, &stats->fallback[i], now);
	}
	admin_flush(admin, buf, "SHOW");
	return true;
}

bool workload_show_status(PgSocket *admin, const char *arg)
{
	PktBuf *buf = pktbuf_dynamic(128);

	if (!buf)
		return admin_error(admin, "no mem");
	pktbuf_write_RowDescription(buf, "NNNN", "enabled", "detail_limit_per_database", "allocation_failures", "database_resets");
	pktbuf_write_DataRow(buf, "NNNN", (uint64_t)(cf_workload_observation != 0),
			     (uint64_t)MAX(0, cf_workload_stats_max), allocation_failures, database_resets);
	admin_flush(admin, buf, "SHOW");
	return true;
}
