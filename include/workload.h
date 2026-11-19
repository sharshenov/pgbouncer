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

#ifndef WORKLOAD_H
#define WORKLOAD_H

struct WorkloadRow;
struct WorkloadClient;
struct WorkloadDatabase;

/* Owned by the canonical outstanding request, not the client's latest tags. */
struct WorkloadEvent {
	struct List live_node;
	struct WorkloadRow *row;
	usec_t forwarded_at;
	usec_t started_at;
	bool failed;
	uint64_t statement_id;
	uint64_t bind_id;
	bool extended;
};

struct WorkloadTransaction {
	struct WorkloadRow *row;
	usec_t started_at;
};

enum WorkloadCompletion {
	WORKLOAD_COMPLETED,
	WORKLOAD_SKIPPED,
	WORKLOAD_ABORTED,
};

extern int cf_workload_observation;
extern int cf_workload_stats_max;

void workload_inspect_packet(PgSocket *client, PktHdr *pkt);
uint64_t workload_current_statement_id(PgSocket *client);
void workload_request_add(PgSocket *client, OutstandingRequest *request);
void workload_request_finish(PgSocket *server, OutstandingRequest *request, enum WorkloadCompletion outcome);
void workload_advance(PgSocket *server);
void workload_server_error(PgSocket *server);
void workload_server_close(PgSocket *server);
void workload_copy_start(PgSocket *server);
void workload_copy_finish(PgSocket *server);
void workload_copy_command_complete(PgSocket *server);
void workload_transaction_end(PgSocket *server, bool aborted);
void workload_pool_wait(PgSocket *client, usec_t waited);
void workload_client_free(PgSocket *client);
void workload_database_free(PgDatabase *db);
bool workload_show_stats(PgSocket *admin, const char *arg);
bool workload_show_status(PgSocket *admin, const char *arg);

#endif
