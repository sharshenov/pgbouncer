import socket
import struct
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest
from psycopg.rows import dict_row

from .test_prepared import collect_pipeline_results
from .utils import LIBPQ_SUPPORTS_PIPELINING, LINUX, wait_until


@pytest.fixture
def observed(bouncer):
    with bouncer.ini_path.open("a") as config:
        config.write(
            "\nworkload_observation=1\nworkload_stats_max=100\n"
            "pool_mode=transaction\nmax_prepared_statements=200\n"
        )
    bouncer.admin("RELOAD")
    return bouncer


def rows(bouncer):
    return {
        (r["database"], r["attribution"], r["team"], r["caller"]): r
        for r in bouncer.admin("SHOW WORKLOAD_STATS", row_factory=dict_row)
    }


def tagged(bouncer, team="one", caller="Jobs::Sync", database="p0"):
    return rows(bouncer)[(database, "tagged", team, caller)]


def wait_running(bouncer, team="one", caller="Jobs::Sync"):
    for _ in wait_until("Workload did not become visible"):
        row = rows(bouncer).get(("p0", "tagged", team, caller))
        if row and row["running"] == 1:
            return row


def receive_ready(wire):
    def receive(length):
        data = b""
        while len(data) < length:
            chunk = wire.recv(length - len(data))
            assert chunk
            data += chunk
        return data

    packets = []
    for _ in range(100):
        header = receive(5)
        length = struct.unpack("!I", header[1:])[0] - 4
        assert 0 <= length < 1024 * 1024
        packets.append((header[:1], receive(length)))
        if header[:1] == b"Z":
            return packets
    raise AssertionError("No ReadyForQuery within the test's packet bound")


def test_workload_disabled_by_default(bouncer):
    bouncer.sql("/*team=one,caller=job*/SELECT 1")
    assert rows(bouncer) == {}
    status = bouncer.admin("SHOW WORKLOAD_STATUS", row_factory=dict_row)[0]
    assert status["enabled"] == 0


def test_workload_simple_and_unnamed(observed):
    with observed.conn(prepare_threshold=None) as conn:
        conn.execute("/*team=one,caller='Jobs%3A%3ASync'*/SELECT 1")
        conn.execute("/*team=one,caller='Jobs%%3A%%3ASync'*/SELECT %s::int", (2,))
    row = tagged(observed)
    assert row["forwarded"] == row["completed"] == 2
    assert row["inflight"] == row["running"] == 0
    assert row["failed"] == row["aborted"] == row["skipped"] == 0
    assert row["transactions"] == 2
    assert row["transactions_inflight"] == 0


def test_workload_named_reuse(observed):
    query = "/*team=one,caller='Jobs%%3A%%3ASync'*/SELECT %s::int"
    with observed.conn() as conn:
        for i in range(8):
            assert conn.execute(query, (i,), prepare=True).fetchone() == (i,)
    assert tagged(observed)["completed"] == 8


@pytest.mark.skipif("not LIBPQ_SUPPORTS_PIPELINING")
def test_workload_pipeline_identity_is_not_last_writer_wins(observed):
    with observed.conn(prepare_threshold=None) as conn, conn.pipeline():
        for i in range(6):
            conn.execute(f"/*team=one,caller=job{i}*/SELECT %s::int", (i,))
    for i in range(6):
        row = tagged(observed, caller=f"job{i}")
        assert row["forwarded"] == row["completed"] == 1
        assert row["inflight"] == row["running"] == 0


def test_workload_transaction_owner_pinned(observed):
    with observed.conn() as conn:
        conn.execute("/*team=one,caller='Jobs%3A%3ASync'*/BEGIN")
        assert tagged(observed)["transactions_inflight"] == 1
        conn.execute("/*team=two,caller=other*/SELECT 1")
        assert tagged(observed, "two", "other")["transactions_inflight"] == 0
        conn.execute("COMMIT")
    row = tagged(observed)
    assert row["transactions"] == 1
    assert row["transaction_us"] > 0
    assert row["transactions_inflight"] == 0
    assert tagged(observed, "two", "other")["transactions"] == 0


def test_workload_inflight_and_disable_cleanup(observed):
    with observed.conn() as conn, ThreadPoolExecutor(max_workers=1) as pool:
        task = pool.submit(
            conn.execute, "/*team=one,caller='Jobs%3A%3ASync'*/SELECT pg_sleep(0.5)"
        )
        row = wait_running(observed)
        assert row["inflight"] == 1
        assert row["oldest_running_us"] >= 0
        observed.admin("SET workload_observation = 0")
        task.result(timeout=5)
        row = tagged(observed)
        assert row["completed"] == 1
        assert row["inflight"] == row["running"] == row["transactions_inflight"] == 0
        assert row["execution_us"] >= 400000


def test_workload_cardinality_preserves_overflow(observed):
    observed.admin("SET workload_stats_max = 2")
    with observed.conn() as conn:
        for name in ("a", "b", "c", "a"):
            conn.execute(f"/*team=one,caller={name}*/SELECT 1")
    snapshot = rows(observed)
    detail = [r for k, r in snapshot.items() if k[1] == "tagged"]
    assert len(detail) == 2
    assert snapshot[("p0", "overflow", "", "")]["completed"] == 1
    assert tagged(observed, caller="a")["completed"] == 2
    assert sum(r["completed"] for r in snapshot.values()) == 4


@pytest.mark.parametrize("limit", [-1, 0, 2])
def test_workload_status_reports_effective_detail_limit(observed, limit):
    observed.admin(f"SET workload_stats_max = '{limit}'")
    status = observed.admin("SHOW WORKLOAD_STATUS", row_factory=dict_row)[0]
    assert status["detail_limit_per_database"] == max(0, limit)
    observed.sql("/*team=one,caller=limit*/SELECT 1")
    snapshot = rows(observed)
    if limit <= 0:
        assert snapshot[("p0", "overflow", "", "")]["completed"] == 1
    else:
        assert tagged(observed, caller="limit")["completed"] == 1


def test_workload_unknown_malformed_and_long_identities(observed):
    with observed.conn() as conn:
        conn.execute("SELECT 1")
        conn.execute("/*team=x,caller='%ZZ'*/SELECT 1")
        conn.execute("/*team=x,caller='" + "a" * 513 + "'*/SELECT 1")
    snapshot = rows(observed)
    for status in ("missing", "malformed", "unsupported"):
        assert snapshot[("p0", status, "", "")]["completed"] == 1


def test_workload_errors_reconcile_without_protocol_changes(observed):
    with observed.conn(prepare_threshold=None) as conn:
        with pytest.raises(psycopg.errors.DivisionByZero):
            conn.execute("/*team=one,caller='Jobs%%3A%%3ASync'*/SELECT 1/%s", (0,))
        assert conn.execute("SELECT 1").fetchone() == (1,)
    row = tagged(observed)
    assert row["completed"] == row["failed"] == 1
    assert row["inflight"] == row["running"] == row["transactions_inflight"] == 0


def test_workload_cancellation_is_a_failed_completed_exchange(observed):
    with observed.conn() as conn, ThreadPoolExecutor(max_workers=1) as pool:
        task = pool.submit(
            conn.execute, "/*team=one,caller='Jobs%3A%3ASync'*/SELECT pg_sleep(5)"
        )
        wait_running(observed)
        conn.cancel()
        with pytest.raises(psycopg.errors.QueryCanceled):
            task.result(timeout=5)
    row = tagged(observed)
    assert row["completed"] == row["failed"] == 1
    assert row["inflight"] == row["running"] == row["transactions_inflight"] == 0


def test_workload_server_loss_preserves_observed_work(observed):
    with observed.conn() as conn, ThreadPoolExecutor(max_workers=1) as pool:
        task = pool.submit(
            conn.execute, "/*team=one,caller='Jobs%3A%3ASync'*/SELECT pg_sleep(5)"
        )
        wait_running(observed)
        observed.admin("KILL p0")
        with pytest.raises(psycopg.Error):
            task.result(timeout=5)
    row = tagged(observed)
    assert row["aborted"] == 1
    assert row["execution_us"] > 0
    assert row["aborted_transactions"] == 1
    assert row["inflight"] == row["running"] == row["transactions_inflight"] == 0


def test_workload_pool_wait_does_not_decrease_execution_counter(observed):
    with observed.conn() as conn:
        conn.execute("/*team=one,caller='Jobs%3A%3ASync'*/SELECT 1")
    before = tagged(observed)["execution_us"]
    with (
        observed.conn() as a,
        observed.conn() as b,
        observed.conn() as c,
        ThreadPoolExecutor(max_workers=3) as pool,
    ):
        blockers = [
            pool.submit(conn.execute, "/*team=one,caller=blocker*/SELECT pg_sleep(0.5)")
            for conn in (a, b)
        ]
        for _ in wait_until("Both backends did not become occupied"):
            row = rows(observed).get(("p0", "tagged", "one", "blocker"))
            if row and row["running"] == 2:
                break
        waiter = pool.submit(
            c.execute, "/*team=one,caller='Jobs%3A%3ASync'*/SELECT pg_sleep(0.5)"
        )
        for _ in wait_until("Backend acquisition wait was not recorded"):
            row = tagged(observed)
            assert row["execution_us"] >= before
            if row["pool_wait_us"] > 100000 and row["running"] == 1:
                assert row["completed"] == 1
                break
        for task in (*blockers, waiter):
            task.result(timeout=5)
    row = tagged(observed)
    assert row["execution_us"] >= before + 400000
    assert row["pool_wait_us"] > 100000


@pytest.mark.skipif("not LIBPQ_SUPPORTS_PIPELINING")
def test_workload_pipeline_error_skips_later_operations(observed):
    with observed.conn(prepare_threshold=None) as conn:
        with pytest.raises(psycopg.errors.DivisionByZero), conn.pipeline():
            conn.execute("/*team=one,caller=failed*/SELECT 1/%s", (0,))
            conn.execute("/*team=one,caller=skipped*/SELECT %s::int", (1,))
        assert conn.execute("SELECT 1").fetchone() == (1,)
    failed = tagged(observed, caller="failed")
    skipped = tagged(observed, caller="skipped")
    assert failed["failed"] == failed["completed"] == 1
    assert skipped["skipped"] == 1
    assert skipped["completed"] == skipped["inflight"] == skipped["running"] == 0


def test_workload_database_dimensions_remain_separate(observed):
    for database in ("p0", "p1"):
        observed.sql("/*team=one,caller='Jobs%3A%3ASync'*/SELECT 1", dbname=database)
    assert tagged(observed, database="p0")["completed"] == 1
    assert tagged(observed, database="p1")["completed"] == 1


def test_workload_unnamed_metadata_is_not_reused_across_disablement(observed):
    observed.admin("SET pool_mode = session")
    with observed.conn() as conn:
        raw = conn.pgconn
        assert (
            raw.prepare(b"", b"/*team=one,caller=old*/SELECT $1::int").status
            == psycopg.pq.ExecStatus.COMMAND_OK
        )
        observed.admin("SET workload_observation = 0")
        assert (
            raw.prepare(b"", b"/*team=one,caller=new*/SELECT $1::int").status
            == psycopg.pq.ExecStatus.COMMAND_OK
        )
        observed.admin("SET workload_observation = 1")
        result = raw.exec_prepared(b"", [b"12"])
        assert result.get_value(0, 0) == b"12"
    snapshot = rows(observed)
    assert snapshot[("p0", "unsupported", "", "")]["completed"] == 1
    assert ("p0", "tagged", "one", "old") not in snapshot


@pytest.mark.skipif("not LIBPQ_SUPPORTS_PIPELINING")
def test_workload_skipped_parse_cannot_poison_unnamed_metadata(observed):
    observed.admin("SET pool_mode = session")
    with observed.conn() as conn:
        raw = conn.pgconn
        assert (
            raw.prepare(b"", b"/*team=one,caller=old*/SELECT $1::int").status
            == psycopg.pq.ExecStatus.COMMAND_OK
        )
        raw.enter_pipeline_mode()
        raw.send_query_prepared(b"", [b"not-an-integer"])
        raw.send_prepare(b"", b"/*team=one,caller=never-executed*/SELECT $1::int")
        raw.pipeline_sync()
        assert collect_pipeline_results(raw) == [
            psycopg.pq.ExecStatus.FATAL_ERROR,
            psycopg.pq.ExecStatus.PIPELINE_ABORTED,
            psycopg.pq.ExecStatus.PIPELINE_SYNC,
        ]
        raw.exit_pipeline_mode()
        result = raw.exec_prepared(b"", [b"12"])
        assert result.get_value(0, 0) == b"12"
    snapshot = rows(observed)
    assert snapshot[("p0", "unsupported", "", "")]["completed"] == 1
    assert ("p0", "tagged", "one", "never-executed") not in snapshot


def test_workload_copy_success_is_not_skipped(observed):
    observed.admin("SET pool_mode = session")
    with observed.conn() as conn:
        conn.execute("CREATE TEMP TABLE workload_copy (i int)")
        with conn.cursor().copy(
            "/*team=one,caller=copy*/COPY workload_copy FROM STDIN"
        ) as copy:
            copy.write_row((1,))
        with conn.cursor().copy(
            "/*team=one,caller=copy*/COPY workload_copy TO STDOUT"
        ) as copy:
            assert len(list(copy.rows())) == 1
    row = tagged(observed, caller="copy")
    assert row["completed"] == 2
    assert row["failed"] == row["skipped"] == row["aborted"] == 0
    assert row["inflight"] == row["running"] == row["transactions_inflight"] == 0


def test_workload_copy_failure_reconciles(observed):
    observed.admin("SET pool_mode = session")
    with observed.conn() as conn:
        conn.execute("CREATE TEMP TABLE workload_copy (i int)")
        with (
            pytest.raises(psycopg.errors.InvalidTextRepresentation),
            conn.cursor().copy(
                "/*team=one,caller=copy*/COPY workload_copy FROM STDIN"
            ) as copy,
        ):
            copy.write(b"not-an-integer\n")
        assert conn.execute("SELECT 1").fetchone() == (1,)
    row = tagged(observed, caller="copy")
    assert row["completed"] == row["failed"] == 1
    assert row["skipped"] == 0
    assert row["inflight"] == row["running"] == row["transactions_inflight"] == 0


@pytest.mark.skipif("not LINUX")
def test_workload_fragmented_parse_does_not_inherit_previous_statement(observed):
    observed.admin("SET pool_mode = session")
    observed.admin("SET max_prepared_statements = 0")
    with observed.conn() as conn:
        raw = conn.pgconn
        assert (
            raw.prepare(b"", b"/*team=one,caller=old*/SELECT $1::int").status
            == psycopg.pq.ExecStatus.COMMAND_OK
        )
        payload = b"\0/*team=one,caller=new*/SELECT $1::int+1\0\0\0"
        packet_header = b"P" + struct.pack("!I", len(payload) + 4)
        before = next(
            r
            for r in observed.admin("SHOW STATS", row_factory=dict_row)
            if r["database"] == "p0"
        )["total_received"]
        with socket.fromfd(raw.socket, socket.AF_INET, socket.SOCK_STREAM) as wire:
            wire.settimeout(5)
            wire.sendall(packet_header)
            for _ in wait_until("Fragmented Parse header did not reach the proxy"):
                current = next(
                    r
                    for r in observed.admin("SHOW STATS", row_factory=dict_row)
                    if r["database"] == "p0"
                )["total_received"]
                if current > before:
                    break
            wire.sendall(payload + b"S" + struct.pack("!I", 4))

            receive_ready(wire)
        result = raw.exec_prepared(b"", [b"12"])
        assert result.get_value(0, 0) == b"13"
    assert rows(observed)[("p0", "unsupported", "", "")]["completed"] == 1
    assert ("p0", "tagged", "one", "old") not in rows(observed)


@pytest.mark.skipif("not LIBPQ_SUPPORTS_PIPELINING")
@pytest.mark.parametrize("same_attribution", [False, True])
def test_workload_unrelated_parse_error_does_not_charge_skipped_work(
    observed, same_attribution
):
    with observed.conn() as conn:
        raw = conn.pgconn
        raw.enter_pipeline_mode()
        caller = b"skipped" if same_attribution else b"badparse"
        raw.send_prepare(
            b"bad", b"/*team=one,caller=" + caller + b"*/SELECT no_such_column"
        )
        raw.send_query_params(b"/*team=one,caller=skipped*/SELECT $1::int", [b"1"])
        raw.pipeline_sync()
        assert collect_pipeline_results(raw) == [
            psycopg.pq.ExecStatus.FATAL_ERROR,
            psycopg.pq.ExecStatus.PIPELINE_ABORTED,
            psycopg.pq.ExecStatus.PIPELINE_SYNC,
        ]
        raw.exit_pipeline_mode()
    row = tagged(observed, caller="skipped")
    assert row["skipped"] == 1
    assert row["execution_us"] == row["failed"] == row["completed"] == 0


@pytest.mark.skipif("not LIBPQ_SUPPORTS_PIPELINING")
def test_workload_parse_arriving_after_error_is_ignored_until_sync(observed):
    observed.admin("SET pool_mode = session")
    with observed.conn() as conn:
        raw = conn.pgconn
        assert (
            raw.prepare(b"", b"/*team=one,caller=old*/SELECT $1::int").status
            == psycopg.pq.ExecStatus.COMMAND_OK
        )
        raw.enter_pipeline_mode()
        raw.send_query_prepared(b"", [b"not-an-integer"])
        raw.send_flush_request()
        assert raw.flush() == 0
        for _ in range(10):
            result = raw.get_result()
            if result is not None:
                assert result.status == psycopg.pq.ExecStatus.FATAL_ERROR
                break
        else:
            raise AssertionError("No Bind error arrived before sending Parse")
        raw.send_prepare(b"", b"/*team=one,caller=never-executed*/SELECT $1::int+1")
        raw.pipeline_sync()
        assert collect_pipeline_results(raw) == [
            psycopg.pq.ExecStatus.PIPELINE_ABORTED,
            psycopg.pq.ExecStatus.PIPELINE_SYNC,
        ]
        raw.exit_pipeline_mode()
        result = raw.exec_prepared(b"", [b"12"])
        assert result.get_value(0, 0) == b"12"
    snapshot = rows(observed)
    assert ("p0", "tagged", "one", "never-executed") not in snapshot
    assert snapshot[("p0", "unsupported", "", "")]["completed"] == 1


@pytest.mark.skipif("not LIBPQ_SUPPORTS_PIPELINING")
def test_workload_post_sync_metadata_survives_earlier_pipeline_error(observed):
    observed.admin("SET pool_mode = session")
    with observed.conn() as conn:
        raw = conn.pgconn
        raw.enter_pipeline_mode()
        raw.send_prepare(b"bad", b"/*team=one,caller=bad*/SELECT no_such_column")
        raw.pipeline_sync()
        raw.send_prepare(b"", b"/*team=one,caller=new*/SELECT $1::int+1")
        raw.send_query_prepared(b"", [b"12"])
        raw.pipeline_sync()
        assert collect_pipeline_results(raw) == [
            psycopg.pq.ExecStatus.FATAL_ERROR,
            psycopg.pq.ExecStatus.PIPELINE_SYNC,
        ]
        assert collect_pipeline_results(raw) == [
            psycopg.pq.ExecStatus.COMMAND_OK,
            psycopg.pq.ExecStatus.TUPLES_OK,
            psycopg.pq.ExecStatus.PIPELINE_SYNC,
        ]
        raw.exit_pipeline_mode()
        result = raw.exec_prepared(b"", [b"12"])
        assert result.get_value(0, 0) == b"13"
    assert tagged(observed, caller="new")["completed"] == 2


@pytest.mark.skipif("not LINUX")
def test_workload_extended_copy_finishes_before_later_error_in_same_sync(observed):
    def packet(code, payload):
        return code + struct.pack("!I", len(payload) + 4) + payload

    with (
        observed.conn() as conn,
        socket.fromfd(conn.pgconn.socket, socket.AF_INET, socket.SOCK_STREAM) as wire,
    ):
        wire.settimeout(5)
        wire.sendall(
            b"".join(
                [
                    packet(
                        b"P",
                        b"copy\0/*team=one,caller=copy*/COPY (SELECT 1) TO STDOUT\0\0\0",
                    ),
                    packet(b"B", b"\0copy\0" + b"\0" * 6),
                    packet(b"E", b"\0" + b"\0" * 4),
                    packet(b"P", b"bad\0/*team=one,caller=bad*/SELECT 1/0\0\0\0"),
                    packet(b"B", b"\0bad\0" + b"\0" * 6),
                    packet(b"E", b"\0" + b"\0" * 4),
                    packet(b"S", b""),
                ]
            )
        )
        packets = receive_ready(wire)
        assert any(code == b"H" for code, _ in packets)
        assert any(code == b"E" for code, _ in packets)
    copied = tagged(observed, caller="copy")
    failed = tagged(observed, caller="bad")
    assert copied["completed"] == 1
    assert copied["failed"] == copied["skipped"] == copied["inflight"] == 0
    assert failed["failed"] == failed["completed"] == 1


@pytest.mark.skipif("not LIBPQ_SUPPORTS_PIPELINING")
def test_workload_bulk_close_releases_all_pipeline_claims(observed):
    count = 200
    with (
        observed.conn(prepare_threshold=None) as conn,
        ThreadPoolExecutor(max_workers=1) as pool,
    ):

        def run_pipeline():
            with conn.pipeline():
                for i in range(count):
                    conn.execute(
                        "/*team=one,caller=bulk*/SELECT pg_sleep(%s)",
                        (10 if i == 0 else 0,),
                    )

        task = pool.submit(run_pipeline)
        try:
            for _ in wait_until("Pipeline claims were not all visible"):
                snapshot = rows(observed)
                # A TCP prefix fragment may be explicitly unsupported. Its
                # claim still belongs in the resource total and must release.
                if sum(row["forwarded"] for row in snapshot.values()) == count:
                    break
        except TimeoutError as exc:
            raise AssertionError(f"Pipeline observations: {rows(observed)}") from exc
        observed.admin("KILL p0")
        with pytest.raises(psycopg.Error):
            task.result(timeout=5)
    snapshot = rows(observed)
    assert sum(row["forwarded"] for row in snapshot.values()) == count
    assert sum(row["aborted"] for row in snapshot.values()) == count
    assert sum(row["completed"] for row in snapshot.values()) == 0
    assert all(
        row["inflight"] == row["running"] == row["transactions_inflight"] == 0
        for row in snapshot.values()
    )
