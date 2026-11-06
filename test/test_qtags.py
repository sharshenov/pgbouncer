import threading
import time

import psycopg
import pytest
from psycopg.rows import dict_row

# All tests in this file need a tenant key to be configured, otherwise the
# query tag feature is inert. The params are appended to the existing
# [pgbouncer] section: re-entering a section resets its earlier params
# to defaults (see fill_defaults() in usual/cfparser.c).
TAG_CONFIG = """
query_tag_tenant_key = team
query_tag_keys = action,sidekiq
tag_budget_mode = off
tag_budget_share = 100
tag_budget_burst = 0
tag_stats_max = 1000
"""


def add_config(bouncer, text):
    """Append config text to the bouncer ini and reload."""
    with bouncer.ini_path.open("a") as ini:
        ini.write(text)
    bouncer.admin("RELOAD")


def tag_stats(bouncer):
    """Return SHOW TAG_STATS rows keyed by (tenant, secondary_value)."""
    rows = bouncer.admin("SHOW TAG_STATS", row_factory=dict_row)
    return {(r["tenant"], r["secondary_value"]): r for r in rows}


def tag_budgets(bouncer):
    """Return SHOW TAG_BUDGETS rows keyed by tenant."""
    rows = bouncer.admin("SHOW TAG_BUDGETS", row_factory=dict_row)
    return {r["tenant"]: r for r in rows}


@pytest.fixture
def tagged_bouncer(bouncer):
    add_config(bouncer, TAG_CONFIG)
    return bouncer


def test_tag_stats_monitoring(tagged_bouncer):
    """Queries tagged with the tenant key are accounted per tenant."""
    conn = tagged_bouncer.conn()

    conn.execute("/* team=payments */ SELECT 1")
    conn.execute("/* team=payments */ SELECT 2")
    conn.execute("/* team=web,action=api */ SELECT 3")

    rows = tag_stats(tagged_bouncer)
    assert rows[("payments", "")]["query_count"] == 2
    assert rows[("payments", "")]["db_time"] > 0
    assert rows[("web", "api")]["query_count"] == 1
    assert rows[("web", "api")]["secondary_key"] == "action"


def test_tag_stats_ignores_unknown_keys(tagged_bouncer):
    """High cardinality keys such as trace ids must not create stats rows."""
    conn = tagged_bouncer.conn()

    conn.execute("/* team=payments trace_id=abc123 */ SELECT 1")

    rows = tag_stats(tagged_bouncer)
    assert len(rows) == 1
    assert ("payments", "") in rows


def test_tag_stats_ignores_prose_comments(tagged_bouncer):
    """Regular SQL comments are not tags, and only leading tags count."""
    conn = tagged_bouncer.conn()

    conn.execute("/* payments report query */ SELECT 1")
    conn.execute("SELECT 2 /* team=payments */")

    assert len(tag_stats(tagged_bouncer)) == 0


def test_tag_stats_quoted_values(tagged_bouncer):
    conn = tagged_bouncer.conn()

    conn.execute("/* team='big team' action=run */ SELECT 1")

    rows = tag_stats(tagged_bouncer)
    assert rows[("big team", "run")]["query_count"] == 1


def test_tag_stats_prepared_statements(tagged_bouncer):
    """Tags travel with named prepared statements on every Bind."""
    conn = tagged_bouncer.conn()

    cur = conn.cursor()
    query = "/* team=payments,action=checkout */ SELECT %s::int"
    cur.execute(query, [1], prepare=True)
    cur.execute(query, [2], prepare=True)
    cur.execute(query, [3], prepare=True)

    rows = tag_stats(tagged_bouncer)
    assert rows[("payments", "checkout")]["query_count"] >= 2


def test_tag_budget_config_section(tagged_bouncer):
    """Budgets from the [tag_budgets] section are loaded on RELOAD."""
    add_config(
        tagged_bouncer,
        "\n[tag_budgets]\n"
        "payments = share=25 burst=10 mode=enforce\n"
        "web = share=50 burst=20 mode=warn per_query_ms=100\n",
    )

    rows = tag_budgets(tagged_bouncer)
    assert rows["payments"]["share"] == 25
    assert rows["payments"]["burst"] == 10
    assert rows["payments"]["mode"] == "enforce"
    assert rows["web"]["per_query_ms"] == 100


def test_tag_budget_admin_commands(tagged_bouncer):
    """TAG_BUDGET SET/DEL/RESET manage budgets at runtime."""
    tagged_bouncer.admin(
        'TAG_BUDGET "SET payments share=50 burst=5 mode=enforce"')
    rows = tag_budgets(tagged_bouncer)
    assert rows["payments"]["share"] == 50
    assert rows["payments"]["mode"] == "enforce"

    tagged_bouncer.admin('TAG_BUDGET "RESET payments"')
    rows = tag_budgets(tagged_bouncer)
    assert rows["payments"]["debt"] == 0

    tagged_bouncer.admin('TAG_BUDGET "DEL payments"')
    assert "payments" not in tag_budgets(tagged_bouncer)


def test_tag_budget_admin_survives_reload(tagged_bouncer):
    """Admin only budgets survive RELOAD, config budgets are reapplied."""
    add_config(
        tagged_bouncer,
        "\n[tag_budgets]\nweb = share=50 burst=20 mode=warn\n",
    )

    tagged_bouncer.admin(
        'TAG_BUDGET "SET payments share=50 burst=5 mode=enforce"')
    tagged_bouncer.admin('TAG_BUDGET "SET web share=90"')
    tagged_bouncer.admin("RELOAD")

    rows = tag_budgets(tagged_bouncer)
    assert rows["payments"]["share"] == 50
    # the admin tweak on a config budget is reverted by the reload
    assert rows["web"]["share"] == 50


def test_tag_budget_queueing(tagged_bouncer):
    """
    An over budget tenant queues queries inside pgbouncer until the
    bucket has drained. The client only experiences latency, the query
    must still succeed.
    """
    tagged_bouncer.admin(
        'TAG_BUDGET "SET payments share=50 burst=5 mode=enforce"')
    conn = tagged_bouncer.conn()

    # fill the bucket with 4 seconds of db time
    conn.execute("/* team=payments */ SELECT pg_sleep(4)")

    # the small query has to queue roughly the drain time of the debt
    # above the burst limit, around 2s here
    start = time.monotonic()
    conn.execute("/* team=payments */ SELECT 1")
    elapsed = time.monotonic() - start

    assert elapsed > 1.0

    rows = tag_budgets(tagged_bouncer)
    assert rows["payments"]["queued"] >= 1
    assert rows["payments"]["active"] == 0


def test_tag_budget_queueing_transparent(tagged_bouncer):
    """Queued queries get no error and no cancellation."""
    tagged_bouncer.admin(
        'TAG_BUDGET "SET payments share=50 burst=5 mode=enforce"')
    conn = tagged_bouncer.conn()

    conn.execute("/* team=payments */ SELECT pg_sleep(4)")

    cur = conn.execute("/* team=payments */ SELECT 42")
    assert cur.fetchone()[0] == 42


def test_tag_budget_concurrency_limit(tagged_bouncer):
    """At most `concurrency` queries of a tenant run at the same time."""
    tagged_bouncer.admin(
        'TAG_BUDGET "SET payments concurrency=1 mode=enforce"')

    conn1 = tagged_bouncer.conn()
    conn2 = tagged_bouncer.conn()
    results = {}

    def run_first():
        cur = conn1.execute("/* team=payments */ SELECT pg_sleep(2)")
        results["first"] = cur.fetchall()

    thread = threading.Thread(target=run_first)
    start = time.monotonic()
    thread.start()
    time.sleep(0.5)  # make sure the first query is in flight

    conn2.execute("/* team=payments */ SELECT pg_sleep(2)")
    elapsed = time.monotonic() - start
    thread.join()

    # the second query can only start after the first one finished, so
    # it takes the full 4s instead of 2.5s
    assert elapsed > 3.0
    assert results["first"] == [(2,)]
    rows = tag_budgets(tagged_bouncer)
    assert rows["payments"]["active"] == 0


def test_tag_budget_reject(tagged_bouncer):
    """Queries that can never fit the budget are rejected with an error."""
    tagged_bouncer.admin(
        'TAG_BUDGET "SET payments share=100 burst=0 mode=enforce"')
    conn = tagged_bouncer.conn()

    # establish the cost estimate with a long query
    conn.execute("/* team=payments */ SELECT pg_sleep(1)")

    # the estimate makes the next long query over the per query limit
    tagged_bouncer.admin('TAG_BUDGET "SET payments per_query_ms=100"')
    with pytest.raises(psycopg.Error, match="query rejected"):
        conn.execute("/* team=payments */ SELECT pg_sleep(1)")

    # the connection is still usable, the rejection only consumed one query
    cur = conn.execute("SELECT 1")
    assert cur.fetchone()[0] == 1


def test_tag_budget_wait_timeout(tagged_bouncer):
    """Queued queries are disconnected when the timeout expires."""
    add_config(tagged_bouncer, "tag_budget_wait_timeout = 2\n")
    tagged_bouncer.admin(
        'TAG_BUDGET "SET payments share=1 burst=1 mode=enforce"')

    conn = tagged_bouncer.conn()
    conn.execute("/* team=payments */ SELECT pg_sleep(1)")

    # with a 1% drain rate the bucket effectively never drains, so the
    # query has to hit the wait timeout
    with pytest.raises(psycopg.Error):
        conn.execute("/* team=payments */ SELECT 1")


def test_tag_budget_warn_mode(tagged_bouncer):
    """In warn mode the query runs and the client gets a warning."""
    tagged_bouncer.admin(
        'TAG_BUDGET "SET payments share=5 burst=1 mode=warn"')
    conn = tagged_bouncer.conn()

    conn.execute("/* team=payments */ SELECT pg_sleep(1)")

    notices = []
    conn.add_notice_handler(
        lambda diag: notices.append(f"{diag.severity}: {diag.message_primary}")
    )

    cur = conn.execute("/* team=payments */ SELECT 1")
    assert cur.fetchone()[0] == 1
    assert any("query tag budget reached" in n for n in notices)
