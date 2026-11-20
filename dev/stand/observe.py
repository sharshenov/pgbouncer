"""Read observation SQL from fixed synthetic-stand targets only."""

import json
import os
import time
from decimal import Decimal

import psycopg
from psycopg.rows import dict_row


def integer_counters(rows):
    result = []
    for row in rows:
        item = {}
        for key, value in row.items():
            if isinstance(value, Decimal):
                if not value.is_finite() or value != value.to_integral_value():
                    raise ValueError(f"Non-integral observation counter: {key}")
                value = int(value)
            item[key] = value
        result.append(item)
    return result


def collect():
    result = {"collected_at_unix": time.time(), "processes": {}}
    for name in ("a", "b"):
        with psycopg.connect(
            host=f"proxy-{name}",
            hostaddr=None,
            port=6432,
            dbname="pgbouncer",
            user="stand",
            password=os.environ["STAND_PASSWORD"],
            sslmode="disable",
            connect_timeout=5,
            autocommit=True,
            prepare_threshold=None,
            row_factory=dict_row,
        ) as conn:
            snapshot = {}
            for key, sql in (
                ("status", "SHOW WORKLOAD_STATUS"),
                ("stats", "SHOW WORKLOAD_STATS"),
            ):
                snapshot[key] = integer_counters(conn.execute(sql).fetchall())
            result["processes"][f"proxy-{name}"] = snapshot
    return result


if __name__ == "__main__":
    print(json.dumps(collect(), indent=2))
