"""Synthetic workload driver: fixed stand destinations, bounded histograms."""

import argparse
import asyncio
import concurrent.futures
import json
import math
import multiprocessing
import os
import random
import time
from collections import Counter
from pathlib import Path
from urllib.parse import quote

import psycopg

HOSTS = ("proxy-a", "proxy-b")
DATABASES = ("primary", "secondary")
HISTOGRAM_BINS = 250  # Exponential 1.1x buckets, microseconds through hours.


class Histogram:
    def __init__(self):
        self.bins = [0] * HISTOGRAM_BINS
        self.total = 0
        self.maximum = 0

    def observe(self, seconds):
        us = max(1, seconds * 1_000_000)
        i = min(len(self.bins) - 1, int(math.log(us, 1.1)))
        self.bins[i] += 1
        self.total += 1
        self.maximum = max(self.maximum, seconds)

    def percentile_ms(self, fraction):
        remaining = math.ceil(self.total * fraction)
        if remaining == 0:
            return None
        for i, count in enumerate(self.bins):
            remaining -= count
            if remaining <= 0:
                return 1.1 ** (i + 1) / 1000
        raise AssertionError("Histogram count mismatch")

    def report(self):
        return {
            "count": self.total,
            "p50_ms_upper": self.percentile_ms(0.5),
            "p95_ms_upper": self.percentile_ms(0.95),
            "p99_ms_upper": self.percentile_ms(0.99),
            "max_ms": self.maximum * 1000,
        }


def comment(client, sequence):
    # Production-shaped, but fabricated team/caller values and no copied SQL/data.
    team = f"synthetic-team-{client % 4}"
    caller = f"Synthetic::Workers::Workload{client % 32}"
    return f"/*caller='{quote(caller, safe='')}',team='{team}',trace_id='synthetic-{client}-{sequence}'*/ "


async def connect(host, database, prepared_mode):
    # No arbitrary connection string, environment PGHOST, service, or hostaddr.
    return await psycopg.AsyncConnection.connect(
        host=host,
        hostaddr=None,
        port=6432,
        dbname=database,
        user="stand",
        password=os.environ["STAND_PASSWORD"],
        sslmode="disable",
        connect_timeout=10,
        autocommit=True,
        prepare_threshold=5 if prepared_mode == "named" else None,
    )


async def initialize():
    # Both aliases connect to different stand-only backend containers.
    for database in DATABASES:
        async with await connect(HOSTS[0], database, "unnamed") as conn:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS synthetic_items (id int PRIMARY KEY, value int NOT NULL)"
            )
            await conn.execute(
                "INSERT INTO synthetic_items SELECT n, n FROM generate_series(1,100000) n ON CONFLICT DO NOTHING"
            )
            await conn.execute("ANALYZE synthetic_items")


async def operation(conn, profile, rng, tag, key):
    parameter_tag = tag.replace("%", "%%")
    if profile in ("smoke", "throughput"):
        cur = await conn.execute(
            parameter_tag + "SELECT value FROM synthetic_items WHERE id = %s", (key,)
        )
        if (await cur.fetchone()) is None:
            raise AssertionError("Missing synthetic row")
        return 1
    if profile == "occupancy":
        await conn.execute(tag + "SELECT pg_sleep(0.02)")
        return 1
    choice = rng.randrange(100)
    if choice < 65:
        await conn.execute(
            parameter_tag + "SELECT value FROM synthetic_items WHERE id = %s", (key,)
        )
        return 1
    if choice < 85:
        await conn.execute(
            parameter_tag + "UPDATE synthetic_items SET value=value+1 WHERE id=%s",
            (key,),
        )
        return 1
    if choice < 98:
        # Count transaction entry/exit as SQL messages too.
        await conn.execute(tag + "BEGIN")
        try:
            await conn.execute(
                parameter_tag + "SELECT value FROM synthetic_items WHERE id=%s", (key,)
            )
            await conn.execute(
                parameter_tag + "UPDATE synthetic_items SET value=value+1 WHERE id=%s",
                (key,),
            )
            await conn.execute(tag + "COMMIT")
        except BaseException:
            await conn.execute(tag + "ROLLBACK")
            raise
        return 4
    await conn.execute(tag + "SELECT pg_sleep(0.05)")
    return 1


async def worker(index, args, ready, start, results):
    identity = index + args.offset
    host = HOSTS[identity % len(HOSTS)]
    database = DATABASES[(identity // len(HOSTS)) % len(DATABASES)]
    rng = random.Random(1000 + identity)
    errors = Counter()
    latencies = Histogram()
    scheduling_lag = Histogram()
    successes = messages = missed = notices = 0

    def notice(_):
        nonlocal notices
        notices += 1

    try:
        conn = await connect(host, database, args.prepared_mode)
    except Exception as exc:
        ready.set_exception(exc)
        raise
    async with conn:
        conn.add_notice_handler(notice)
        ready.set_result(True)
        await start.wait()
        began = args.began
        deadline = began + args.duration
        sequence = 0
        interval = args.clients / args.rate if args.rate else 0
        stable_tag = comment(identity, 0)
        while time.monotonic() < deadline:
            scheduled = (
                began + index / args.rate + sequence * interval
                if interval
                else time.monotonic()
            )
            if scheduled >= deadline:
                break
            if interval:
                delay = scheduled - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                elif -delay > interval:
                    # Shed missed arrivals rather than accumulating an unbounded backlog.
                    skip = int(-delay / interval)
                    missed += skip
                    sequence += skip
                    scheduled += skip * interval
            now = time.monotonic()
            if now >= deadline:
                break
            scheduling_lag.observe(max(0, now - scheduled))
            try:
                key = rng.randint(1, 100000)
                tag = (
                    stable_tag
                    if args.prepared_mode == "named"
                    else comment(identity, sequence)
                )
                messages += await operation(conn, args.profile, rng, tag, key)
                successes += 1
            except psycopg.Error as exc:
                if isinstance(exc, psycopg.ProgrammingError) and exc.sqlstate is None:
                    raise
                errors[getattr(exc, "sqlstate", None) or type(exc).__name__] += 1
                if conn.closed:
                    break
            latencies.observe(time.monotonic() - now)
            sequence += 1
    results.append(
        {
            "client": identity,
            "host": host,
            "database": database,
            "successes": successes,
            "sql_messages": messages,
            "missed_arrivals": missed,
            "notices": notices,
            "errors": dict(errors),
            "latency": latencies,
            "scheduling_lag": scheduling_lag,
        }
    )


async def measure(args):
    loop = asyncio.get_running_loop()
    ready = [loop.create_future() for _ in range(args.clients)]
    start = asyncio.Event()
    results = []
    tasks = [
        asyncio.create_task(worker(i, args, ready[i], start, results))
        for i in range(args.clients)
    ]
    try:
        await asyncio.wait_for(asyncio.gather(*ready), timeout=30)
        if args.start_at:
            if time.time() > args.start_at + 1:
                raise RuntimeError("Load generator missed the common start deadline")
            await asyncio.sleep(max(0, args.start_at - time.time()))
        args.began = time.monotonic()
        start.set()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=args.duration + 40)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = time.monotonic() - args.began
    combined = Histogram()
    lag = Histogram()
    errors = Counter()
    for row in results:
        errors.update(row["errors"])
        for target, key in ((combined, "latency"), (lag, "scheduling_lag")):
            h = row[key]
            target.bins = [a + b for a, b in zip(target.bins, h.bins)]
            target.total += h.total
            target.maximum = max(target.maximum, h.maximum)
            row[key] = h.report()
    total = sum(r["successes"] for r in results)
    report = {
        "profile": args.profile,
        "prepared_mode": args.prepared_mode,
        "duration_seconds": elapsed,
        "clients": args.clients,
        "requested_operations_per_second": args.rate,
        "successes": total,
        "operations_per_second": total / elapsed,
        "sql_messages": sum(r["sql_messages"] for r in results),
        "missed_arrivals": sum(r["missed_arrivals"] for r in results),
        "notices": sum(r["notices"] for r in results),
        "errors": dict(errors),
        "latency": combined.report(),
        "scheduling_lag": lag.report(),
        "workers": results,
    }
    report["histogram_bins"] = combined.bins
    report["scheduling_lag_bins"] = lag.bins
    return report


def shard(options):
    return asyncio.run(measure(argparse.Namespace(**options)))


def merge(reports):
    combined = Histogram()
    lag = Histogram()
    errors = Counter()
    for report in reports:
        errors.update(report["errors"])
        for target, bins, metric in (
            (combined, "histogram_bins", "latency"),
            (lag, "scheduling_lag_bins", "scheduling_lag"),
        ):
            target.bins = [a + b for a, b in zip(target.bins, report[bins])]
            target.total += report[metric]["count"]
            target.maximum = max(target.maximum, report[metric]["max_ms"] / 1000)
    duration = max(r["duration_seconds"] for r in reports)
    successes = sum(r["successes"] for r in reports)
    return {
        "profile": reports[0]["profile"],
        "prepared_mode": reports[0]["prepared_mode"],
        "duration_seconds": duration,
        "clients": sum(r["clients"] for r in reports),
        "requested_operations_per_second": sum(
            r["requested_operations_per_second"] for r in reports
        ),
        "successes": successes,
        "operations_per_second": successes / duration,
        "sql_messages": sum(r["sql_messages"] for r in reports),
        "missed_arrivals": sum(r["missed_arrivals"] for r in reports),
        "notices": sum(r["notices"] for r in reports),
        "errors": dict(errors),
        "latency": combined.report(),
        "scheduling_lag": lag.report(),
        "histogram_bins": combined.bins,
        "scheduling_lag_bins": lag.bins,
        "workers": [worker for r in reports for worker in r["workers"]],
    }


def execute(args):
    asyncio.run(initialize())
    if args.processes == 1:
        args.start_at = None
        report = asyncio.run(measure(args))
    else:
        options = []
        start = time.time() + 15
        offset = 0
        for index in range(args.processes):
            n = args.clients // args.processes + (index < args.clients % args.processes)
            rate = args.rate // args.processes + (index < args.rate % args.processes)
            options.append(
                {
                    **vars(args),
                    "clients": n,
                    "rate": rate,
                    "offset": offset,
                    "start_at": start,
                }
            )
            offset += n
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=args.processes, mp_context=multiprocessing.get_context("spawn")
        ) as pool:
            report = merge(list(pool.map(shard, options)))
    report["generator_processes"] = args.processes
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                k: v
                for k, v in report.items()
                if k not in ("workers", "histogram_bins", "scheduling_lag_bins")
            },
            indent=2,
        )
    )
    if report["errors"] or not report["successes"] or report["notices"]:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        choices=("smoke", "mixed", "throughput", "occupancy"),
        required=True,
    )
    parser.add_argument("--duration", type=int, required=True)
    parser.add_argument("--clients", type=int, required=True)
    parser.add_argument("--rate", type=int, default=0)
    parser.add_argument("--processes", type=int, default=1)
    parser.add_argument(
        "--prepared-mode", choices=("unnamed", "named"), default="unnamed"
    )
    parser.add_argument("--output", type=Path, required=True)
    options = parser.parse_args()
    if options.duration <= 0 or not 1 <= options.clients <= 4000 or options.rate < 0:
        parser.error("Duration must be positive, clients 1..4000, rate nonnegative")
    if not 1 <= options.processes <= min(4, options.clients) or (
        0 < options.rate < options.processes
    ):
        parser.error(
            "Processes must be 1..min(4,clients); positive rate must be >= processes"
        )
    options.offset = 0
    execute(options)
