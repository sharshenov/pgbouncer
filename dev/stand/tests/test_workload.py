"""Load-driver regression tests; all database operations are faked."""

import importlib.util
import random
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

STAND = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("stand_workload", STAND / "workload.py")
workload = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(workload)


class HistogramTest(unittest.TestCase):
    def test_empty(self):
        self.assertIsNone(workload.Histogram().report()["p99_ms_upper"])

    def test_bounds_and_count(self):
        histogram = workload.Histogram()
        for seconds in (0.000001, 0.001, 1, 30):
            histogram.observe(seconds)
        self.assertEqual(histogram.total, 4)
        self.assertEqual(sum(histogram.bins), 4)
        self.assertGreaterEqual(histogram.percentile_ms(1), 30000)
        self.assertLess(histogram.percentile_ms(1), 33000)

    def test_memory_is_independent_of_sample_count(self):
        histogram = workload.Histogram()
        for _ in range(10000):
            histogram.observe(0.001)
        self.assertEqual(len(histogram.bins), workload.HISTOGRAM_BINS)

    def test_comment_is_production_shaped_and_varies_only_unrelated_trace(self):
        first = workload.comment(1, 1)
        self.assertNotEqual(first, workload.comment(1, 200))
        self.assertIn("%3A%3A", first)
        self.assertIn("team='synthetic-team-1'", first)

    def test_merge_combines_histograms_not_percentiles(self):
        reports = []
        for seconds, count in ((0.001, 100), (0.1, 1)):
            histogram = workload.Histogram()
            for _ in range(count):
                histogram.observe(seconds)
            reports.append(
                {
                    "profile": "smoke",
                    "prepared_mode": "unnamed",
                    "duration_seconds": 1,
                    "clients": 1,
                    "requested_operations_per_second": count,
                    "successes": count,
                    "sql_messages": count,
                    "missed_arrivals": 0,
                    "notices": 0,
                    "errors": {},
                    "latency": histogram.report(),
                    "scheduling_lag": histogram.report(),
                    "histogram_bins": histogram.bins,
                    "scheduling_lag_bins": histogram.bins,
                    "workers": [],
                }
            )
        result = workload.merge(reports)
        self.assertEqual(result["successes"], 101)
        self.assertEqual(result["operations_per_second"], 101)
        self.assertEqual(sum(result["histogram_bins"]), 101)
        self.assertLess(result["latency"]["p99_ms_upper"], 2)


class OperationTest(unittest.IsolatedAsyncioTestCase):
    async def test_connection_uses_the_requested_prepared_mode(self):
        for mode, threshold in (("unnamed", None), ("named", 5)):
            with (
                self.subTest(mode=mode),
                patch.dict(workload.os.environ, {"STAND_PASSWORD": "synthetic"}),
                patch.object(
                    workload.psycopg.AsyncConnection, "connect", new_callable=AsyncMock
                ) as connect,
            ):
                await workload.connect("proxy-a", "primary", mode)
                self.assertEqual(
                    connect.call_args.kwargs["prepare_threshold"], threshold
                )

    async def test_parameterized_tag_escapes_driver_percent_markers(self):
        cursor = Mock()
        cursor.fetchone = AsyncMock(return_value=(1,))
        conn = Mock()
        conn.execute = AsyncMock(return_value=cursor)
        tag = workload.comment(1, 1)
        self.assertEqual(
            await workload.operation(conn, "throughput", random.Random(1), tag, 1), 1
        )
        query, params = conn.execute.call_args.args
        self.assertIn("%%3A%%3A", query)
        self.assertEqual(params, (1,))

    async def test_transaction_reports_all_four_sql_executions(self):
        conn = Mock()
        conn.execute = AsyncMock()
        rng = Mock()
        rng.randrange.return_value = 90
        self.assertEqual(
            await workload.operation(conn, "mixed", rng, workload.comment(1, 1), 1), 4
        )
        self.assertEqual(conn.execute.await_count, 4)
        self.assertTrue(conn.execute.call_args_list[0].args[0].endswith("BEGIN"))
        self.assertTrue(conn.execute.call_args_list[-1].args[0].endswith("COMMIT"))

    async def test_failed_transaction_is_rolled_back(self):
        conn = Mock()
        conn.execute = AsyncMock(
            side_effect=[None, RuntimeError("synthetic failure"), None]
        )
        rng = Mock()
        rng.randrange.return_value = 90
        with self.assertRaisesRegex(RuntimeError, "synthetic failure"):
            await workload.operation(conn, "mixed", rng, workload.comment(1, 1), 1)
        self.assertTrue(conn.execute.call_args_list[-1].args[0].endswith("ROLLBACK"))


if __name__ == "__main__":
    unittest.main()
