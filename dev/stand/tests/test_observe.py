"""Observation-reader boundary tests; no Docker or database access."""

import importlib.util
import json
import os
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch

STAND = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("stand_observe", STAND / "observe.py")
observe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(observe)


class ObserveTest(unittest.TestCase):
    def test_integral_numeric_counters_are_json_safe_without_precision_loss(self):
        result = observe.integer_counters(
            [{"completed": Decimal(18446744073709551615), "team": "synthetic"}]
        )
        self.assertEqual(
            json.loads(json.dumps(result))[0]["completed"], 18446744073709551615
        )

    def test_invalid_numeric_counters_fail_collection(self):
        for value in ("0.5", "NaN", "Infinity"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                observe.integer_counters([{"completed": Decimal(value)}])

    def test_collection_only_uses_fixed_stand_admin_targets(self):
        conn = Mock()
        conn.execute.return_value.fetchall.return_value = [{"enabled": Decimal(1)}]
        context = Mock()
        context.__enter__ = Mock(return_value=conn)
        context.__exit__ = Mock(return_value=None)
        with (
            patch.dict(os.environ, {"STAND_PASSWORD": "synthetic"}),
            patch.object(observe.psycopg, "connect", return_value=context) as connect,
        ):
            result = observe.collect()
        self.assertEqual(set(result["processes"]), {"proxy-a", "proxy-b"})
        self.assertEqual(
            [call.kwargs["host"] for call in connect.call_args_list],
            ["proxy-a", "proxy-b"],
        )
        self.assertTrue(
            all(call.kwargs["dbname"] == "pgbouncer" for call in connect.call_args_list)
        )
        self.assertEqual(
            [call.args[0] for call in conn.execute.call_args_list],
            [
                "SHOW WORKLOAD_STATUS",
                "SHOW WORKLOAD_STATS",
                "SHOW WORKLOAD_STATUS",
                "SHOW WORKLOAD_STATS",
            ],
        )
