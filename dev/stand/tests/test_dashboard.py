"""Synthetic dashboard contract checks; no Grafana or datasource access."""

import importlib.util
import json
import unittest
from pathlib import Path

STAND = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("stand_dashboard", STAND / "dashboard.py")
dashboard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(dashboard)


class DashboardTest(unittest.TestCase):
    def test_dashboard_is_json_safe_and_has_unique_panels(self):
        value = json.loads(json.dumps(dashboard.make_dashboard()))
        ids = [p["id"] for p in value["panels"]]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertFalse(value["editable"])

    def test_consumption_panels_are_gated_on_complete_coverage(self):
        value = dashboard.make_dashboard()
        charts = [p for p in value["panels"] if p["type"] == "timeseries"]
        self.assertGreaterEqual(len(charts), 6)
        for panel in charts:
            for target in panel["targets"]:
                self.assertIn("pgbouncer_workload_collection_success", target["expr"])
                self.assertIn("== 2", target["expr"])
                self.assertIn("and on()", target["expr"])
                self.assertNotIn("or vector(0)", target["expr"].split("and on()")[0])

    def test_live_elapsed_is_not_treated_as_a_counter(self):
        value = dashboard.make_dashboard()
        for panel in value["panels"]:
            for target in panel.get("targets", []):
                self.assertNotIn(
                    "rate(pgbouncer_workload_stats_live_execution", target["expr"]
                )
                self.assertEqual(panel["datasource"]["uid"], "stand-prometheus")

    def test_multi_resource_views_never_drop_resource_identity(self):
        for panel in dashboard.make_dashboard()["panels"]:
            if panel["type"] != "timeseries":
                continue
            for target in panel["targets"]:
                expression = target["expr"].split("and on()")[0]
                self.assertRegex(expression, r"(sum|max) by\(resource")
                self.assertIn("{{resource}}", target["legendFormat"])
