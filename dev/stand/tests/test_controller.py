"""Fast stand safety tests: no Docker, SSH, or database access."""

import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

STAND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(STAND))
SPEC = importlib.util.spec_from_file_location("stand_controller", STAND / "stand.py")
stand = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(stand)


@unittest.skipIf(os.name == "nt", "The container stand requires a POSIX runtime")
class ControllerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.state = self.root / "state"
        self.state.mkdir()
        self.results = self.root / "results"
        self.patches = patch.multiple(stand, STATE=self.state, RESULTS=self.results)
        self.patches.start()
        self.addCleanup(self.patches.stop)
        self.capture = patch.object(stand, "capture", return_value="")
        self.capture.start()
        self.addCleanup(self.capture.stop)
        self.addCleanup(self.tmp.cleanup)
        self.manifest = {
            "variant": "upstream",
            "image": "pgobs-test:local",
            "image_id": "sha256:" + "a" * 64,
            "workload_image_id": "sha256:" + "c" * 64,
        }
        (self.state / "upstream.json").write_text(json.dumps(self.manifest))
        self.dependencies = {
            name: f"{name}@sha256:" + "b" * 64 for name in stand.IMAGES
        }

    def prepare(self):
        with patch.object(stand, "images", return_value=self.dependencies):
            stand.prepare("upstream")
        return json.loads((self.state / "compose.json").read_text())

    def test_stand_is_isolated_and_resource_bounded(self):
        config = self.prepare()
        self.assertEqual(config["networks"]["stand"], {"internal": True})
        for name, service in config["services"].items():
            with self.subTest(service=name):
                self.assertEqual(
                    service["networks"],
                    ["stand", "management"] if name == "prometheus" else ["stand"],
                )
                self.assertGreater(service["cpus"], 0)
                self.assertIn("mem_limit", service)
                self.assertNotIn("network_mode", service)
                self.assertNotIn("privileged", service)
                for port in service.get("ports", []):
                    self.assertTrue(port.startswith("127.0.0.1:"))
                if name.startswith("db-"):
                    self.assertNotIn("ports", service)
                if name.startswith("proxy-"):
                    self.assertNotIn("ports", service)

    def test_configuration_has_only_synthetic_destinations(self):
        self.prepare()
        config = (self.state / "config/a.ini").read_text()
        self.assertIn("host=db-a", config)
        self.assertIn("host=db-b", config)
        self.assertIn("auth_type=scram-sha-256", config)
        self.assertNotIn("auth_type=trust", config)
        self.assertEqual((self.state / "password").stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.state.stat().st_mode & 0o777, 0o700)
        prom = json.loads((self.state / "config/prometheus.json").read_text())
        relabel = prom["scrape_configs"][0]["metric_relabel_configs"][0]
        self.assertEqual(relabel["source_labels"], ["database"])
        self.assertEqual(relabel["regex"], "(primary|secondary)")

    def test_password_is_stable_between_restarts(self):
        self.prepare()
        first = (self.state / "password").read_text()
        with patch.object(stand, "capture", return_value=""):
            self.prepare()
        self.assertEqual((self.state / "password").read_text(), first)

    def test_config_mount_permissions_do_not_depend_on_host_umask(self):
        previous = os.umask(0o077)
        try:
            self.prepare()
        finally:
            os.umask(previous)
        self.assertEqual((self.state / "config").stat().st_mode & 0o777, 0o755)
        self.assertEqual(
            (self.state / "config/prometheus.json").stat().st_mode & 0o777, 0o644
        )
        self.assertEqual((self.state / "password").stat().st_mode & 0o777, 0o600)

    def test_cannot_switch_variant_while_running(self):
        self.prepare()
        with (
            patch.object(stand, "images", return_value=self.dependencies),
            patch.object(stand, "capture", return_value="running-container"),
            self.assertRaisesRegex(SystemExit, "running"),
        ):
            stand.prepare("upstream")

    def test_cannot_take_over_running_project_without_local_state(self):
        with (
            patch.object(stand, "images", return_value=self.dependencies),
            patch.object(stand, "capture", return_value="another-checkouts-container"),
            self.assertRaisesRegex(SystemExit, "owning checkout"),
        ):
            stand.prepare("upstream")
        self.assertFalse((self.state / "compose.json").exists())

    def test_proxy_uses_recorded_immutable_image(self):
        config = self.prepare()
        for name in ("a", "b"):
            self.assertEqual(
                config["services"][f"proxy-{name}"]["image"], self.manifest["image_id"]
            )
        self.assertEqual(
            config["services"]["load"]["image"], self.manifest["workload_image_id"]
        )

    def test_source_digest_changes_when_header_changes(self):
        for directory in ("src", "include", "lib"):
            (self.root / directory).mkdir()
        for name in (
            "Makefile",
            "meson.build",
            "meson_options.txt",
            "configure.ac",
            "include/a.h",
        ):
            (self.root / name).write_text("original")
        before = stand.source_digest(self.root)
        (self.root / "include/a.h").write_text("changed")
        self.assertNotEqual(before, stand.source_digest(self.root))

    def test_upstream_variant_rejects_modified_source_before_building(self):
        (self.root / "src").mkdir()
        (self.root / "src/client.c").write_text("modified")
        with (
            patch.object(stand, "images", return_value=self.dependencies),
            patch.object(stand, "source_digest", return_value="modified"),
            patch.object(stand, "run") as command,
            self.assertRaisesRegex(SystemExit, "unmodified"),
        ):
            stand.build(SimpleNamespace(source=self.root, variant="upstream"))
        command.assert_not_called()

    def test_observation_cannot_be_enabled_on_upstream(self):
        with (
            patch.object(stand, "images", return_value=self.dependencies),
            self.assertRaisesRegex(SystemExit, "candidate"),
        ):
            stand.prepare("upstream", observe=True)

    def test_workload_stats_cannot_be_requested_from_a_baseline(self):
        self.prepare()
        with self.assertRaisesRegex(SystemExit, "--observe"):
            stand.stats()

    def test_dashboard_authentication_uses_only_the_loopback_endpoint(self):
        self.prepare()
        request = stand.dashboard_request("/api/health")
        self.assertEqual(request.full_url, "http://127.0.0.1:26300/api/health")
        self.assertTrue(request.get_header("Authorization").startswith("Basic "))

    def test_workload_exporter_requires_observation(self):
        with (
            patch.object(stand, "images", return_value=self.dependencies),
            self.assertRaisesRegex(SystemExit, "--observe"),
        ):
            stand.prepare("candidate", workload_exporter=True)

    def test_workload_exporter_requires_a_built_artifact(self):
        (self.state / "candidate.json").write_text(
            json.dumps({**self.manifest, "variant": "candidate"})
        )
        with (
            patch.object(stand, "images", return_value=self.dependencies),
            self.assertRaisesRegex(SystemExit, "metrics-build"),
        ):
            stand.prepare("candidate", observe=True, workload_exporter=True)

    def test_workload_exporter_is_opt_in_and_immutable(self):
        (self.state / "candidate.json").write_text(
            json.dumps({**self.manifest, "variant": "candidate"})
        )
        exporter = {"image_id": "sha256:" + "d" * 64}
        (self.state / "workload-exporter.json").write_text(json.dumps(exporter))
        with patch.object(stand, "images", return_value=self.dependencies):
            stand.prepare("candidate", observe=True, workload_exporter=True)
        config = json.loads((self.state / "compose.json").read_text())
        for name in ("a", "b"):
            service = config["services"][f"exporter-{name}"]
            self.assertEqual(service["image"], exporter["image_id"])
            self.assertIn("--pgBouncer.include-workloads", service["command"])
        active = json.loads((self.state / "active.json").read_text())
        self.assertEqual(active["workload_exporter"], exporter)
        grafana = config["services"]["grafana"]
        self.assertEqual(grafana["ports"], ["127.0.0.1:26300:3000"])
        self.assertEqual(grafana["networks"], ["stand", "management"])
        self.assertEqual(grafana["environment"]["GF_USERS_ALLOW_SIGN_UP"], "false")

    def test_verify_rejects_published_sql_ports(self):
        self.prepare()
        samples = [
            {"metric": {"process": f"proxy-{n}"}, "value": [0, "1"]} for n in ("a", "b")
        ]
        opener = Mock()
        opener.open.side_effect = [
            io.StringIO(json.dumps({"data": {"result": samples}})) for _ in range(3)
        ]
        container = {
            "Name": "/pgobs-proxy-a-1",
            "Image": self.manifest["image_id"],
            "HostConfig": {
                "Memory": 1024,
                "NanoCpus": 1_000_000_000,
                "PortBindings": {"6432/tcp": [{"HostIp": "0.0.0.0"}]},
            },
            "NetworkSettings": {"Networks": {"pgobs_stand": {}}},
        }
        with (
            patch.object(stand, "build_opener", return_value=opener),
            patch.object(stand, "capture", return_value=json.dumps([container])),
            self.assertRaisesRegex(SystemExit, "exclusively isolated"),
        ):
            stand.verify()


if __name__ == "__main__":
    unittest.main()
