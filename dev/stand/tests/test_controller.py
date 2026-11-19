"""Fast stand safety tests: no Docker, SSH, or database access."""

import importlib.util
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

STAND = Path(__file__).resolve().parents[1]
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

    def test_password_is_stable_between_restarts(self):
        self.prepare()
        first = (self.state / "password").read_text()
        with patch.object(stand, "capture", return_value=""):
            self.prepare()
        self.assertEqual((self.state / "password").read_text(), first)

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
