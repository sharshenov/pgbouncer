"""Self-contained Docker stand. Never accepts a database destination."""

import argparse
import base64
import hashlib
import json
import os
import re
import secrets
import subprocess
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import ProxyHandler, Request, build_opener

from dashboard import make_dashboard

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
STATE = HERE / ".state"
RESULTS = HERE / "results"
LOCK = HERE / "images.lock.json"
PROJECT = "pgobs"
IMAGES = {
    "postgres": "postgres:18-bookworm",
    "exporter": "prometheuscommunity/pgbouncer-exporter:v0.9.0",
    "prometheus": "prom/prometheus:v3.5.0",
}
VARIANTS = ("upstream", "candidate", "legacy")
LEGACY_REVISION = "de1e9cb75324a624d18fc3dd5d714d12a8da0db6"
UPSTREAM_REVISION = "ac3bb6d748cefc9470c2c66e061549a59a072ce3"
UPSTREAM_SOURCE_DIGEST = (
    "68bb967ea6a75ccc5fd7c9ccf81faf6e8df7935635a93081e370fd69aa5ed3be"
)


def run(args, **kwargs):
    return subprocess.run([str(x) for x in args], check=True, **kwargs)


def capture(args):
    return run(args, capture_output=True, text=True).stdout.strip()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def dashboard_request(path):
    password = (STATE / "password").read_text().strip()
    token = base64.b64encode(("stand:" + password).encode()).decode()
    return Request(
        "http://127.0.0.1:26300" + path,
        headers={"Authorization": "Basic " + token},
    )


def images():
    if not LOCK.exists():
        raise SystemExit("Run 'lock' first to resolve immutable dependency images.")
    value = json.loads(LOCK.read_text())
    if set(value) != set(IMAGES) or any("@sha256:" not in x for x in value.values()):
        raise SystemExit("Invalid image lock")
    return value


def lock_images():
    if LOCK.exists():
        raise SystemExit("Lock exists; remove it explicitly to refresh dependencies.")
    resolved = {}
    for name, tag in IMAGES.items():
        run(["docker", "pull", tag])
        digests = json.loads(
            capture(
                ["docker", "image", "inspect", tag, "--format", "{{json .RepoDigests}}"]
            )
        )
        resolved[name] = digests[0]
    write_json(LOCK, resolved)


def source_digest(source):
    digest = hashlib.sha256()
    for directory in ("src", "include", "lib"):
        for path in sorted((source / directory).rglob("*")):
            if path.is_file() and ".objs" not in path.parts:
                digest.update(str(path.relative_to(source)).encode() + b"\0")
                digest.update(path.read_bytes())
    for name in ("Makefile", "meson.build", "meson_options.txt", "configure.ac"):
        digest.update(name.encode() + b"\0" + (source / name).read_bytes())
    return digest.hexdigest()


def build(args):
    deps = images()
    source = args.source.resolve()
    if not (source / "src/client.c").exists():
        raise SystemExit("Source must be a PgBouncer checkout")
    digest = source_digest(source)
    if args.variant == "upstream" and digest != UPSTREAM_SOURCE_DIGEST:
        raise SystemExit("Upstream build requires the pinned, unmodified source tree")
    revision = args.revision
    if (source / ".git").exists():
        revision = capture(["git", "-C", source, "rev-parse", "HEAD"])
    binary_revision = {
        "upstream": UPSTREAM_REVISION,
        "legacy": LEGACY_REVISION,
        "candidate": revision,
    }[args.variant]
    target = "legacy-runtime" if args.variant == "legacy" else "runtime"
    tag = f"pgobs-{args.variant}:{digest[:12]}-{args.build_system}"
    run(
        [
            "docker",
            "build",
            "--target",
            target,
            "--build-arg",
            f"POSTGRES_IMAGE={deps['postgres']}",
            "--build-arg",
            f"BUILD_SYSTEM={args.build_system}",
            "--build-arg",
            f"SOURCE_REVISION={binary_revision}",
            "--build-arg",
            f"SOURCE_DIGEST={digest}",
            "-f",
            HERE / "Dockerfile",
            "-t",
            tag,
            source,
        ]
    )
    STATE.mkdir(exist_ok=True)
    manifest = {
        "variant": args.variant,
        "revision": binary_revision,
        "build_context_revision": revision,
        "source_sha256": None if args.variant == "legacy" else digest,
        "build_context_sha256": digest,
        "image": tag,
        "image_id": capture(["docker", "image", "inspect", tag, "--format", "{{.Id}}"]),
        "build_system": args.build_system,
        "dependencies": deps,
        "version": capture(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "--cpus",
                "1",
                "--memory",
                "256m",
                tag,
                "--version",
            ]
        ),
    }
    # Build a reusable test/load image from exactly the same source.
    run(
        [
            "docker",
            "build",
            "--target",
            "workload",
            "--build-arg",
            f"POSTGRES_IMAGE={deps['postgres']}",
            "-f",
            HERE / "Dockerfile",
            "-t",
            "pgobs-workload:local",
            source,
        ]
    )
    manifest["workload_image_id"] = capture(
        ["docker", "image", "inspect", "pgobs-workload:local", "--format", "{{.Id}}"]
    )
    write_json(STATE / f"{args.variant}.json", manifest)


def compose(*args):
    return [
        "docker",
        "compose",
        "--project-name",
        PROJECT,
        "-f",
        STATE / "compose.json",
        *args,
    ]


def build_exporter():
    deps = images()
    directory = HERE / "exporter"
    locked = json.loads((directory / "images.lock.json").read_text())
    if set(locked) != {"go", "upstream_revision"}:
        raise SystemExit("Invalid exporter dependency lock")
    if "@sha256:" not in locked["go"] or not re.fullmatch(
        r"[0-9a-f]{40}", locked["upstream_revision"]
    ):
        raise SystemExit("Exporter inputs must be immutable")
    digest = hashlib.sha256()
    for path in sorted(directory.iterdir()):
        if path.is_file():
            digest.update(path.name.encode() + b"\0" + path.read_bytes())
    source = digest.hexdigest()
    tag = f"pgobs-workload-exporter:{source[:12]}"
    run(
        [
            "docker",
            "build",
            "-f",
            directory / "Dockerfile",
            "--build-arg",
            f"GO_IMAGE={locked['go']}",
            "--build-arg",
            f"EXPORTER_IMAGE={deps['exporter']}",
            "--build-arg",
            f"UPSTREAM_REVISION={locked['upstream_revision']}",
            "--build-arg",
            f"SOURCE_DIGEST={source}",
            "-t",
            tag,
            ROOT,
        ]
    )
    STATE.mkdir(exist_ok=True)
    write_json(
        STATE / "workload-exporter.json",
        {
            "image": tag,
            "image_id": capture(
                ["docker", "image", "inspect", tag, "--format", "{{.Id}}"]
            ),
            "source_sha256": source,
            "upstream_revision": locked["upstream_revision"],
            "go_image": locked["go"],
            "runtime_base": deps["exporter"],
        },
    )


def prepare(variant, observe=False, workload_exporter=False):
    deps = images()
    if observe and variant != "candidate":
        raise SystemExit("Observation settings require a candidate build")
    if workload_exporter and not observe:
        raise SystemExit("Workload exporter qualification requires --observe")
    manifest_path = STATE / f"{variant}.json"
    if not manifest_path.exists():
        raise SystemExit(f"Build '{variant}' first")
    manifest = json.loads(manifest_path.read_text())
    exporter = None
    if workload_exporter:
        exporter_file = STATE / "workload-exporter.json"
        if not exporter_file.exists():
            raise SystemExit("Run metrics-build before selecting the workload exporter")
        exporter = json.loads(exporter_file.read_text())
    running = capture(
        [
            "docker",
            "ps",
            "--quiet",
            "--filter",
            f"label=com.docker.compose.project={PROJECT}",
        ]
    )
    if running:
        raise SystemExit(
            "Stand is running; use its owning checkout to stop it before changing variants"
        )
    STATE.mkdir(exist_ok=True)
    STATE.chmod(0o700)
    RESULTS.mkdir(exist_ok=True)
    password_file = STATE / "password"
    if not password_file.exists():
        password_file.write_text(secrets.token_hex(24))
        password_file.chmod(0o600)
    password = password_file.read_text().strip()
    config = STATE / "config"
    config.mkdir(exist_ok=True)
    (config / "users.txt").write_text(f'"stand" "{password}"\n')
    # Synthetic-only credential. Mount directory requires runtime read access.
    (config / "users.txt").chmod(0o644)
    for name in ("a", "b"):
        (config / f"{name}.ini").write_text(
            "[databases]\n"
            "primary = host=db-a port=5432 dbname=stand\n"
            "secondary = host=db-b port=5432 dbname=stand\n\n"
            "[pgbouncer]\nlisten_addr=0.0.0.0\nlisten_port=6432\n"
            "auth_type=scram-sha-256\nauth_file=/etc/pgbouncer/users.txt\n"
            "admin_users=stand\nstats_users=stand\npool_mode=transaction\n"
            "max_client_conn=4000\ndefault_pool_size=75\nmin_pool_size=0\n"
            "max_prepared_statements=200\nquery_wait_timeout=30\n"
            "ignore_startup_parameters=extra_float_digits\n"
            + ("workload_observation=1\nworkload_stats_max=1000\n" if observe else "")
            + "unix_socket_dir=/tmp\npidfile=/tmp/pgbouncer.pid\n"
            "log_connections=0\nlog_disconnections=0\n"
        )
    services = {}
    for name in ("a", "b"):
        services[f"db-{name}"] = {
            "image": deps["postgres"],
            "environment": {
                "POSTGRES_USER": "stand",
                "POSTGRES_DB": "stand",
                "POSTGRES_PASSWORD": password,
            },
            "command": [
                "postgres",
                "-c",
                "max_connections=400",
                "-c",
                "shared_buffers=512MB",
                "-c",
                "statement_timeout=30000",
                "-c",
                "idle_in_transaction_session_timeout=30000",
            ],
            "volumes": [f"db-{name}:/var/lib/postgresql"],
            "healthcheck": {
                "test": ["CMD", "pg_isready", "-U", "stand", "-d", "stand"],
                "interval": "2s",
                "timeout": "2s",
                "retries": 45,
            },
            "cpus": 3,
            "mem_limit": "3g",
            "shm_size": "1g",
        }
        services[f"proxy-{name}"] = {
            "image": manifest["image_id"],
            "volumes": [f"{config}:/etc/pgbouncer:ro"],
            "command": [f"/etc/pgbouncer/{name}.ini"],
            "depends_on": {
                f"db-{x}": {"condition": "service_healthy"} for x in ("a", "b")
            },
            "cpus": 1,
            "mem_limit": "512m",
            "stop_signal": "SIGINT",
            "stop_grace_period": "45s",
            "healthcheck": {
                "test": [
                    "CMD",
                    "pg_isready",
                    "-h",
                    "127.0.0.1",
                    "-p",
                    "6432",
                    "-U",
                    "stand",
                    "-d",
                    "pgbouncer",
                ],
                "interval": "2s",
                "timeout": "2s",
                "retries": 30,
            },
            "ulimits": {"nofile": {"soft": 16384, "hard": 16384}},
        }
        services[f"exporter-{name}"] = {
            "image": exporter["image_id"] if exporter else deps["exporter"],
            "command": [
                "--pgBouncer.connectionString",
                f"postgres://stand:{password}@proxy-{name}:6432/pgbouncer?sslmode=disable",
                "--log.level=warn",
            ]
            + (["--pgBouncer.include-workloads"] if exporter else []),
            "depends_on": {f"proxy-{name}": {"condition": "service_healthy"}},
            "cpus": 0.25,
            "mem_limit": "128m",
        }
    prom_config = config / "prometheus.json"
    write_json(
        prom_config,
        {
            "global": {"scrape_interval": "5s", "scrape_timeout": "4s"},
            "scrape_configs": [
                {
                    "job_name": "stand",
                    "metric_relabel_configs": [
                        {
                            "source_labels": ["database"],
                            "regex": "(primary|secondary)",
                            "target_label": "resource",
                            "replacement": "synthetic-$1",
                        }
                    ],
                    "static_configs": [
                        {
                            "targets": [f"exporter-{n}:9127"],
                            "labels": {
                                "process": f"proxy-{n}",
                                "fleet": "worker",
                                "stand_variant": variant,
                            },
                        }
                        for n in ("a", "b")
                    ],
                }
            ],
        },
    )
    services["prometheus"] = {
        "image": deps["prometheus"],
        "command": [
            "--config.file=/etc/prometheus/prometheus.yml",
            "--storage.tsdb.retention.time=3d",
        ],
        "volumes": [
            f"{prom_config}:/etc/prometheus/prometheus.yml:ro",
            "metrics:/prometheus",
        ],
        "ports": ["127.0.0.1:26909:9090"],
        "networks": ["stand", "management"],
        "cpus": 0.5,
        "mem_limit": "512m",
    }
    if exporter:
        dashboard_images = json.loads((HERE / "dashboard-images.lock.json").read_text())
        if (
            set(dashboard_images) != {"grafana"}
            or "@sha256:" not in dashboard_images["grafana"]
        ):
            raise SystemExit("Dashboard image must be immutable")
        dashboard = config / "grafana"
        for directory in ("datasources", "dashboards", "panels"):
            (dashboard / directory).mkdir(parents=True, exist_ok=True)
        write_json(
            dashboard / "datasources/stand.yaml",
            {
                "apiVersion": 1,
                "datasources": [
                    {
                        "name": "Synthetic stand",
                        "uid": "stand-prometheus",
                        "type": "prometheus",
                        "access": "proxy",
                        "url": "http://prometheus:9090",
                        "isDefault": True,
                        "editable": False,
                        "jsonData": {"timeInterval": "5s"},
                    }
                ],
            },
        )
        write_json(
            dashboard / "dashboards/stand.yaml",
            {
                "apiVersion": 1,
                "providers": [
                    {
                        "name": "synthetic",
                        "type": "file",
                        "disableDeletion": True,
                        "editable": False,
                        "options": {"path": "/var/lib/grafana/dashboards"},
                    }
                ],
            },
        )
        write_json(dashboard / "panels/workload.json", make_dashboard())
        services["grafana"] = {
            "image": dashboard_images["grafana"],
            "environment": {
                "GF_SECURITY_ADMIN_USER": "stand",
                "GF_SECURITY_ADMIN_PASSWORD": password,
                "GF_USERS_ALLOW_SIGN_UP": "false",
                "GF_ANALYTICS_REPORTING_ENABLED": "false",
                "GF_ANALYTICS_CHECK_FOR_UPDATES": "false",
                "GF_PLUGINS_PREINSTALL_DISABLED": "true",
            },
            "volumes": [
                f"{dashboard / 'datasources'}:/etc/grafana/provisioning/datasources:ro",
                f"{dashboard / 'dashboards'}:/etc/grafana/provisioning/dashboards:ro",
                f"{dashboard / 'panels'}:/var/lib/grafana/dashboards:ro",
            ],
            "networks": ["stand", "management"],
            "ports": ["127.0.0.1:26300:3000"],
            "cpus": 0.5,
            "mem_limit": "512m",
        }
    services["load"] = {
        "image": manifest["workload_image_id"],
        "profiles": ["tools"],
        "entrypoint": ["python3", "/workload.py"],
        "environment": {"STAND_PASSWORD": password},
        "volumes": [
            f"{RESULTS}:/results",
            f"{HERE / 'workload.py'}:/workload.py:ro",
            f"{HERE / 'observe.py'}:/observe.py:ro",
        ],
        "cpus": 4,
        "mem_limit": "2g",
        "user": f"{os.getuid()}:{os.getgid()}",
    }
    for service in services.values():
        service.setdefault("networks", ["stand"])
        service["labels"] = {"app": "pgbouncer-synthetic-stand"}
        service["logging"] = {
            "driver": "json-file",
            "options": {"max-size": "10m", "max-file": "3"},
        }
    # Parent state stays private; synthetic-only config mounts must be readable
    # by their unprivileged container users even under a restrictive host umask.
    config.chmod(0o755)
    for path in config.rglob("*"):
        path.chmod(0o755 if path.is_dir() else 0o644)
    write_json(
        STATE / "compose.json",
        {
            "services": services,
            "networks": {"stand": {"internal": True}, "management": {}},
            "volumes": {name: {} for name in ("db-a", "db-b", "metrics")},
        },
    )
    write_json(
        STATE / "active.json",
        {
            **manifest,
            "observation_enabled": observe,
            "workload_exporter": exporter,
            "grafana_image": dashboard_images["grafana"] if exporter else None,
        },
    )


def workload(args):
    if not (STATE / "active.json").exists():
        raise SystemExit("Run up first")
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    output = f"{args.profile}-{stamp}.json"
    completed = subprocess.run(
        [
            str(x)
            for x in compose(
                "run",
                "--rm",
                "--no-deps",
                "load",
                "--profile",
                args.profile,
                "--duration",
                args.duration,
                "--clients",
                args.clients,
                "--rate",
                args.rate,
                "--output",
                f"/results/{output}",
                "--processes",
                args.processes,
                "--prepared-mode",
                args.prepared_mode,
            )
        ],
        check=False,
    )
    result_path = RESULTS / output
    result = (
        json.loads(result_path.read_text())
        if result_path.exists()
        else {"failure": "Driver exited before producing a workload report"}
    )
    result["driver_exit_code"] = completed.returncode
    result["driver_sha256"] = hashlib.sha256(
        (HERE / "workload.py").read_bytes()
    ).hexdigest()
    result["artifact"] = json.loads((STATE / "active.json").read_text())
    write_json(result_path, result)
    print(result_path)
    completed.check_returncode()


def verify():
    active = json.loads((STATE / "active.json").read_text())
    opener = build_opener(ProxyHandler({}))
    observations = {}
    selector = f'{{job="stand",stand_variant="{active["variant"]}"}}'
    expressions = {
        "up": "up" + selector,
        "pgbouncer_up": "pgbouncer_up" + selector,
        "scrape_age_seconds": "time() - timestamp(pgbouncer_up" + selector + ")",
    }
    if active.get("workload_exporter"):
        expressions.update(
            {
                "workload_collection": "pgbouncer_workload_collection_success"
                + selector,
                "workload_enabled": "pgbouncer_workload_status_enabled" + selector,
                "workload_allocations": "pgbouncer_workload_status_allocation_failures_total"
                + selector,
                "workload_age_seconds": "time() - pgbouncer_workload_last_success_timestamp_seconds"
                + selector,
            }
        )
    for metric, expression in expressions.items():
        url = "http://127.0.0.1:26909/api/v1/query?" + urlencode({"query": expression})
        with opener.open(url, timeout=10) as response:
            samples = json.load(response)["data"]["result"]
        if {s["metric"]["process"] for s in samples} != {"proxy-a", "proxy-b"}:
            raise SystemExit(f"Missing collector targets for {metric}")
        healthy = (
            all(0 <= float(s["value"][1]) <= 15 for s in samples)
            if metric in ("scrape_age_seconds", "workload_age_seconds")
            else all(
                s["value"][1] == ("0" if metric == "workload_allocations" else "1")
                for s in samples
            )
        )
        if not healthy:
            raise SystemExit(f"Unhealthy collector targets for {metric}")
        observations[metric] = samples
    names = [
        f"pgobs-{role}-{n}-1"
        for role in ("db", "proxy", "exporter")
        for n in ("a", "b")
    ]
    containers = json.loads(capture(["docker", "inspect", *names]))
    limits = {}
    for container in containers:
        config = container["HostConfig"]
        networks = container["NetworkSettings"]["Networks"]
        if config["Memory"] <= 0 or config["NanoCpus"] <= 0:
            raise SystemExit("Stand service has no resource limit")
        if set(networks) != {"pgobs_stand"} or config["PortBindings"]:
            raise SystemExit("SQL/traffic services are not exclusively isolated")
        if (
            container["Name"].startswith("/pgobs-proxy-")
            and container["Image"] != active["image_id"]
        ):
            raise SystemExit("Running proxy image does not match the active manifest")
        if (
            container["Name"].startswith("/pgobs-exporter-")
            and active.get("workload_exporter")
            and container["Image"] != active["workload_exporter"]["image_id"]
        ):
            raise SystemExit(
                "Running workload exporter does not match the active manifest"
            )
        limits[container["Name"]] = {
            "memory_bytes": config["Memory"],
            "cpu_quota": config["NanoCpus"] / 1e9,
        }
    report = {
        "variant": active["variant"],
        "collectors": observations,
        "limits": limits,
    }
    if active.get("grafana_image"):
        with opener.open(
            dashboard_request("/api/dashboards/uid/synthetic-workload-observation"),
            timeout=10,
        ) as response:
            dashboard = json.load(response)["dashboard"]
        expected = make_dashboard()

        def queries(value):
            return {
                panel["id"]: [target["expr"] for target in panel.get("targets", [])]
                for panel in value["panels"]
            }

        if dashboard["uid"] != expected["uid"] or queries(dashboard) != queries(
            expected
        ):
            raise SystemExit(
                "Provisioned synthetic dashboard does not match this checkout"
            )
        for panel in dashboard["panels"]:
            for target in panel.get("targets", []):
                expr = target["expr"].replace("$resource", ".*")
                url = "http://127.0.0.1:26909/api/v1/query?" + urlencode(
                    {"query": expr}
                )
                with opener.open(url, timeout=10) as response:
                    if json.load(response).get("status") != "success":
                        raise SystemExit("A provisioned dashboard expression failed")
        report["dashboard"] = {
            "uid": dashboard["uid"],
            "panels": len(dashboard["panels"]),
        }
    write_json(RESULTS / "verification.json", report)
    print(json.dumps(report, indent=2))


def stats():
    manifest = json.loads((STATE / "active.json").read_text())
    if not manifest.get("observation_enabled"):
        raise SystemExit(
            "Start a candidate with --observe before collecting workload statistics"
        )
    raw = capture(
        compose(
            "run", "--rm", "--no-deps", "--entrypoint", "python3", "load", "/observe.py"
        )
    )
    result = json.loads(raw)
    result["artifact"] = manifest
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    write_json(RESULTS / f"observation-{stamp}.json", result)
    print(json.dumps(result, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("lock")
    sub.add_parser("metrics-build")
    p = sub.add_parser("build")
    p.add_argument("--variant", choices=VARIANTS, required=True)
    p.add_argument("--source", type=Path, default=ROOT)
    p.add_argument("--revision", default="unknown")
    p.add_argument("--build-system", choices=("autoconf", "meson"), default="autoconf")
    p = sub.add_parser("up")
    p.add_argument("--variant", choices=VARIANTS, required=True)
    p.add_argument("--observe", action="store_true")
    p.add_argument("--workload-exporter", action="store_true")
    p = sub.add_parser("down")
    p.add_argument("--destroy-data", action="store_true")
    sub.add_parser("status")
    sub.add_parser("verify")
    sub.add_parser("stats")
    p = sub.add_parser("run")
    p.add_argument(
        "--profile",
        choices=("smoke", "mixed", "throughput", "occupancy"),
        default="smoke",
    )
    p.add_argument("--duration", type=int, default=30)
    p.add_argument("--clients", type=int, default=16)
    p.add_argument(
        "--rate",
        type=int,
        default=0,
        help="Total scheduled operations/s; 0 is closed-loop",
    )
    p.add_argument("--processes", type=int, default=1, choices=range(1, 5))
    p.add_argument("--prepared-mode", choices=("unnamed", "named"), default="unnamed")
    p = sub.add_parser("test")
    p.add_argument("--build-system", choices=("autoconf", "meson"), default="autoconf")
    args = parser.parse_args()
    if args.action == "lock":
        lock_images()
    elif args.action == "metrics-build":
        build_exporter()
    elif args.action == "build":
        build(args)
    elif args.action == "up":
        prepare(args.variant, args.observe, args.workload_exporter)
        run(compose("up", "-d", "--wait", "--wait-timeout", "180"))
    elif args.action == "down":
        run(
            compose(
                "down",
                "--remove-orphans",
                *(["--volumes"] if args.destroy_data else []),
            )
        )
    elif args.action == "status":
        run(compose("ps"))
    elif args.action == "verify":
        verify()
    elif args.action == "stats":
        stats()
    elif args.action == "run":
        workload(args)
    elif args.action == "test":
        deps = images()
        run(
            [
                "docker",
                "build",
                "--target",
                "build",
                "--build-arg",
                f"POSTGRES_IMAGE={deps['postgres']}",
                "--build-arg",
                f"BUILD_SYSTEM={args.build_system}",
                "-f",
                HERE / "Dockerfile",
                "-t",
                f"pgobs-test:{args.build_system}",
                ROOT,
            ]
        )
        command = (
            ["make", "check", "CONCURRENCY=4"]
            if args.build_system == "autoconf"
            else ["meson", "test", "-C", "build", "--print-errorlogs"]
        )
        run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "--cpus",
                "4",
                "--memory",
                "4g",
                "--env",
                "PYTEST_XDIST_AUTO_NUM_WORKERS=4",
                "--entrypoint",
                "",
                f"pgobs-test:{args.build_system}",
                *command,
            ]
        )


if __name__ == "__main__":
    main()
