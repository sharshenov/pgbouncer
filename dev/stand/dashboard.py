"""Provisioned, synthetic-only observation dashboard."""

HEALTHY_TARGETS = """(
  up{job="stand",stand_variant="candidate"} == 1
) and on(instance,job,process) (
  pgbouncer_workload_collection_success == 1
) and on(instance,job,process) (
  pgbouncer_workload_status_enabled == 1
) and on(instance,job,process) (
  pgbouncer_workload_status_allocation_failures_total == 0
) and on(instance,job,process) (
  time() - pgbouncer_workload_last_success_timestamp_seconds < 15
)"""
HEALTHY_COUNT = f"(count({HEALTHY_TARGETS}) or vector(0))"
COMPLETE_COVERAGE = f"and on() ({HEALTHY_COUNT} == 2)"
SELECTOR = '{job="stand",stand_variant="candidate",resource=~"$resource"}'
UNKNOWN_TEAM = '{job="stand",stand_variant="candidate",resource=~"$resource",attribution="tagged",team="unknown"}'
UNKNOWN_CALLER = '{job="stand",stand_variant="candidate",resource=~"$resource",attribution="tagged",caller="unknown"}'
DATASOURCE = {"type": "prometheus", "uid": "stand-prometheus"}


def series(panel_id, title, expressions, unit, x, y):
    return {
        "id": panel_id,
        "title": title,
        "type": "timeseries",
        "datasource": DATASOURCE,
        "gridPos": {"x": x, "y": y, "w": 12, "h": 7},
        "targets": [
            {
                "refId": chr(65 + i),
                "expr": expr + " " + COMPLETE_COVERAGE,
                "legendFormat": legend,
                "range": True,
            }
            for i, (expr, legend) in enumerate(expressions)
        ],
        "fieldConfig": {"defaults": {"unit": unit, "min": 0}},
        "options": {
            "legend": {
                "displayMode": "table",
                "placement": "bottom",
                "calcs": ["lastNotNull", "max"],
            },
            "tooltip": {"mode": "multi"},
        },
    }


def make_dashboard():
    stats = "pgbouncer_workload_stats_"
    panels = [
        {
            "id": 1,
            "title": "Synthetic observation — not CPU/IO isolation",
            "type": "text",
            "gridPos": {"x": 0, "y": 0, "w": 24, "h": 4},
            "options": {
                "mode": "markdown",
                "content": (
                    "Synthetic data and traffic only. DB wall time includes proxy/transfer effects and lock waits; "
                    "the largest consumer is not automatically the root cause.\n\n"
                    "Charts require **both healthy collectors**. Missing/failed collection is not zero consumption. "
                    "Counter rates need multiple scrapes and do not recover increments before a new series' first sample; "
                    "use lifetime counters and live activity alongside them. Live elapsed time is a gauge, not a counter. "
                    "'Tagged' means well-formed labels, not verified ownership; literal unknown tags remain unknown."
                ),
            },
        },
        {
            "id": 2,
            "title": "Healthy observed proxies (expected 2)",
            "type": "stat",
            "datasource": DATASOURCE,
            "gridPos": {"x": 0, "y": 4, "w": 8, "h": 4},
            "targets": [{"refId": "A", "expr": HEALTHY_COUNT, "instant": True}],
            "fieldConfig": {
                "defaults": {
                    "unit": "short",
                    "min": 0,
                    "max": 2,
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "red", "value": None},
                            {"color": "green", "value": 2},
                        ],
                    },
                }
            },
        },
        {
            "id": 3,
            "title": "Oldest successful collection age",
            "type": "stat",
            "datasource": DATASOURCE,
            "gridPos": {"x": 8, "y": 4, "w": 8, "h": 4},
            "targets": [
                {
                    "refId": "A",
                    "expr": "max(time() - pgbouncer_workload_last_success_timestamp_seconds)",
                    "instant": True,
                }
            ],
            "fieldConfig": {
                "defaults": {
                    "unit": "s",
                    "min": 0,
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "green", "value": None},
                            {"color": "red", "value": 15},
                        ],
                    },
                }
            },
        },
        {
            "id": 100,
            "title": "Allocation faults since restart",
            "type": "stat",
            "datasource": DATASOURCE,
            "gridPos": {"x": 16, "y": 4, "w": 8, "h": 4},
            "targets": [
                {
                    "refId": "A",
                    "expr": 'sum(pgbouncer_workload_status_allocation_failures_total{job="stand",stand_variant="candidate"})',
                    "instant": True,
                }
            ],
            "fieldConfig": {
                "defaults": {
                    "unit": "short",
                    "min": 0,
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "green", "value": None},
                            {"color": "red", "value": 1},
                        ],
                    },
                }
            },
        },
    ]
    specs = [
        (
            "Top active wall-time rates (DB-seconds / second)",
            [
                (
                    f"topk(10, sum by(resource,attribution,team,caller) (rate({stats}execution_seconds_total{SELECTOR}[1m])))",
                    "{{resource}} | {{attribution}}: {{team}} / {{caller}}",
                ),
            ],
            "short",
        ),
        (
            "Running versus in-flight exchanges",
            [
                (
                    f"sum by(resource) ({stats}running{SELECTOR})",
                    "{{resource}} running estimate",
                ),
                (
                    f"sum by(resource) ({stats}inflight{SELECTOR})",
                    "{{resource}} in-flight, including pipeline wait",
                ),
            ],
            "short",
        ),
        (
            "Live elapsed execution — not a rate",
            [
                (
                    f"topk(10, sum by(resource,attribution,team,caller) ({stats}live_execution_seconds{SELECTOR}))",
                    "{{resource}} | {{attribution}}: {{team}} / {{caller}}",
                ),
            ],
            "s",
        ),
        (
            "Transaction/backend occupancy",
            [
                (
                    f"sum by(resource) ({stats}transactions_inflight{SELECTOR})",
                    "{{resource}} open occupancy episodes",
                ),
                (
                    f"sum by(resource) (rate({stats}transaction_seconds_total{SELECTOR}[1m]))",
                    "{{resource}} occupancy seconds / second",
                ),
            ],
            "short",
        ),
        (
            "Pool versus pipeline wait rates",
            [
                (
                    f"sum by(resource) (rate({stats}pool_wait_seconds_total{SELECTOR}[1m]))",
                    "{{resource}} pool wait seconds / second",
                ),
                (
                    f"sum by(resource) (rate({stats}pipeline_wait_seconds_total{SELECTOR}[1m]))",
                    "{{resource}} pipeline wait seconds / second",
                ),
            ],
            "short",
        ),
        (
            "Tag parsing coverage — completed exchange rates",
            [
                (
                    f"sum by(resource,attribution) (rate({stats}completed_total{SELECTOR}[1m]))",
                    "{{resource}} | {{attribution}}",
                ),
            ],
            "short",
        ),
        (
            "Application unknown owner/caller — wall-time rates",
            [
                (
                    (
                        f"sum by(resource,team,caller) (rate({stats}execution_seconds_total{UNKNOWN_TEAM}[1m]) "
                        f"or rate({stats}execution_seconds_total{UNKNOWN_CALLER}[1m]))"
                    ),
                    "{{resource}} | {{team}} / {{caller}}",
                ),
            ],
            "short",
        ),
        (
            "Lifetime completed exchanges by attribution",
            [
                (
                    f"sum by(resource,attribution) ({stats}completed_total{SELECTOR})",
                    "{{resource}} | {{attribution}}",
                ),
            ],
            "short",
        ),
        (
            "Oldest running exchange by caller",
            [
                (
                    f"topk(10, max by(resource,attribution,team,caller) ({stats}oldest_running_seconds{SELECTOR}))",
                    "{{resource}} | {{attribution}}: {{team}} / {{caller}}",
                ),
            ],
            "s",
        ),
        (
            "Lifetime observed DB-seconds — not windowed consumption",
            [
                (
                    f"topk(10, sum by(resource,attribution,team,caller) ({stats}execution_seconds_total{SELECTOR}))",
                    "{{resource}} | {{attribution}}: {{team}} / {{caller}}",
                ),
            ],
            "s",
        ),
    ]
    for index, (title, expressions, unit) in enumerate(specs):
        panels.append(
            series(
                index + 4,
                title,
                expressions,
                unit,
                (index % 2) * 12,
                8 + (index // 2) * 7,
            )
        )
    return {
        "uid": "synthetic-workload-observation",
        "title": "PgBouncer Workload Observation — Synthetic",
        "schemaVersion": 39,
        "version": 1,
        "editable": False,
        "tags": ["synthetic", "pgbouncer", "workload"],
        "timezone": "utc",
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "templating": {
            "list": [
                {
                    "name": "resource",
                    "label": "Synthetic resource",
                    "type": "query",
                    "datasource": DATASOURCE,
                    "query": "label_values(pgbouncer_workload_stats_completed_total, resource)",
                    "refresh": 1,
                    "multi": True,
                    "includeAll": True,
                    "allValue": ".*",
                    "current": {"text": "All", "value": "$__all"},
                    "allowCustomValue": False,
                }
            ]
        },
        "panels": panels,
    }
