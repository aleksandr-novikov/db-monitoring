"""Prometheus instrumentation (#101).

Exposes ``GET /metrics`` in the standard text exposition format and the
collector primitives the rest of the codebase increments:

- ``http_requests_total{method,endpoint,status}`` — bumped via Flask
  ``before_request`` / ``after_request`` hooks installed in
  :func:`install_http_instrumentation`.
- ``http_request_duration_seconds`` — histogram observed in the same hooks.
- ``collector_runs_total{result}`` — incremented from
  ``collectors/per_project.py::collect_for_connection`` on success / error.
- ``failed_login_attempts_total`` — incremented from
  ``app/auth.py::login`` on wrong password / unknown email.

We use the default global ``REGISTRY`` so a future ``prometheus-multiproc``
swap (for gunicorn -w N) only changes the registry, not the call sites.
"""

from __future__ import annotations

import logging
import time

from flask import Response, g, request
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    generate_latest,
)

logger = logging.getLogger(__name__)


# ── Counters / histograms ──────────────────────────────────────────────────

http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests served, partitioned by method, endpoint and status.",
    labelnames=("method", "endpoint", "status"),
)

# Bucket boundaries cover the realistic range for a Flask app: <10ms (cached
# lookups), 25/50/100ms (typical DB-roundtrip), 250/500ms (slower queries),
# 1/2.5/5/10s (anomalous; aim to alert on these). Default prometheus-client
# buckets stop at 10s which is fine for our SLO ceiling.
http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency (seconds), by method + endpoint.",
    labelnames=("method", "endpoint"),
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

collector_runs_total = Counter(
    "collector_runs_total",
    "Per-connection collector ticks, partitioned by outcome.",
    labelnames=("result",),  # "ok" | "error"
)

failed_login_attempts_total = Counter(
    "failed_login_attempts_total",
    "Failed /auth/login attempts (wrong password or unknown email).",
)


# ── HTTP hooks ─────────────────────────────────────────────────────────────


def _start_timer() -> None:
    """Record start time so after_request can compute latency.

    Using ``g`` keeps it request-scoped without polluting ``request``.
    """
    g._prom_start_time = time.perf_counter()


def _record_response(response):
    """Bump http_requests_total / http_request_duration_seconds.

    ``request.endpoint`` is None for 404s on unknown routes — we substitute
    a literal so the cardinality doesn't explode with the request path
    (which can be arbitrary user-controlled garbage).
    """
    endpoint = request.endpoint or "unknown"
    method = request.method
    status = str(response.status_code)
    http_requests_total.labels(
        method=method, endpoint=endpoint, status=status,
    ).inc()
    start = getattr(g, "_prom_start_time", None)
    if start is not None:
        http_request_duration_seconds.labels(
            method=method, endpoint=endpoint,
        ).observe(time.perf_counter() - start)
    return response


def install_http_instrumentation(app) -> None:
    """Wire the request-level Counter / Histogram into a Flask app."""
    app.before_request(_start_timer)
    app.after_request(_record_response)


# ── /metrics endpoint ──────────────────────────────────────────────────────

def metrics_response() -> Response:
    """Render the current registry in Prometheus text exposition format.

    No CSRF (would be hostile to scrapers), no auth (the convention is that
    the endpoint sits behind a network ACL — exposing /metrics behind login
    breaks every Prometheus deployment). Rate-limited at the route level
    in app.app to discourage someone using it as an infinite-pull DoS.

    Note: ``CONTENT_TYPE_LATEST`` already encodes the charset; we pass it
    via ``content_type=`` (verbatim) rather than ``mimetype=`` to avoid
    Flask appending a second ``charset=utf-8`` that some strict scrapers
    refuse to parse.
    """
    return Response(generate_latest(), content_type=CONTENT_TYPE_LATEST)


__all__ = [
    "collector_runs_total",
    "failed_login_attempts_total",
    "http_request_duration_seconds",
    "http_requests_total",
    "install_http_instrumentation",
    "metrics_response",
]
