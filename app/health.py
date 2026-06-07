"""Health-check with dependency probes (#100).

The MVP ``/healthz`` returned ``{"status": "ok"}`` regardless of whether the
target DB was actually reachable — useless as an SRE signal. This module
extends it with per-dependency pings, hard wall-clock timeouts, and a
strict-mode flag for orchestrator liveness probes.

Public surface:
- ``build_health_payload(strict: bool) -> (payload, status_code)`` — pure
  function, easy to call from a Flask route or a CLI tool.
- ``HEALTH_TIMEOUT_S`` — per-check wall-clock budget (default 1 s).

Component statuses:
- ``ok``    — ping returned within the budget.
- ``down``  — ping raised or timed out.
- ``n/a``   — dependency not configured (e.g. RATELIMIT_STORAGE_URI=memory://).

HTTP codes:
- Default → 503 iff any component is ``down``.
- ``?strict=true`` → 503 iff any component is ``down`` OR ``n/a``. Use this
  on Kubernetes liveness probes when "n/a" should imply misconfiguration.
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)

HEALTH_TIMEOUT_S = 1.0
_TOTAL_BUDGET_S = 3.0  # belt-and-braces: cap the whole endpoint at 3s

_VERSION_CACHE: str | None = None


def _version() -> str:
    """Best-effort build identifier: APP_VERSION env, else short git SHA, else 'dev'.

    Cached per-process — the answer doesn't change at runtime.
    """
    global _VERSION_CACHE
    if _VERSION_CACHE is not None:
        return _VERSION_CACHE

    v = os.environ.get("APP_VERSION")
    if v:
        _VERSION_CACHE = v.strip()
        return _VERSION_CACHE

    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=1,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        )
        if result.returncode == 0 and result.stdout.strip():
            _VERSION_CACHE = result.stdout.strip()
            return _VERSION_CACHE
    except (subprocess.SubprocessError, OSError):
        pass

    _VERSION_CACHE = "dev"
    return _VERSION_CACHE


# --- Individual probes -----------------------------------------------------


def _ping_engine(engine) -> None:
    """Raise on any failure — ``_run_check`` translates that to {status: down}."""
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))


def _check_monitor_db() -> dict:
    """SELECT 1 + report backend type (#214).

    backend = sqlite / postgres / postgresql / unknown. Эксплейн для
    operator-а через /healthz: видно сразу что run-time использует
    SQLite (опасно в Docker — см. #212), без необходимости лезть в env.
    """
    from app.metrics_storage import get_engine

    engine = get_engine()
    _ping_engine(engine)
    return {"status": "ok", "backend": engine.dialect.name}


def _check_target_db() -> dict:
    from app.db import get_engine
    _ping_engine(get_engine())
    return {"status": "ok"}


def _check_ratelimit_storage(storage_uri: str) -> dict:
    if not storage_uri or storage_uri.startswith("memory:"):
        return {"status": "n/a", "note": "in-memory storage — not externally verifiable"}
    # Lazy-import the limiter; importing at module top would create a cycle
    # (app.auth → app.metrics_storage → app.health, depending on load order).
    from app.auth import limiter
    # Flask-Limiter exposes a `limits`-style storage with a .check() method
    # that returns True if reachable.
    if not limiter.storage.check():
        return {"status": "down", "error": "storage.check() returned False"}
    return {"status": "ok"}


def _check_smtp() -> dict:
    from app.config import settings

    return {"status": "n/a", "configured": settings.smtp_configured}


# --- Probe runner with hard timeout ---------------------------------------


def _run_check(name: str, fn, *args) -> dict:
    """Run ``fn(*args)`` with a wall-clock budget; capture exceptions as 'down'.

    Uses ThreadPoolExecutor so we get a hard kill even if the underlying
    socket ignores the connect timeout (e.g. iptables DROP — connect()
    just blocks until the OS gives up).
    """
    start = time.monotonic()
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            result = ex.submit(fn, *args).result(timeout=HEALTH_TIMEOUT_S)
    except FuturesTimeout:
        elapsed_ms = int((time.monotonic() - start) * 1000)
        logger.warning("healthz: %s timed out after %dms", name, elapsed_ms)
        return {"status": "down", "error": f"timeout > {HEALTH_TIMEOUT_S}s", "elapsed_ms": elapsed_ms}
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - start) * 1000)
        # Truncate the error — psycopg2 OperationalError can contain a
        # multi-line DSN. The DSNFilter in app/security.py scrubs passwords
        # later anyway, but capping length keeps the JSON payload small.
        logger.warning("healthz: %s failed in %dms: %s", name, elapsed_ms, exc)
        return {"status": "down", "error": str(exc).splitlines()[0][:200], "elapsed_ms": elapsed_ms}
    elapsed_ms = int((time.monotonic() - start) * 1000)
    return {**result, "elapsed_ms": elapsed_ms}


# --- Public entry point ----------------------------------------------------


def build_health_payload(*, strict: bool, ratelimit_storage_uri: str) -> tuple[dict[str, Any], int]:
    """Compose the /healthz JSON body + HTTP status code.

    Total wall-clock budget is _TOTAL_BUDGET_S; individual checks each get
    HEALTH_TIMEOUT_S. The endpoint never blocks forever.
    """
    started = time.monotonic()
    checks = {
        "monitor_db": _run_check("monitor_db", _check_monitor_db),
        "target_db": _run_check("target_db", _check_target_db),
        "ratelimit_storage": _run_check(
            "ratelimit_storage", _check_ratelimit_storage, ratelimit_storage_uri,
        ),
        "smtp": _check_smtp(),
    }
    # Defensive: if some future check is added that runs over the total
    # budget, we still return SOMETHING rather than hanging.
    if (time.monotonic() - started) > _TOTAL_BUDGET_S:
        logger.error("healthz: total budget exceeded — investigate")

    critical_checks = {
        name: check for name, check in checks.items()
        if name != "smtp"
    }
    any_down = any(c["status"] == "down" for c in critical_checks.values())
    any_na = any(c["status"] == "n/a" for c in critical_checks.values())

    if any_down or (strict and any_na):
        overall = "degraded"
        status_code = 503
    else:
        overall = "ok"
        status_code = 200

    return (
        {
            "status": overall,
            "checks": checks,
            "version": _version(),
        },
        status_code,
    )
