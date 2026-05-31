"""Feature flags via environment variables (#104).

Two ways to use:

1. ``is_enabled("forecast")`` — direct check anywhere in the code:

   .. code-block:: python

      if is_enabled("forecast"):
          run_forecast()

2. ``require_flag("forecast")`` — decorator that 404s a Flask route when
   the flag is off:

   .. code-block:: python

      @api.route("/forecast/<table>")
      @require_flag("forecast")
      def forecast_endpoint(table): ...

   404 (not 503 / 501) on purpose: from the outside, a disabled feature
   should look identical to a never-existed endpoint. Scanners won't
   discover the route, monitoring won't alert on legit 503 spam during
   a planned cut-off.

Why ``os.environ`` and not pydantic-settings? Flags churn faster than the
typed config — every new flag would otherwise need a code change to
register. ``FF_<NAME>`` lookups keep flag additions to one env-var line.

Truthy values (case-insensitive): ``1`` / ``true`` / ``yes`` / ``on``.
Anything else (including empty / missing) — flag is OFF.
"""

from __future__ import annotations

import logging
import os
from functools import wraps

from flask import abort

logger = logging.getLogger(__name__)

_TRUTHY = frozenset({"1", "true", "yes", "on"})


def _env_key(flag_name: str) -> str:
    """Normalise a flag name to its env var key. ``forecast`` → ``FF_FORECAST``."""
    return f"FF_{flag_name.strip().upper().replace('-', '_')}"


def is_enabled(flag_name: str) -> bool:
    """Return True if the env var ``FF_<NAME>`` is set to a truthy value.

    Default OFF — a missing env var is the same as ``FF_FORECAST=false``.
    Treating "unknown" as "off" means a never-set flag can't accidentally
    expose half-finished functionality in production.
    """
    raw = os.environ.get(_env_key(flag_name), "").strip().lower()
    return raw in _TRUTHY


def require_flag(flag_name: str):
    """Decorator: 404 the wrapped Flask view when the flag is off.

    Lookup happens on every request — toggling the env var + restarting
    the worker flips the route within seconds. No code change, no
    redeploy required for emergency feature shutdown.
    """
    def decorator(view):
        @wraps(view)
        def wrapper(*args, **kwargs):
            if not is_enabled(flag_name):
                logger.info(
                    "feature-flag '%s' is off; serving 404 for view=%s",
                    flag_name, view.__name__,
                )
                abort(404)
            return view(*args, **kwargs)
        # Surface the flag name on the wrapper so the /admin/feature-flags
        # listing can show which routes are gated by which flag.
        wrapper._feature_flag = flag_name  # type: ignore[attr-defined]
        return wrapper
    return decorator


# Registry of flags we want shown on the admin page. Adding a flag here
# is purely cosmetic — ``is_enabled`` works with any name — but the
# admin UI needs *something* to enumerate. Keep alphabetically sorted.
KNOWN_FLAGS: tuple[str, ...] = (
    "forecast",
)


def snapshot() -> list[dict]:
    """Return the current on/off state of every KNOWN_FLAGS entry.

    Used by ``/admin/feature-flags`` to render the table. We don't read
    every ``FF_*`` env var — that would expose unrelated env entries
    starting with ``FF_``.
    """
    return [
        {
            "name": name,
            "env_key": _env_key(name),
            "enabled": is_enabled(name),
        }
        for name in KNOWN_FLAGS
    ]


__all__ = [
    "KNOWN_FLAGS",
    "is_enabled",
    "require_flag",
    "snapshot",
]
