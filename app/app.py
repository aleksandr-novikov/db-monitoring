import os
import re

from flask import Flask, jsonify, redirect
from flask_wtf.csrf import CSRFProtect

from .admin import bp as admin_bp
from .api import api
from .auth import _abort_if_unauthenticated, limiter, login_manager
from .auth import bp as auth_bp
from .config import settings
from .connections import bp as connections_bp
from .dashboard import bp as dashboard_bp
from .dashboard import status_class
from .health import build_health_payload
from .projects import bp as projects_bp
from .projects import load_current_project_into_g
from .security import init_logging_filter

_ISO_TS_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})"
)


def _fmt_iso_in_text(text: str) -> str:
    """Replace ISO 8601 timestamps inside a string with 'YYYY-MM-DD HH:MM UTC'."""
    if not text:
        return text
    return _ISO_TS_RE.sub(lambda m: m.group()[:16].replace("T", " ") + " UTC", text)


_logging_filter_installed = False


def _ensure_dsn_logging_filter() -> None:
    """Install the DSN-scrubbing log filter once per process.

    Calling ``init_logging_filter`` on every ``create_app()`` (which tests
    do dozens of times) would re-attach the same filter on every call;
    benign but wasteful and clutters introspection. Idempotent guard.
    """
    global _logging_filter_installed
    if not _logging_filter_installed:
        init_logging_filter()
        _logging_filter_installed = True


def _maybe_install_proxy_fix(app: Flask) -> None:
    """Wire ``werkzeug.middleware.proxy_fix.ProxyFix`` when running behind a
    reverse proxy (nginx, Cloudflare, …). Without it, ``request.remote_addr``
    is the proxy's IP — the per-IP rate limiter (#56) would collapse every
    user into one shared bucket. Gated on the ``TRUST_PROXY`` env var so
    operators have to opt in deliberately: trusting forwarded headers
    when there's no actual proxy lets attackers spoof their IP via
    ``X-Forwarded-For``.
    """
    trust = (os.environ.get("TRUST_PROXY") or "").lower() in ("1", "true", "yes")
    if not trust:
        return
    from werkzeug.middleware.proxy_fix import ProxyFix

    # x_for=1 → trust exactly one proxy hop. Bump per layer; never higher
    # than the actual proxy chain or X-Forwarded-For becomes spoofable.
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)


def create_app(config: dict | None = None):
    _ensure_dsn_logging_filter()
    app = Flask(__name__)
    app.config["SECRET_KEY"] = settings.SECRET_KEY
    app.config["COLLECT_INTERVAL_MINUTES"] = settings.COLLECT_INTERVAL_MINUTES
    # Session cookie hardening (#49). SameSite=Lax is what makes the
    # CSRF-exempt /api and /admin POSTs safe — without it, Flask defaults
    # to no SameSite attribute and a cross-site form POST would carry the
    # session cookie. HttpOnly blocks JS read; Secure is gated on
    # TESTING so the local dev server (HTTP) can still set the cookie.
    app.config.setdefault("SESSION_COOKIE_SAMESITE", "Lax")
    app.config.setdefault("SESSION_COOKIE_HTTPONLY", True)
    app.jinja_env.filters["status_class"] = status_class
    app.jinja_env.filters["fmt_iso_in_text"] = _fmt_iso_in_text

    if config:
        app.config.update(config)

    # Under TESTING, disable login_required gating and CSRF so existing
    # dashboard/admin/api tests that don't care about auth keep working.
    # Tests that *do* exercise the auth flow toggle these flags explicitly.
    if app.config.get("TESTING"):
        app.config.setdefault("LOGIN_DISABLED", True)
        app.config.setdefault("WTF_CSRF_ENABLED", False)
    else:
        # Only require HTTPS for the session cookie outside TESTING — the
        # local dev server (HTTP) wouldn't be able to set the cookie at all
        # with Secure=True.
        app.config.setdefault("SESSION_COOKIE_SECURE", True)

    # In TESTING, disable rate limiting so existing auth tests can issue
    # many login attempts in a row without hitting 429. Tests that actually
    # exercise the rate limit flip this back on explicitly.
    if app.config.get("TESTING"):
        app.config.setdefault("RATELIMIT_ENABLED", False)
    else:
        # Production deploys (gunicorn -w N) need a shared backend so the
        # per-IP counter is global, not per-worker. Default to in-memory
        # for single-process dev/Docker; override via env for prod Redis.
        app.config.setdefault(
            "RATELIMIT_STORAGE_URI",
            os.environ.get("RATELIMIT_STORAGE_URI", "memory://"),
        )

    # Flask-Login + Flask-WTF (#49) + Flask-Limiter (#56).
    login_manager.init_app(app)
    limiter.init_app(app)
    _maybe_install_proxy_fix(app)
    csrf = CSRFProtect(app)
    # Exempt the JSON API and admin endpoints from CSRF — they're called
    # from curl/scripts, not browser forms. Cross-site POSTs would carry
    # the session cookie only if SESSION_COOKIE_SAMESITE allows it; we
    # set it to "Lax" above, which blocks cross-site form POSTs.
    csrf.exempt(api)
    csrf.exempt(admin_bp)

    app.register_blueprint(auth_bp)
    app.register_blueprint(api)
    app.register_blueprint(admin_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(projects_bp)
    app.register_blueprint(connections_bp)

    # Gate the HTML surface (dashboard + admin + projects) behind login.
    # Done as an app-level before_request with path-based dispatch (not a
    # blueprint hook) because blueprints are module-level objects shared
    # across `create_app()` calls — Flask refuses a second `before_request`
    # once they've been registered once.
    _PROTECTED_PREFIXES = ("/dashboard", "/admin", "/projects")
    @app.before_request
    def _require_login_for_html():
        from flask import request
        if request.path.startswith(_PROTECTED_PREFIXES):
            return _abort_if_unauthenticated()
        return None

    # Populate g.current_project on every request for authenticated users.
    # Runs *after* the login gate above (Flask runs before_request hooks in
    # registration order), so anonymous requests never hit the DB lookup.
    app.before_request(load_current_project_into_g)

    @app.route("/")
    def index():
        # Use url_for so we hit the canonical /dashboard/ trailing-slash
        # form directly instead of /dashboard → 308 → /dashboard/.
        from flask import url_for
        return redirect(url_for("dashboard.overview"))

    @app.route("/healthz")
    @limiter.exempt
    def health():
        """Per-dependency health probe with optional strict mode (#100).

        Default → 503 iff any dependency is ``down``.
        ``?strict=true`` → also 503 iff any dependency is ``n/a`` (use this
        on Kubernetes liveness probes).
        """
        from flask import request as flask_request
        strict = flask_request.args.get("strict", "").lower() in ("1", "true", "yes")
        payload, status_code = build_health_payload(
            strict=strict,
            ratelimit_storage_uri=app.config.get("RATELIMIT_STORAGE_URI", "memory://"),
        )
        return jsonify(payload), status_code

    # In debug mode the Werkzeug reloader forks the process; only start
    # the scheduler in the child (worker) process, not the parent.
    if not app.config.get("TESTING") and (
        not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    ):
        from collectors.scheduler import start_scheduler
        start_scheduler(app)

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(
        debug=os.environ.get("FLASK_DEBUG", "1") == "1",
        host=os.environ.get("HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", 5001)),
    )
