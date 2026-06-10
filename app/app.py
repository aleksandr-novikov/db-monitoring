import logging
import os
import re
from datetime import UTC, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from flask import Flask, jsonify, redirect, render_template
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
from .instrumentation import install_http_instrumentation, metrics_response
from .logging_setup import configure_logging
from .projects import bp as projects_bp
from .projects import invites_bp, load_current_project_into_g
from .security import init_logging_filter
from .sentry import init_sentry
from .settings import bp as settings_bp

_ISO_TS_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})"
)


def _fmt_iso_in_text(text: str) -> str:
    """Replace ISO 8601 timestamps inside a string with 'YYYY-MM-DD HH:MM UTC'."""
    if not text:
        return text
    return _ISO_TS_RE.sub(lambda m: m.group()[:16].replace("T", " ") + " UTC", text)


def _fmt_interval_minutes(minutes: int | str | None) -> str:
    """Human-readable collection interval for connection cards."""
    try:
        value = int(minutes)
    except (TypeError, ValueError):
        return "интервал не задан"
    if value == 1440:
        return "раз в сутки"
    if value == 60:
        return "каждый час"
    if value % 60 == 0:
        hours = value // 60
        return f"каждые {hours} ч"
    return f"каждые {value} мин"


def _get_display_tz() -> ZoneInfo:
    tz_name = os.environ.get("DISPLAY_TZ", "UTC")
    try:
        return ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, KeyError):
        return ZoneInfo("UTC")


_DISPLAY_TZ = _get_display_tz()


def _format_datetime(value) -> str:
    """Format stored UTC timestamps for compact dashboard display."""
    if value is None or value == "":
        return "—"
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    else:
        return str(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    localized = parsed.astimezone(_DISPLAY_TZ)
    tz_label = localized.tzname() or _DISPLAY_TZ.key or "UTC"
    return localized.strftime(f"%d.%m %H:%M {tz_label}")


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


def _warn_unsafe_sqlite_metrics_store() -> None:
    """Log a LOUD warning if MONITOR_DB_URL=sqlite:/// в production-like
    runtime (#214). Не fail-fast — это бы блокировало смешанные сценарии
    (CI, мини-демо без compose), но JSON-логи кричат WARNING на
    каждом старте чтобы оператор не пропустил.

    Heuristic для "production-like runtime":
      - SQLite scheme в MONITOR_DB_URL, AND
      - либо FLASK_ENV != development, либо процесс выглядит как
        запущенный в Docker (/.dockerenv exists)

    В чистом локальном dev (FLASK_ENV=development без /.dockerenv) —
    silently OK, это и есть intended usage SQLite.
    """
    import os
    from urllib.parse import urlparse

    url = settings.MONITOR_DB_URL.strip()
    try:
        scheme = urlparse(url).scheme
    except (ValueError, AttributeError):
        return
    if not scheme.startswith("sqlite"):
        return

    is_docker = os.path.exists("/.dockerenv")
    is_dev = (settings.FLASK_ENV or "").lower() == "development"
    if is_dev and not is_docker:
        return  # intended local-dev path

    logging.getLogger("app.startup").warning(
        "MONITOR_DB_URL is SQLite (%s) in production-like runtime "
        "(FLASK_ENV=%s, in_docker=%s). SQLite metrics store corrupts under "
        "scheduler write load and breaks /dashboard/notifications. "
        "Switch to Postgres/Timescale — see .env.example MONITOR_DB_URL "
        "block + docs README 'Metrics store backend' section.",
        url, settings.FLASK_ENV, is_docker,
    )


def _auto_promote_admin() -> None:
    """Если settings.ADMIN_EMAIL задан и юзер с этим email существует —
    выставить is_admin=True (#220). Идемпотентно: повторный старт ничего
    не ломает. Юзер должен сначала зарегистрироваться через /auth/register
    — auto-promote не создаёт аккаунт.

    Silently no-op:
    - ADMIN_EMAIL пустой (intended dev path);
    - юзер не найден (ещё не зарегистрировался — promote применится
      на следующем app start, после регистрации).
    """
    email = (settings.ADMIN_EMAIL or "").strip().lower()
    if not email:
        return
    try:
        from app.metrics_storage import get_user_by_email, set_user_admin
        user = get_user_by_email(email)
        if user is None:
            logging.getLogger("app.startup").info(
                "ADMIN_EMAIL=%s set but no such user yet — promote will apply "
                "after they register.", email,
            )
            return
        if user.get("is_admin"):
            return  # уже admin, не пишем в БД повторно
        set_user_admin(user["id"], True)
        logging.getLogger("app.startup").info(
            "Promoted %s to system admin (ADMIN_EMAIL match)", email,
        )
    except Exception as exc:  # pragma: no cover - defensive
        # storage не должен ломать boot — admin promote можно сделать руками.
        logging.getLogger("app.startup").warning(
            "Auto-promote of ADMIN_EMAIL failed: %s", exc,
        )


def _cleanup_stale_collector_runs() -> None:
    """Mark collector runs left in running state by a crashed process."""
    try:
        from app.metrics_storage import cleanup_stale_collector_runs
        updated = cleanup_stale_collector_runs()
        if updated:
            logging.getLogger("app.startup").warning(
                "Marked %d stale collector run(s) as failed", updated,
            )
    except Exception as exc:  # pragma: no cover - defensive boot guard
        logging.getLogger("app.startup").warning(
            "Collector run stale cleanup failed: %s", exc,
        )


def create_app(config: dict | None = None):
    # Order matters: configure formatters/handlers BEFORE the DSN-scrub
    # filter so the scrubber gets attached to the JSON/text handler we
    # actually use. _ensure_dsn_logging_filter is idempotent per process.
    configure_logging(settings.LOG_FORMAT, level=settings.LOG_LEVEL)
    _ensure_dsn_logging_filter()
    # #214: warn ДО Sentry init, чтобы первая ошибка от corrupted SQLite
    # уже шла в Sentry с этим warning'ом сверху breadcrumb-стека.
    _warn_unsafe_sqlite_metrics_store()
    # #220: попытка авто-промоушена ADMIN_EMAIL → is_admin=True. Silently
    # no-op если юзер ещё не зарегистрирован.
    _auto_promote_admin()
    # Sentry init (#103) — no-op when SENTRY_DSN is empty. Must run
    # before Flask() so FlaskIntegration can patch the right symbols.
    init_sentry()
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
    app.jinja_env.filters["fmt_interval_minutes"] = _fmt_interval_minutes
    app.jinja_env.filters["format_datetime"] = _format_datetime

    if config:
        app.config.update(config)
    _cleanup_stale_collector_runs()

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
    app.register_blueprint(invites_bp)
    app.register_blueprint(connections_bp)
    app.register_blueprint(settings_bp)

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

    # Request-id correlation (#102). Honour upstream X-Request-Id if a load
    # balancer / proxy issued one; otherwise mint a fresh uuid4. The id is
    # echoed back on the response so a caller can grep server logs after
    # the fact. JsonFormatter pulls g.request_id into every log line.
    @app.before_request
    def _assign_request_id():
        import uuid

        from flask import g, request
        upstream = request.headers.get("X-Request-Id", "").strip()
        # Cap incoming header length so a hostile peer can't make logs
        # disgusting; UUID hex is 32 chars, give a 4× buffer for upstream
        # systems that prefix their own tracing IDs.
        g.request_id = upstream[:128] if upstream else uuid.uuid4().hex

    @app.after_request
    def _echo_request_id(response):
        from flask import g

        rid = getattr(g, "request_id", None)
        if rid:
            response.headers["X-Request-Id"] = rid
        return response

    @app.route("/")
    def index():
        """Public landing for anonymous, dashboard for signed-in users (#55).

        The marketing page is the same surface a cold visitor sees; an
        authenticated user always wants the dashboard, never the pitch.
        """
        from flask import url_for
        from flask_login import current_user

        if current_user.is_authenticated:
            return redirect(url_for("dashboard.overview"))
        return render_template("landing.html")

    # Prometheus instrumentation (#101). HTTP-level Counter/Histogram on
    # every request; /metrics endpoint serves the registry. Rate-limited
    # at 60/min so a misconfigured scrape interval can't DDoS the worker.
    install_http_instrumentation(app)

    @app.route("/metrics")
    @limiter.limit("60/minute")
    def metrics():
        """Prometheus text-exposition endpoint.

        Convention: no auth, no CSRF — scrapers are behind a network ACL.
        Don't render it behind /dashboard or anything login-gated; every
        Prometheus deployment assumes this is open inside the cluster.
        """
        return metrics_response()

    # CSRF exempt: /metrics is GET-only but Flask-WTF middleware can still
    # complain if a scraper sends odd headers; explicit exempt keeps the
    # contract clean.
    csrf.exempt(metrics)

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
