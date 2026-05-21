import os
import re

from flask import Flask, jsonify, redirect
from flask_wtf.csrf import CSRFProtect

from .admin import bp as admin_bp
from .api import api
from .auth import _abort_if_unauthenticated, login_manager
from .auth import bp as auth_bp
from .config import settings
from .dashboard import bp as dashboard_bp
from .dashboard import status_class

_ISO_TS_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})"
)


def _fmt_iso_in_text(text: str) -> str:
    """Replace ISO 8601 timestamps inside a string with 'YYYY-MM-DD HH:MM UTC'."""
    if not text:
        return text
    return _ISO_TS_RE.sub(lambda m: m.group()[:16].replace("T", " ") + " UTC", text)


def create_app(config: dict | None = None):
    app = Flask(__name__)
    app.config["SECRET_KEY"] = settings.SECRET_KEY
    app.config["COLLECT_INTERVAL_MINUTES"] = settings.COLLECT_INTERVAL_MINUTES
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

    # Flask-Login + Flask-WTF (#49).
    login_manager.init_app(app)
    csrf = CSRFProtect(app)
    # Exempt the JSON API and admin endpoints from CSRF — they're called
    # from curl/scripts, not browser forms, and SameSite=Lax on the session
    # cookie already blocks cross-site POSTs from forging credentialed
    # requests. The /auth/* forms validate their own CSRF tokens via
    # FlaskForm regardless.
    csrf.exempt(api)
    csrf.exempt(admin_bp)

    app.register_blueprint(auth_bp)
    app.register_blueprint(api)
    app.register_blueprint(admin_bp)
    app.register_blueprint(dashboard_bp)

    # Gate the HTML surface (dashboard + admin) behind login. Done as an
    # app-level before_request with path-based dispatch (not a blueprint
    # hook) because blueprints are module-level objects shared across
    # `create_app()` calls — Flask refuses a second `before_request` once
    # they've been registered once.
    _PROTECTED_PREFIXES = ("/dashboard", "/admin")
    @app.before_request
    def _require_login_for_html():
        from flask import request
        if request.path.startswith(_PROTECTED_PREFIXES):
            return _abort_if_unauthenticated()
        return None

    @app.route("/")
    def index():
        return redirect("/dashboard")

    @app.route("/healthz")
    def health():
        return jsonify({"status": "ok"})

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
