import os

from flask import Flask, jsonify, redirect

from .admin import bp as admin_bp
from .api import api
from .config import settings
from .dashboard import bp as dashboard_bp, status_class


def create_app(config: dict | None = None):
    app = Flask(__name__)
    app.config["SECRET_KEY"] = settings.SECRET_KEY
    app.config["COLLECT_INTERVAL_MINUTES"] = settings.COLLECT_INTERVAL_MINUTES
    app.jinja_env.filters["status_class"] = status_class

    if config:
        app.config.update(config)

    app.register_blueprint(api)
    app.register_blueprint(admin_bp)
    app.register_blueprint(dashboard_bp)

    @app.route("/")
    def index():
        return redirect("/dashboard")

    @app.route("/healthz")
    def health():
        return jsonify({"status": "ok"})

    if not app.config.get("TESTING"):
        # In debug mode the Werkzeug reloader forks the process; only start
        # the scheduler in the child (worker) process, not the parent.
        if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
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
