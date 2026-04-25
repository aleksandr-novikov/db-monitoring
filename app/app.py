from flask import Flask, jsonify

from .api import api
from .config import settings


def create_app():
    app = Flask(__name__)
    app.config["SECRET_KEY"] = settings.SECRET_KEY

    app.register_blueprint(api)

    @app.route("/healthz")
    def health():
        return jsonify({"status": "ok"})

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
