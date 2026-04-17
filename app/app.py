from flask import Flask, jsonify
from config import settings


def create_app():
    app = Flask(__name__)
    app.config["SECRET_KEY"] = settings.SECRET_KEY

    # Blueprints (подключаем по мере готовности)
    # from api.routes import api_bp
    # app.register_blueprint(api_bp, url_prefix="/api")

    @app.route("/healthz")
    def health():
        return jsonify({"status": "ok"})

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
