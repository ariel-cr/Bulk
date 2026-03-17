"""CDC Bulk Tester - App principal"""
from flask import Flask


def create_app():
    app = Flask(__name__)

    from routes.common import common_bp
    from routes.newcore_to_legacy import newcore_bp
    from routes.legacy_to_newcore import legacy_bp

    app.register_blueprint(common_bp)
    app.register_blueprint(newcore_bp)
    app.register_blueprint(legacy_bp)

    return app


if __name__ == "__main__":
    create_app().run(debug=True, port=5050)
