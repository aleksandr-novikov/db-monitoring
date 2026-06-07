"""E2E: /admin/* gated by ``is_admin`` flag (#220).

Acceptance из тикета — обычный юзер видит 403 на /admin/jobs, админ
проходит. Проверяем через реальный браузер + реальный Werkzeug, поднятый
с ``LOGIN_DISABLED=False`` (главный ``live_dashboard`` фикстур ставит
True для остального E2E suite — поэтому здесь свой сервер).
"""

from __future__ import annotations

import socket
import threading
from collections.abc import Iterator

import pytest
from cryptography.fernet import Fernet
from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def admin_server(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """Боевой Flask с LOGIN_DISABLED=False и пустой БД метрик.

    Module-scoped — register/login прогон один на оба теста, юзер
    шарится. Admin промоушен идёт через ``set_user_admin`` прямо в
    storage между тест-кейсами.
    """
    import os
    os.environ["FERNET_KEY"] = Fernet.generate_key().decode()

    from app import crypto
    crypto.reset_for_tests()

    metrics_db = tmp_path_factory.mktemp("e2e-admin") / "metrics.db"

    from app.config import settings
    settings.MONITOR_DB_URL = f"sqlite:///{metrics_db}"

    from app import metrics_storage
    metrics_storage._engine = None
    metrics_storage._initialized = False
    metrics_storage.get_engine()

    # Stub db introspection — иначе apsheduler / dashboard будут лезть
    # в реальный DATABASE_URL, что нам не нужно.
    from app import db
    originals = {
        "list_tables": db.list_tables,
        "table_schema": db.table_schema,
        "table_stats": db.table_stats,
        "column_nulls": db.column_nulls,
        "column_distribution": db.column_distribution,
    }
    db.list_tables = lambda schema=None: []
    db.table_schema = lambda t, schema=None: []
    db.table_stats = lambda t, schema=None: None
    db.column_nulls = lambda t, schema=None: []
    db.column_distribution = lambda t, schema=None, top_n=20: []

    from app.app import create_app

    app = create_app({
        "TESTING": True,
        "LOGIN_DISABLED": False,  # реально проверяем гейт
        "WTF_CSRF_ENABLED": False,
        "RATELIMIT_ENABLED": False,
    })
    port = _free_port()

    from werkzeug.serving import make_server

    server = make_server("127.0.0.1", port, app, threaded=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        for name, fn in originals.items():
            setattr(db, name, fn)


_EMAIL = "e2e-admin@x.io"
_PASSWORD = "supersecret1"


def _register_via_ui(page: Page, base_url: str, email: str, password: str) -> None:
    page.goto(f"{base_url}/auth/register")
    page.fill("input[name='email']", email)
    page.fill("input[name='password']", password)
    page.fill("input[name='confirm']", password)
    page.click("input[type='submit'], button[type='submit']")
    # Register auto-logs in и редиректит на onboarding/dashboard — ждём
    # пока URL уйдёт с /auth/register.
    page.wait_for_url(lambda url: "/auth/register" not in url, timeout=5_000)


def _login_via_ui(page: Page, base_url: str, email: str, password: str) -> None:
    page.goto(f"{base_url}/auth/login")
    page.fill("input[name='email']", email)
    page.fill("input[name='password']", password)
    page.click("input[type='submit'], button[type='submit']")
    page.wait_for_url(lambda url: "/auth/login" not in url, timeout=5_000)


def test_regular_user_gets_403_on_admin_jobs(admin_server: str, page: Page):
    """Обычный (не-admin) юзер логинится → /admin/jobs возвращает 403."""
    _register_via_ui(page, admin_server, _EMAIL, _PASSWORD)

    # Делаем запрос через page.request, чтобы поймать HTTP статус —
    # browser-side page.goto на 403 рендерит error page, статус не виден.
    resp = page.request.get(f"{admin_server}/admin/jobs")
    assert resp.status == 403


def test_admin_user_gets_200_on_admin_jobs(admin_server: str, page: Page):
    """После set_user_admin(True) тот же юзер получает 200 + JSON."""
    from app.metrics_storage import get_user_by_email, set_user_admin

    user = get_user_by_email(_EMAIL)
    assert user is not None, "fixture order сломан — юзер должен быть из теста выше"
    set_user_admin(user["id"], True)

    # Re-login — нужно фреш session с обновлённым is_admin (flask-login
    # хранит привязку к user_id, is_admin читается лениво из storage —
    # но safer перелогиниться, чтобы тест был детерминистский).
    page.request.post(f"{admin_server}/auth/logout")
    _login_via_ui(page, admin_server, _EMAIL, _PASSWORD)

    resp = page.request.get(f"{admin_server}/admin/jobs")
    assert resp.status == 200
    # Контракт /admin/jobs — JSON-список.
    body = resp.json()
    assert isinstance(body, list)

    # Sanity для UI: rollback checklist рендерится для админа.
    page.goto(f"{admin_server}/admin/rollback-checklist")
    expect(page.locator("body")).to_be_visible()
