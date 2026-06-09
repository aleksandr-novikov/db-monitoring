"""E2E: smoke-check «Тест подключения» рендерит preview + privileges + warning (#229, #230).

Поднимаем Flask с реальным auth и mock-нутым `probe_connection`:
драйв через Playwright по пути регистрация → создать проект →
добавить подключение → нажать «Тест» → убедиться что:

- Видим список найденных таблиц (tables_preview, tables_found) — #230
- Видим privileges {select, insert} — #230
- При insert=True видим warning «write_privileges_detected» — #229
- При no_select_permission видим error block — #229
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


# Контролируется тест-кодом: каждый тест ставит сюда canned dict,
# probe_connection-mock возвращает копию.
_PROBE_RESULT: dict = {}


def _mock_probe(_dsn: str, **_kwargs) -> dict:
    return dict(_PROBE_RESULT)


@pytest.fixture(scope="module")
def smoke_server(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    import os
    os.environ["FERNET_KEY"] = Fernet.generate_key().decode()

    from app import crypto
    crypto.reset_for_tests()

    metrics_db = tmp_path_factory.mktemp("e2e-smoke") / "metrics.db"

    from app.config import settings
    settings.MONITOR_DB_URL = f"sqlite:///{metrics_db}"

    from app import connections, metrics_storage
    metrics_storage._engine = None
    metrics_storage._initialized = False
    metrics_storage.get_engine()

    # Подменяем probe_connection ДО boot — route смотрит на модульный
    # символ `probe_connection` в момент запроса, что нам и нужно.
    orig_probe = connections.probe_connection
    connections.probe_connection = _mock_probe

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
        "LOGIN_DISABLED": False,
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
        connections.probe_connection = orig_probe
        for name, fn in originals.items():
            setattr(db, name, fn)


def _submit_form(page: Page) -> None:
    """Кликаем submit-кнопку внутри основного <form>.

    Base-template содержит и logout-форму, и project-switcher, тоже с
    button[type='submit'] — глобальный селектор брал их первыми. Скоупим
    к ближайшей форме на content-area.
    """
    page.locator("main form, .max-w-md form, [class*='content'] form").first.locator(
        "input[type='submit'], button[type='submit']"
    ).first.click()


def _register(page: Page, base: str, email: str) -> None:
    page.goto(f"{base}/auth/register")
    page.fill("input[name='email']", email)
    page.fill("input[name='password']", "supersecret1")
    page.fill("input[name='confirm']", "supersecret1")
    _submit_form(page)
    page.wait_for_url(lambda url: "/auth/register" not in url, timeout=5_000)


def _go_to_connections(page: Page, base: str, slug: str) -> None:
    page.goto(f"{base}/projects/new")
    page.fill("input[name='name']", "Smoke Demo")
    page.fill("input[name='slug']", slug)
    _submit_form(page)
    page.wait_for_url(f"{base}/projects/{slug}", timeout=5_000)


def _add_connection(page: Page, base: str, slug: str) -> None:
    page.goto(f"{base}/projects/{slug}/connections/new")
    page.fill("input[name='name']", "demo-db")
    page.fill("input[name='dsn']", "postgresql://u:p@h:5432/d")
    page.fill("input[name='schema_name']", "public")
    page.fill("input[name='interval_minutes']", "15")
    _submit_form(page)
    # После create редирект либо на /dashboard (first-connection
    # auto-test ok), либо на /connections (auto-test error). Оба
    # подходят — главное чтобы ушли с /connections/new.
    page.wait_for_url(
        lambda url: "/connections/new" not in url, timeout=5_000,
    )


def test_smoke_check_shows_tables_privileges_and_write_warning(
    smoke_server: str, page: Page,
):
    """Главный happy path #229 + #230: видим preview, privileges, warning."""
    global _PROBE_RESULT
    _PROBE_RESULT = {
        "status": "ok",
        "database": "demo",
        "version": "PostgreSQL 16.1",
        "latency_ms": 42,
        "tables_found": 12,
        "tables_preview": [f"t{i:02d}" for i in range(10)],
        "privileges": {"select": True, "insert": True},
        "warnings": ["write_privileges_detected"],
    }

    _register(page, smoke_server, "smoke-warn@x.io")
    _go_to_connections(page, smoke_server, "smoke-warn")
    _add_connection(page, smoke_server, "smoke-warn")

    # Главная проверка — UI-страница списка подключений.
    page.goto(f"{smoke_server}/projects/smoke-warn/connections")
    page.click(".js-test-conn")

    panel = page.locator(".js-test-result").first
    expect(panel).to_be_visible()

    # tables_found рендерится в [data-tables-found].
    expect(panel.locator("[data-tables-found]")).to_have_text("12")

    # Хотя бы первая таблица из preview видна.
    expect(panel.get_by_text("t00", exact=True)).to_be_visible()

    # privileges select/insert — оба ✓.
    expect(panel.locator("[data-priv='select']")).to_have_text("✓")
    expect(panel.locator("[data-priv='insert']")).to_have_text("✓")

    # Warning блок.
    expect(panel.locator("[data-warning='write_privileges_detected']")).to_be_visible()


def test_smoke_check_shows_no_select_permission_error(
    smoke_server: str, page: Page,
):
    """Acceptance #229: USAGE=false → UI показывает code/message."""
    global _PROBE_RESULT
    _PROBE_RESULT = {
        "status": "error",
        "code": "no_select_permission",
        "message": "Нет прав USAGE на схему 'public'.",
        "latency_ms": 7,
    }

    _register(page, smoke_server, "smoke-err@x.io")
    _go_to_connections(page, smoke_server, "smoke-err")
    # Для no_select_permission — auto-test при создании connection
    # вернёт error и редиректит на /connections (не /dashboard).
    _add_connection(page, smoke_server, "smoke-err")

    page.goto(f"{smoke_server}/projects/smoke-err/connections")
    page.click(".js-test-conn")

    panel = page.locator(".js-test-result").first
    expect(panel).to_be_visible()
    expect(panel.locator("[data-error-code='no_select_permission']")).to_be_visible()
