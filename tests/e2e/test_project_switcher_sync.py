"""E2E: switcher в шапке следует за URL slug (#258).

Симптом, обнаруженный QA: ходишь по `/projects/<slug>/*`, а top-right
project switcher продолжает показывать предыдущий current project. Фикс:
slug-роуты синкают `session.current_project_id` через
`_require_owned_project`. Здесь — реальный браузер проверяет что после
навигации по slug-роуту шапка показывает имя проекта из URL.

Single-test структура: page-fixture у pytest-playwright function-scope,
поэтому держать «логин один раз и потом 4 теста» нельзя без боли с
shared-page. Делаем один последовательный сценарий — всё-равно вся
проверка это «прошли N URL и в шапке правильный заголовок».
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
def switcher_server(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """Real Flask с LOGIN_DISABLED=False — нужен реальный auth-flow."""
    import os
    os.environ["FERNET_KEY"] = Fernet.generate_key().decode()

    from app import crypto
    crypto.reset_for_tests()

    metrics_db = tmp_path_factory.mktemp("e2e-switcher") / "metrics.db"

    from app.config import settings
    settings.MONITOR_DB_URL = f"sqlite:///{metrics_db}"

    from app import metrics_storage
    metrics_storage._engine = None
    metrics_storage._initialized = False
    metrics_storage.get_engine()

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
        for name, fn in originals.items():
            setattr(db, name, fn)


_EMAIL = "e2e-switcher@x.io"
_PASSWORD = "supersecret1"


def _switcher_text(page: Page) -> str:
    return page.locator("header details summary").first.inner_text()


def test_switcher_follows_url_slug_across_project_pages(
    switcher_server: str, page: Page,
):
    """Один сквозной сценарий — register → создать 2-й проект → пройти
    по slug-роутам и убедиться что шапка следует за URL."""
    base = switcher_server

    # 1. Register → onboarding redirects to /projects/new?onboarding=1.
    #    Create «Default» project there — becomes current.
    page.goto(f"{base}/auth/register")
    page.fill("input[name='email']", _EMAIL)
    page.fill("input[name='password']", _PASSWORD)
    page.fill("input[name='confirm']", _PASSWORD)
    page.click("input[type='submit'], button[type='submit']")
    page.wait_for_url(lambda url: "onboarding" in url, timeout=5_000)
    page.fill("input[name='name']", "Default")
    page.fill("input[name='slug']", "default")
    page.click("main form button[type='submit'], main form input[type='submit']")
    page.wait_for_url(lambda url: "/projects/new" not in url, timeout=5_000)
    assert "Default" in _switcher_text(page)

    # 2. Create second project «Production» — projects.new_project
    # auto-switches, current = Production after this.
    page.goto(f"{base}/projects/new")
    page.fill("input[name='name']", "Production")
    page.fill("input[name='slug']", "prod")
    page.click("main form button[type='submit'], main form input[type='submit']")
    page.wait_for_url(lambda url: "/projects/new" not in url, timeout=5_000)
    assert "Production" in _switcher_text(page)

    # 3. Бага из QA: /projects/default — шапка должна стать Default,
    # а не остаться на Production.
    page.goto(f"{base}/projects/default")
    expect(page.locator("header details summary")).to_contain_text("Default")
    assert "Default" in _switcher_text(page)

    # 4. То же для /projects/<slug>/connections — переключаемся обратно
    # на Production через URL подключений.
    page.goto(f"{base}/projects/prod/connections")
    expect(page.locator("header details summary")).to_contain_text("Production")

    # 5. То же для /projects/<slug>/settings/notifications.
    page.goto(f"{base}/projects/default/settings/notifications")
    expect(page.locator("header details summary")).to_contain_text("Default")

    # 6. После всей пляски сессия зафиксирована на Default — открытие
    # глобального /dashboard это подтверждает (он берёт current из session).
    page.goto(f"{base}/dashboard/")
    expect(page.locator("header details summary")).to_contain_text("Default")


def test_projects_list_no_longer_renders_make_current_button(
    switcher_server: str, page: Page,
):
    """`templates/projects/list.html`: «Сделать текущим» убрана,
    бейдж «текущий» остаётся."""
    base = switcher_server
    # Юзер уже создан в предыдущем тесте (module-scope server). Логинимся.
    page.goto(f"{base}/auth/login")
    page.fill("input[name='email']", _EMAIL)
    page.fill("input[name='password']", _PASSWORD)
    page.click("input[type='submit'], button[type='submit']")
    page.wait_for_url(lambda url: "/auth/login" not in url, timeout=5_000)

    page.goto(f"{base}/projects")
    expect(page.locator("text=Сделать текущим")).to_have_count(0)
    expect(page.locator("text=текущий")).to_have_count(1)
