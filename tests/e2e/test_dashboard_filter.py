"""E2E: фильтр таблиц на /dashboard (#225).

`live_dashboard` фикстур из conftest сидит две таблицы: `users` и
`events`. Этого достаточно чтобы проверить:

- input#table-filter присутствует на странице
- ввод "use" → видим users, не видим events
- регистронезависимость: ввод "USER" → users всё ещё совпадает
- очистка → обе строки видны
- ввод бессмысленной строки → видим #no-tables-msg
"""

from __future__ import annotations

import pytest
from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e


def test_filter_hides_non_matching_rows(live_dashboard: str, page: Page):
    page.goto(f"{live_dashboard}/dashboard")
    users_row = page.locator("tr[data-table-name='users']")
    events_row = page.locator("tr[data-table-name='events']")
    expect(users_row).to_be_visible()
    expect(events_row).to_be_visible()

    page.fill("#table-filter", "use")
    expect(users_row).to_be_visible()
    expect(events_row).to_be_hidden()


def test_filter_is_case_insensitive(live_dashboard: str, page: Page):
    """Acceptance: «USER» находит «users»."""
    page.goto(f"{live_dashboard}/dashboard")

    page.fill("#table-filter", "USER")
    expect(page.locator("tr[data-table-name='users']")).to_be_visible()
    expect(page.locator("tr[data-table-name='events']")).to_be_hidden()


def test_clearing_filter_restores_all_rows(live_dashboard: str, page: Page):
    page.goto(f"{live_dashboard}/dashboard")
    page.fill("#table-filter", "USER")
    expect(page.locator("tr[data-table-name='events']")).to_be_hidden()

    page.fill("#table-filter", "")
    expect(page.locator("tr[data-table-name='users']")).to_be_visible()
    expect(page.locator("tr[data-table-name='events']")).to_be_visible()


def test_no_matches_shows_empty_state(live_dashboard: str, page: Page):
    page.goto(f"{live_dashboard}/dashboard")
    # #no-tables-msg изначально hidden (есть строки).
    expect(page.locator("#no-tables-msg")).to_be_hidden()

    page.fill("#table-filter", "zzz-no-such-table")
    expect(page.locator("tr[data-table-name='users']")).to_be_hidden()
    expect(page.locator("tr[data-table-name='events']")).to_be_hidden()
    expect(page.locator("#no-tables-msg")).to_be_visible()
    expect(page.locator("#no-tables-msg")).to_have_text("Таблицы не найдены")
