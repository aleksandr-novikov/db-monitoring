"""Playwright dashboard E2E tests (#45).

Drives the real Flask app in a headless Chromium. Tests are intentionally
shallow — they confirm the page wires up, navigation works, the chart
script runs, and the theme toggle persists — not exhaustive UI assertions.
The browser process is the slow part, so we keep the suite tight.
"""

from __future__ import annotations

import re

import pytest
from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e

_ACTIVE_TAB_RE = re.compile(r"bg-accent")


def test_overview_renders_kpi_cards_and_table_list(live_dashboard: str, page: Page):
    page.goto(f"{live_dashboard}/dashboard")
    # exact=True — without it, "Обзор" also matches the "Обзор таблиц" h2.
    expect(page.get_by_role("heading", name="Обзор", exact=True)).to_be_visible()

    # The three KPI labels — order is stable in the template.
    for label in ("Мониторируется таблиц", "Всего строк", "Средний NULL %"):
        expect(page.get_by_text(label, exact=True)).to_be_visible()

    # Both seeded tables appear in the table list.
    expect(page.get_by_role("link", name="users").first).to_be_visible()
    expect(page.get_by_role("link", name="events").first).to_be_visible()


def _wait_for_chart(page: Page) -> None:
    """Block until Plotly has injected something into #chart.

    Checking ``children.length > 0`` is too weak — Plotly injects its
    container skeleton synchronously, before any trace renders. Waiting
    for ``svg.main-svg .scatterlayer .trace`` is the closest reliable
    proxy for "the data line is on screen": the trace group only appears
    after the first data point has been drawn.
    """
    page.wait_for_function(
        "() => document.querySelector('#chart svg.main-svg .scatterlayer .trace') !== null",
        timeout=10_000,
    )


def test_click_table_navigates_to_detail_with_chart(live_dashboard: str, page: Page):
    page.goto(f"{live_dashboard}/dashboard")
    page.get_by_role("link", name="users").first.click()

    expect(page).to_have_url(f"{live_dashboard}/dashboard/schema/users")
    expect(page.get_by_role("heading", name="Таблица: users")).to_be_visible()

    _wait_for_chart(page)
    expect(page.locator("#chart-empty")).to_be_hidden()


def test_chart_metric_toggle_redraws_plot(live_dashboard: str, page: Page):
    page.goto(f"{live_dashboard}/dashboard/schema/users")
    _wait_for_chart(page)

    row_count_tab = page.locator("#chart-tabs button[data-metric='row_count']")
    null_rate_tab = page.locator("#chart-tabs button[data-metric='null_rate']")

    # Initial state: row_count active (bg-accent), null_rate inactive.
    expect(row_count_tab).to_have_class(_ACTIVE_TAB_RE)

    # Click the NULL rate tab — Plotly re-renders, active style swaps.
    null_rate_tab.click()
    expect(null_rate_tab).to_have_class(_ACTIVE_TAB_RE)
    expect(row_count_tab).not_to_have_class(_ACTIVE_TAB_RE)

    # And back — confirms the toggle is reversible.
    row_count_tab.click()
    expect(row_count_tab).to_have_class(_ACTIVE_TAB_RE)


def test_schema_page_lists_table_with_columns(live_dashboard: str, page: Page):
    page.goto(f"{live_dashboard}/dashboard/schema")
    expect(page.get_by_role("heading", name="Схема")).to_be_visible()
    # Seeded tables appear with their column counts.
    expect(page.get_by_role("link", name="users").first).to_be_visible()
    # Column names from SEEDED_SCHEMAS["users"].
    expect(page.get_by_text("email", exact=True).first).to_be_visible()
    expect(page.get_by_text("country", exact=True).first).to_be_visible()


def test_theme_toggle_persists_in_localstorage(live_dashboard: str, page: Page):
    page.goto(f"{live_dashboard}/dashboard")
    # Default theme depends on prefers-color-scheme; force a known starting
    # point by clearing localStorage and reloading.
    page.evaluate("() => localStorage.removeItem('theme')")
    page.reload()

    # First click — switch to whichever isn't current, read localStorage.
    page.locator("#theme-toggle").click()
    stored = page.evaluate("() => localStorage.getItem('theme')")
    assert stored in ("dark", "light")

    # Reload and verify the stored theme wins (no flash → the inline head
    # script applies the class synchronously before paint).
    page.reload()
    has_dark = page.evaluate(
        "() => document.documentElement.classList.contains('dark')"
    )
    assert has_dark == (stored == "dark"), \
        f"theme={stored} but dark class={'present' if has_dark else 'absent'} after reload"


def test_empty_chart_state_on_table_without_history(live_dashboard: str, page: Page):
    """Acceptance: «Нет исторических метрик» when a table has no row_count."""
    page.goto(f"{live_dashboard}/dashboard/schema/events")

    # The chart fetches /api/metrics/events?metric=row_count, gets an empty
    # series, and reveals #chart-empty while keeping #chart hidden.
    expect(page.locator("#chart-empty")).to_be_visible(timeout=10_000)
    expect(page.locator("#chart-empty")).to_have_text(
        "Нет исторических метрик. Запустите коллектор или сидер."
    )


def test_empty_overview_when_no_tables_monitored(live_dashboard: str, page: Page):
    """Acceptance: «Нет таблиц для мониторинга» empty-state on overview."""
    from app import db

    original = db.list_tables
    db.list_tables = lambda schema=None: []
    try:
        page.goto(f"{live_dashboard}/dashboard")
        expect(page.get_by_text("Нет таблиц для мониторинга", exact=True)).to_be_visible()
    finally:
        db.list_tables = original
