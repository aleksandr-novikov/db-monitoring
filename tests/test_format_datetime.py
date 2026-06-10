"""Tests for _format_datetime Jinja filter and DISPLAY_TZ support (#292)."""

from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest

from app.app import _format_datetime

MSK = ZoneInfo("Europe/Moscow")  # UTC+3
NYC = ZoneInfo("America/New_York")  # UTC-4 (summer)


@pytest.fixture()
def tz_utc(monkeypatch):
    import app.app as m
    monkeypatch.setattr(m, "_DISPLAY_TZ", ZoneInfo("UTC"))


@pytest.fixture()
def tz_moscow(monkeypatch):
    import app.app as m
    monkeypatch.setattr(m, "_DISPLAY_TZ", MSK)


# ---------------------------------------------------------------------------
# Базовые случаи — пустые / None
# ---------------------------------------------------------------------------

def test_none_returns_dash(tz_utc):
    assert _format_datetime(None) == "—"


def test_empty_string_returns_dash(tz_utc):
    assert _format_datetime("") == "—"


# ---------------------------------------------------------------------------
# UTC (дефолт)
# ---------------------------------------------------------------------------

def test_datetime_utc_display(tz_utc):
    ts = datetime(2026, 6, 10, 5, 0, tzinfo=UTC)
    assert _format_datetime(ts) == "2026-06-10 05:00"


def test_iso_string_z_suffix_utc(tz_utc):
    assert _format_datetime("2026-06-10T05:00:00Z") == "2026-06-10 05:00"


def test_iso_string_with_offset_utc(tz_utc):
    assert _format_datetime("2026-06-10T05:00:00+00:00") == "2026-06-10 05:00"


# ---------------------------------------------------------------------------
# Europe/Moscow (UTC+3)
# ---------------------------------------------------------------------------

def test_utc_converts_to_msk(tz_moscow):
    ts = datetime(2026, 6, 10, 5, 0, tzinfo=UTC)
    assert _format_datetime(ts) == "2026-06-10 08:00"


def test_iso_string_converts_to_msk(tz_moscow):
    assert _format_datetime("2026-06-10T05:00:00Z") == "2026-06-10 08:00"


def test_midnight_utc_to_msk_crosses_day(tz_moscow):
    # 23:00 UTC = 02:00 MSK следующего дня
    ts = datetime(2026, 6, 10, 23, 0, tzinfo=UTC)
    assert _format_datetime(ts) == "2026-06-11 02:00"


# ---------------------------------------------------------------------------
# Naive datetime — должен трактоваться как UTC
# ---------------------------------------------------------------------------

def test_naive_datetime_treated_as_utc(tz_moscow):
    ts = datetime(2026, 6, 10, 5, 0)  # без tzinfo
    assert _format_datetime(ts) == "2026-06-10 08:00"


# ---------------------------------------------------------------------------
# Невалидная ISO-строка — возвращается как есть
# ---------------------------------------------------------------------------

def test_invalid_iso_string_returned_as_is(tz_utc):
    assert _format_datetime("not-a-date") == "not-a-date"


# ---------------------------------------------------------------------------
# Нестроковый / не-datetime тип
# ---------------------------------------------------------------------------

def test_integer_returns_str(tz_utc):
    assert _format_datetime(42) == "42"
