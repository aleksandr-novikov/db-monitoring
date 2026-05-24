"""Tests for app/llm.py and POST /api/explain."""
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from app import llm as llm_mod
from app.app import create_app

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts() -> str:
    return datetime(2026, 4, 20, 12, 0, tzinfo=UTC).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# _build_prompt
# ---------------------------------------------------------------------------

def test_build_prompt_contains_table_and_ts(tmp_path, monkeypatch):
    monkeypatch.setattr("app.llm.get_schema_snapshot", lambda t: [{"name": "id", "type": "int", "nullable": False}])
    monkeypatch.setattr("app.llm.get_metrics", lambda t, m, p=None, **kw: [])
    monkeypatch.setattr("app.llm.get_changepoints", lambda t, **kw: [])
    monkeypatch.setattr("app.llm.get_anomaly_scores", lambda t, **kw: [])

    ts = _ts()
    prompt = llm_mod._build_prompt("orders", "row_count", ts)
    assert "orders" in prompt
    assert "2026-04-20 12:00 UTC" in prompt
    assert "row_count" in prompt
    assert "id int" in prompt
    assert "Respond in Russian" in prompt


def test_build_prompt_includes_both_metrics(tmp_path, monkeypatch):
    """Prompt must include context for both row_count and null_rate regardless of `metric`."""
    monkeypatch.setattr("app.llm.get_schema_snapshot", lambda t: None)
    rc_rows = [{"ts": _ts(), "value": 500.0, "tags": None}]
    nr_rows = [{"ts": _ts(), "value": 0.15, "tags": None}]
    def _fake_metrics(table, metric, project_id=None, **kw):
        return rc_rows if metric == "row_count" else nr_rows
    monkeypatch.setattr("app.llm.get_metrics", _fake_metrics)
    monkeypatch.setattr("app.llm.get_changepoints", lambda t, **kw: [])
    monkeypatch.setattr("app.llm.get_anomaly_scores", lambda t, **kw: [])

    prompt = llm_mod._build_prompt("orders", "null_rate", _ts())
    assert "500" in prompt
    assert "0.150" in prompt
    assert "2026-04-20 12:00 UTC" in prompt
    assert "T12:00:00+00:00" not in prompt


# ---------------------------------------------------------------------------
# _parse_nim_response
# ---------------------------------------------------------------------------

def test_parse_nim_response_clean_json():
    raw = '{"explanation": "ETL failed", "suggested_fix": "Rerun", "confidence": 0.9}'
    result = llm_mod._parse_nim_response(raw)
    assert result["explanation"] == "ETL failed"
    assert result["suggested_fix"] == "Rerun"
    assert result["confidence"] == pytest.approx(0.9)


def test_parse_nim_response_markdown_fence():
    raw = '```json\n{"explanation": "foo", "suggested_fix": "bar", "confidence": 0.7}\n```'
    result = llm_mod._parse_nim_response(raw)
    assert result["explanation"] == "foo"


def test_parse_nim_response_json_embedded_in_text():
    raw = 'Here is the analysis:\n{"explanation": "baz", "suggested_fix": "fix", "confidence": 0.5}\nDone.'
    result = llm_mod._parse_nim_response(raw)
    assert result["explanation"] == "baz"


def test_parse_nim_response_clamps_confidence():
    raw = '{"explanation": "x", "suggested_fix": "y", "confidence": 1.5}'
    result = llm_mod._parse_nim_response(raw)
    assert result["confidence"] == 1.0

    raw2 = '{"explanation": "x", "suggested_fix": "y", "confidence": -0.2}'
    result2 = llm_mod._parse_nim_response(raw2)
    assert result2["confidence"] == 0.0


def test_parse_nim_response_invalid_returns_empty():
    assert llm_mod._parse_nim_response("not json at all") == {}
    assert llm_mod._parse_nim_response("") == {}


# ---------------------------------------------------------------------------
# _rule_based_explain
# ---------------------------------------------------------------------------

def test_rule_based_explain_returns_correct_structure(monkeypatch):
    monkeypatch.setattr("app.llm.get_metrics", lambda t, m, p=None, **kw: [])
    result = llm_mod._rule_based_explain("orders", "row_count", _ts())
    assert "explanation" in result
    assert "suggested_fix" in result
    assert result["confidence"] == pytest.approx(0.3)


def test_rule_based_explain_row_drop(monkeypatch):
    rows = [
        {"ts": "2026-04-20T11:00:00+00:00", "value": 1000.0, "tags": None},
        {"ts": "2026-04-20T12:00:00+00:00", "value": 200.0, "tags": None},
    ]
    def _fake_metrics(table, metric, project_id=None, **kw):
        return rows if metric == "row_count" else []
    monkeypatch.setattr("app.llm.get_metrics", _fake_metrics)
    result = llm_mod._rule_based_explain("orders", "row_count", _ts())
    assert "снижение" in result["explanation"].lower() or "удален" in result["explanation"].lower()


def test_rule_based_explain_high_null_rate(monkeypatch):
    nr_rows = [{"ts": _ts(), "value": 0.35, "tags": None}]
    def _fake_metrics(table, metric, project_id=None, **kw):
        return [] if metric == "row_count" else nr_rows
    monkeypatch.setattr("app.llm.get_metrics", _fake_metrics)
    result = llm_mod._rule_based_explain("orders", "null_rate", _ts())
    assert "null" in result["explanation"].lower() or "пропуск" in result["explanation"].lower()


# ---------------------------------------------------------------------------
# explain_anomaly — fallback on network error
# ---------------------------------------------------------------------------

def test_explain_anomaly_falls_back_on_network_error(monkeypatch):
    monkeypatch.setattr("app.config.settings.NIM_API_KEY", "test-key")
    monkeypatch.setattr("app.llm.get_schema_snapshot", lambda t: None)
    monkeypatch.setattr("app.llm.get_metrics", lambda t, m, p=None, **kw: [])
    monkeypatch.setattr("app.llm.get_changepoints", lambda t, **kw: [])
    monkeypatch.setattr("app.llm.get_anomaly_scores", lambda t, **kw: [])
    monkeypatch.setattr("app.llm._call_nim", MagicMock(side_effect=httpx.ConnectError("unreachable")))

    result = llm_mod.explain_anomaly("orders", "row_count", _ts())
    assert "explanation" in result
    assert result["confidence"] == pytest.approx(0.3)


def test_explain_anomaly_no_api_key_returns_rule_based(monkeypatch):
    monkeypatch.setattr("app.config.settings.NIM_API_KEY", "")
    monkeypatch.setattr("app.llm.get_metrics", lambda t, m, p=None, **kw: [])
    result = llm_mod.explain_anomaly("orders", "row_count", _ts())
    assert "explanation" in result
    assert result["confidence"] == pytest.approx(0.3)


# ---------------------------------------------------------------------------
# Cache: second call must not invoke LLM
# ---------------------------------------------------------------------------

@pytest.fixture
def storage(tmp_path, monkeypatch):
    import app.metrics_storage as storage_mod
    db_path = tmp_path / "metrics.db"
    monkeypatch.setattr(storage_mod.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage_mod, "_engine", None)
    monkeypatch.setattr(storage_mod, "_initialized", False)
    return storage_mod


def test_cache_second_call_skips_llm(storage):
    from app.metrics_storage import get_cached_explanation, save_explanation

    ts = _ts()
    # Nothing cached yet
    assert get_cached_explanation("orders", "row_count", ts) is None

    # Save an explanation
    save_explanation("orders", "row_count", ts, "Test explanation", "Test fix", 0.85)

    # Should be returned from cache
    cached = get_cached_explanation("orders", "row_count", ts)
    assert cached is not None
    assert cached["explanation"] == "Test explanation"
    assert cached["confidence"] == pytest.approx(0.85)


def test_cache_expired_returns_none(storage, monkeypatch):
    from datetime import timedelta

    from app.metrics_storage import _iso, get_cached_explanation

    ts = _ts()
    # Save with a created_at far in the past (25 h ago)
    old_created_at = _iso(datetime.now(UTC) - timedelta(hours=25))
    from sqlalchemy import text
    with storage.get_engine().begin() as conn:
        conn.execute(text("""
            INSERT OR REPLACE INTO llm_explanations
                (table_name, metric, ts, explanation, suggested_fix, confidence, created_at)
            VALUES ('orders', 'row_count', :ts, 'old', 'old fix', 0.5, :ca)
        """), {"ts": ts, "ca": old_created_at})

    assert get_cached_explanation("orders", "row_count", ts) is None


# ---------------------------------------------------------------------------
# POST /api/explain endpoint
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    app = create_app({"TESTING": True})
    with app.test_client() as c:
        yield c


_KNOWN_TABLES = [{"table_name": "orders"}, {"table_name": "users"}]


def test_explain_endpoint_returns_200(client):
    payload = {"explanation": "причина", "suggested_fix": "решение", "confidence": 0.8}
    with patch("app.llm.explain_anomaly", return_value=payload), \
         patch("app.api.get_cached_explanation", return_value=None), \
         patch("app.api.save_explanation"), \
         patch("app.api.list_tables", return_value=_KNOWN_TABLES):
        resp = client.post("/api/explain", json={"table": "orders", "metric": "row_count", "ts": _ts()})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["explanation"] == "причина"
    assert data["confidence"] == pytest.approx(0.8)


def test_explain_endpoint_unknown_table_returns_404(client):
    with patch("app.api.get_cached_explanation", return_value=None), \
         patch("app.api.list_tables", return_value=_KNOWN_TABLES):
        resp = client.post("/api/explain", json={"table": "nonexistent", "metric": "row_count", "ts": _ts()})
    assert resp.status_code == 404


def test_explain_endpoint_missing_fields(client):
    resp = client.post("/api/explain", json={"table": "orders"})
    assert resp.status_code == 400


def test_explain_endpoint_invalid_metric(client):
    resp = client.post("/api/explain", json={"table": "orders", "metric": "size_bytes", "ts": _ts()})
    assert resp.status_code == 400


def test_explain_endpoint_serves_from_cache(client):
    cached = {"explanation": "кэш", "suggested_fix": "кэш fix", "confidence": 0.6}
    with patch("app.api.get_cached_explanation", return_value=cached):
        resp = client.post("/api/explain", json={"table": "orders", "metric": "row_count", "ts": _ts()})
    assert resp.status_code == 200
    assert resp.get_json()["explanation"] == "кэш"
