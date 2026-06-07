"""Secret-scrub coverage for AWS / S3 / Iceberg / ClickHouse leaks (#231).

#143 already covers Postgres DSN passwords + Telegram bot tokens. This
file pins three new patterns:

* AWS Access Key ID (AKIA/ASIA…)
* AWS Secret Access Key / S3 secret in ``key=value`` form (also catches
  ``s3.secret-access-key``, ``access_key_secret`` — variants Iceberg/MinIO
  configs actually emit in exception messages).
* Bearer / Token auth-header values (Iceberg REST catalog auth pass-through,
  generic HTTP catalog clients).

Plus three negative checks (no false positives) and three integration
checks:
* /healthz payload doesn't contain DSN/tokens (acceptance из тикета)
* _probe_iceberg user-facing JSON message doesn't leak exception text
* _probe_clickhouse same
"""

from __future__ import annotations

import re

import pytest

from app.security import _scrub

# ── AWS Access Key ID ────────────────────────────────────────────────────


@pytest.mark.parametrize("input_str, expected_replacement", [
    ("AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE",
        "AWS_ACCESS_KEY_ID=***"),
    ("AKIAIOSFODNN7EXAMPLE",
        "***"),
    ("token=ASIA1234567890ABCDEF",
        "token=***"),
    # внутри длинного traceback-style message
    ("ClientError(AccessDenied): user AKIAIOSFODNN7EXAMPLE",
        "ClientError(AccessDenied): user ***"),
])
def test_scrub_masks_aws_access_key(input_str, expected_replacement):
    out = _scrub(input_str)
    assert out == expected_replacement
    # AKIA/ASIA не должны остаться в открытом виде нигде в результате.
    assert not re.search(r"AKIA[0-9A-Z]{16}", out)
    assert not re.search(r"ASIA[0-9A-Z]{16}", out)


def test_scrub_does_not_match_short_uppercase(monkeypatch):
    """AKIA — короче 20 char total — не должно матчиться. Защита от
    false positives на типах вроде AKIANCE / AKIAOK = слово, не key."""
    out = _scrub("AKIA hello AKIAOK!")
    # AKIA + space или AKIAOK — не полный 20-char key, остаётся как есть.
    assert "AKIA hello" in out
    assert "AKIAOK" in out


# ── AWS / S3 secret в key=value ─────────────────────────────────────────


@pytest.mark.parametrize("input_str", [
    # Real-shape AWS secrets — 40 char base64-ish. Regex требует >=16
    # чтобы не ловить случайные ID типа key=2025-01-15.
    "AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "secret_access_key=wJalrXUtnFEMIabcdefghijklmnopqrstuvwxyz",
    "s3.secret-access-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "access_key_secret=wJalrXUtnFEMIabcdefghijklmnopqrstuvwxyz",
])
def test_scrub_masks_secret_access_key_form(input_str):
    """ANY вариант ключа с secret_access_key или access_key_secret должен
    получать ***. Сам ключ-имя сохраняется (он публичный)."""
    out = _scrub(input_str)
    assert "wJalrXUtnFEMI" not in out
    assert "***" in out


def test_scrub_masks_quoted_secret_value():
    """Значения часто в quote'ах в JSON/YAML configs."""
    out = _scrub('aws_secret_access_key="wJalrXUtnFEMI/K7"')
    assert "wJalrXUtnFEMI" not in out
    assert '"***"' in out


def test_scrub_preserves_key_name():
    """Самое имя key — публично (это идентификатор переменной), сохраняем."""
    out = _scrub("AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG")
    assert "AWS_SECRET_ACCESS_KEY" in out


# ── Bearer / Token auth ─────────────────────────────────────────────────


@pytest.mark.parametrize("input_str, expected", [
    ("Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig",
        "Authorization: Bearer ***"),
    ("Bearer abc123def456",
        "Bearer ***"),
    ("token Token raw-token-value-xyz",
        "token Token ***"),
    ("rest.authorization-header: Bearer xyz1234567890abc",
        "rest.authorization-header: Bearer ***"),
])
def test_scrub_masks_bearer_tokens(input_str, expected):
    out = _scrub(input_str)
    assert out == expected
    assert "eyJ" not in out
    assert "raw-token-value" not in out
    assert "abc123def456" not in out


def test_scrub_does_not_match_bearer_without_value():
    """``Bearer`` без значения / слишком короткое — не trigger."""
    assert _scrub("Bearer x") == "Bearer x"  # 1 char — слишком коротко
    assert _scrub("class Bearer:") == "class Bearer:"  # с двоеточием


# ── No false positives on normal strings ────────────────────────────────


def test_scrub_passes_normal_text_unchanged():
    msg = "Connecting to host:5432 with user:42; tables_found=10"
    assert _scrub(msg) == msg


def test_scrub_preserves_aws_pattern_in_long_text():
    """AWS keys внутри текста маскируются, остальное не трогаем."""
    msg = (
        "Failed to fetch from S3: bucket='my-bucket', "
        "AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE, "
        "AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG"
    )
    out = _scrub(msg)
    assert "my-bucket" in out
    assert "AKIAIOSFODNN7EXAMPLE" not in out
    assert "wJalrXUtnFEMI" not in out
    # Все три "***" должны быть (один за AKIA, один за secret).
    assert out.count("***") >= 2


# ── /healthz payload regression ──────────────────────────────────────────


@pytest.fixture
def health_client(tmp_path, monkeypatch):
    import app.metrics_storage as storage
    from app.app import create_app
    from app.config import settings as cfg

    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{tmp_path / 'h.db'}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    import app.db
    monkeypatch.setattr(app.db, "list_tables", lambda schema=None: [])

    return create_app({"TESTING": True}).test_client()


def test_healthz_payload_does_not_contain_secrets(health_client):
    """Acceptance из тикета: ``/healthz`` JSON не содержит DSN, password,
    token. Регрессионный guard — если future добавит, скажем, full
    DATABASE_URL в payload, тест ловит."""
    body = health_client.get("/healthz").get_data(as_text=True)
    # DSN-форма
    assert "postgresql://" not in body
    assert "clickhouse://" not in body
    # password в ключах (нечастый, но проверяем)
    assert "password" not in body.lower()
    # AWS key / Bearer
    assert not re.search(r"AKIA[0-9A-Z]{16}", body)
    assert not re.search(r"ASIA[0-9A-Z]{16}", body)
    assert "Bearer " not in body
    # Telegram bot token
    assert not re.search(r"\b\d{8,12}:[A-Za-z0-9_\-]{35}\b", body)


# ── _probe_iceberg / _probe_clickhouse — нет утечек через JSON message ─


def test_probe_iceberg_user_message_does_not_leak_aws_secret(monkeypatch):
    """Симулируем что pyiceberg бросает Exception с AWS-credentials в
    тексте (так пишет boto3 при auth-error). Final JSON message — generic
    hardcoded строка, never включает exc-text. Защита от regression если
    кто-то начнёт class str(exc) в message."""
    from app import connections
    from app.db import make_adapter_for_url

    class FakeAdapter:
        def list_namespaces(self):
            raise RuntimeError(
                "AccessDenied: AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE "
                "AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG "
                "Bearer eyJ-leaky-token"
            )

    monkeypatch.setattr(
        "app.db.make_adapter_for_url",
        lambda dsn: FakeAdapter(),
    )
    # Чтобы Iceberg-ветка действительно вошла:
    monkeypatch.setattr(connections, "make_adapter_for_url",
                        make_adapter_for_url, raising=False)

    result = connections._probe_iceberg("iceberg+rest://localhost")
    blob = " ".join(str(v) for v in result.values() if isinstance(v, str | int))
    assert "AKIAIOSFODNN7EXAMPLE" not in blob
    assert "wJalrXUtnFEMI" not in blob
    assert "eyJ" not in blob
    assert result["status"] == "error"


def test_probe_clickhouse_user_message_does_not_leak_secret(monkeypatch):
    """ClickHouse путь: exc маппится через _classify_error в один из
    предопределённых сообщений. Никаких leakов exc text в message."""
    from sqlalchemy.exc import OperationalError

    from app import connections

    class FakeEngine:
        def connect(self):
            raise OperationalError(
                "SELECT 1", {},
                Exception(
                    "Auth failed with token=Bearer leak-this-token-please "
                    "AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI"
                ),
            )
        def dispose(self):
            pass

    monkeypatch.setattr(
        connections, "create_engine",
        lambda *a, **kw: FakeEngine(),
    )
    result = connections._probe_clickhouse("clickhouse://localhost:9000/demo")
    blob = " ".join(str(v) for v in result.values() if isinstance(v, str | int))
    assert "leak-this-token" not in blob
    assert "wJalrXUtnFEMI" not in blob
    assert result["status"] == "error"
