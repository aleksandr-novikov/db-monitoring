"""Backup + restore smoke tests (#106).

End-to-end: seed a SQLite DB → backup.sh produces a dump + sidecar →
restore.sh validates the sidecar and reproduces the same data. Covers the
acceptance criterion "backup → restore on a clean DB → SELECT count(*)
matches" without needing a real Postgres in CI.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
BACKUP_SH = REPO_ROOT / "scripts" / "backup.sh"
RESTORE_SH = REPO_ROOT / "scripts" / "restore.sh"


def _seed_sqlite(path: Path, *, n_users: int = 5) -> None:
    con = sqlite3.connect(path)
    con.executescript(
        "CREATE TABLE users(id INT PRIMARY KEY, email TEXT);\n"
        + "\n".join(
            f"INSERT INTO users VALUES({i}, 'u{i}@example.com');"
            for i in range(1, n_users + 1)
        )
    )
    con.commit()
    con.close()


def _run(cmd: list[str], **env_extra: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env.update(env_extra)
    return subprocess.run(
        cmd, env=env, capture_output=True, text=True, check=False,
    )


# ── Backup ─────────────────────────────────────────────────────────────────


def test_backup_sqlite_produces_dump_and_sha(tmp_path):
    """backup.sh writes <prefix>.sqlite.gz + .sha256 sidecar."""
    src = tmp_path / "monitor.db"
    _seed_sqlite(src, n_users=3)
    backup_dir = tmp_path / "bkup"

    proc = _run(
        ["bash", str(BACKUP_SH)],
        DATABASE_URL="",
        MONITOR_DB_URL=f"sqlite:///{src}",
        BACKUP_DIR=str(backup_dir),
    )
    assert proc.returncode == 0, proc.stderr

    dumps = list(backup_dir.glob("metrics-*.sqlite.gz"))
    sha = list(backup_dir.glob("metrics-*.sqlite.gz.sha256"))
    assert len(dumps) == 1, f"expected one dump, got {dumps}"
    assert len(sha) == 1
    # Sidecar has 64-char hex sha256.
    sha_value = sha[0].read_text().split()[0]
    assert len(sha_value) == 64
    assert all(c in "0123456789abcdef" for c in sha_value)


def test_backup_skips_when_url_is_empty(tmp_path):
    """Empty DATABASE_URL / MONITOR_DB_URL → script exits cleanly without
    producing files. Operators set only one of the two when running ad-hoc."""
    backup_dir = tmp_path / "bkup"
    proc = _run(
        ["bash", str(BACKUP_SH)],
        DATABASE_URL="",
        MONITOR_DB_URL="",
        BACKUP_DIR=str(backup_dir),
    )
    assert proc.returncode == 0, proc.stderr
    assert list(backup_dir.glob("*.gz")) == []


# ── Restore ────────────────────────────────────────────────────────────────


def test_restore_round_trip_sqlite(tmp_path):
    """Acceptance: SELECT count(*) FROM users after restore must match."""
    src = tmp_path / "monitor.db"
    _seed_sqlite(src, n_users=42)
    backup_dir = tmp_path / "bkup"

    _run(
        ["bash", str(BACKUP_SH)],
        DATABASE_URL="",
        MONITOR_DB_URL=f"sqlite:///{src}",
        BACKUP_DIR=str(backup_dir),
    )
    dump = next(backup_dir.glob("metrics-*.sqlite.gz"))

    restored = tmp_path / "restored.db"
    proc = _run(
        ["bash", str(RESTORE_SH), str(dump)],
        RESTORE_PATH=str(restored),
    )
    assert proc.returncode == 0, proc.stderr

    con = sqlite3.connect(restored)
    n = con.execute("SELECT count(*) FROM users").fetchone()[0]
    con.close()
    assert n == 42


def test_restore_refuses_corrupted_dump(tmp_path):
    """Tamper with the dump bytes → sha256 mismatch → exit 1, no restore."""
    src = tmp_path / "monitor.db"
    _seed_sqlite(src)
    backup_dir = tmp_path / "bkup"

    _run(
        ["bash", str(BACKUP_SH)],
        DATABASE_URL="",
        MONITOR_DB_URL=f"sqlite:///{src}",
        BACKUP_DIR=str(backup_dir),
    )
    dump = next(backup_dir.glob("metrics-*.sqlite.gz"))

    # Flip one byte in the middle of the gzipped dump.
    data = bytearray(dump.read_bytes())
    data[len(data) // 2] ^= 0xFF
    dump.write_bytes(data)

    proc = _run(
        ["bash", str(RESTORE_SH), str(dump)],
        RESTORE_PATH=str(tmp_path / "should-not-exist.db"),
    )
    assert proc.returncode == 1
    assert "sha256 mismatch" in proc.stderr
    assert not (tmp_path / "should-not-exist.db").exists()


def test_restore_refuses_missing_sidecar(tmp_path):
    """Sidecar removed → cannot verify integrity → refuse."""
    src = tmp_path / "monitor.db"
    _seed_sqlite(src)
    backup_dir = tmp_path / "bkup"

    _run(
        ["bash", str(BACKUP_SH)],
        DATABASE_URL="",
        MONITOR_DB_URL=f"sqlite:///{src}",
        BACKUP_DIR=str(backup_dir),
    )
    dump = next(backup_dir.glob("metrics-*.sqlite.gz"))
    sidecar = backup_dir / f"{dump.name}.sha256"
    sidecar.unlink()

    proc = _run(["bash", str(RESTORE_SH), str(dump)])
    assert proc.returncode == 1
    assert "missing sidecar" in proc.stderr


# ── /admin/rollback-checklist (#107) ───────────────────────────────────────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    import app.metrics_storage as storage
    from app.app import create_app
    from app.config import settings as cfg

    db_path = tmp_path / "admin.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    import app.db
    monkeypatch.setattr(app.db, "list_tables", lambda schema=None: [])

    app = create_app({"TESTING": True})
    return app.test_client()


def test_rollback_checklist_renders(admin_client):
    """Page renders with each step + a final "confirm all-clear" section."""
    resp = admin_client.get("/admin/rollback-checklist")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    # Every step heading is present.
    for marker in [
        "1. Подтвердить инцидент",
        "2. Идентифицировать виновную фичу",
        "3. Попробовать feature flag",
        "4. Откат на :previous образ",
        "5. Восстановление из бэкапа",
        "6. Подтвердить отбой",
    ]:
        assert marker in body, f"missing step: {marker}"
    # Link to the full runbook is present (operators may want the full text).
    assert "docs/runbooks/rollback.md" in body


# Skip backup tests on Windows-ish CI (just-in-case future portability check).
pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None, reason="backup scripts require bash"
)
