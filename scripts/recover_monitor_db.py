"""Auto-recover corrupted monitor.db (#213).

Шаги в строгом порядке:
1. Если monitor.db существует — переименовать в monitor.db.broken.<ts>
   (preserve для post-mortem; не удаляем).
2. Запустить timescaledb через docker compose (если ещё не запущен).
3. Дождаться готовности TimescaleDB.
4. Вызвать seed_demo_workspace + demo_prepare для воссоздания демо-данных.
5. Verify через /healthz (если app поднят) — backend должен быть postgresql.

Idempotent: повторный запуск не вредит. broken-файлы суффиксированы
timestamp-ом, не перезаписываются.

Usage:
    python -m scripts.recover_monitor_db
    python -m scripts.recover_monitor_db --skip-demo-prepare  # только preserve + Timescale
    python -m scripts.recover_monitor_db --health-url http://localhost:5001/healthz
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
MONITOR_DB = REPO_ROOT / "monitor.db"


def preserve_broken_db() -> Path | None:
    """Переименовать monitor.db в monitor.db.broken.<unix-ts>. Возвращает
    путь к broken-файлу или None если ничего не было.

    Не делаем integrity_check — preserve безусловно, файл уйдёт в архив.
    operator потом может проверить broken-файл руками.
    """
    if not MONITOR_DB.exists():
        logger.info("monitor.db не существует — нечего сохранять")
        return None
    ts = int(datetime.now(UTC).timestamp())
    target = MONITOR_DB.with_suffix(f".db.broken.{ts}")
    MONITOR_DB.rename(target)
    logger.info("Saved %s → %s for post-mortem", MONITOR_DB.name, target.name)
    return target


def ensure_timescale_running(timeout_s: int = 60) -> None:
    """Запустить timescaledb-сервис через docker compose и дождаться
    healthy. Если уже healthy — no-op."""
    state = _container_health("db-monitoring-timescale")
    if state == "healthy":
        logger.info("timescaledb уже healthy")
        return
    logger.info("Запускаю timescaledb...")
    subprocess.run(
        ["docker", "compose", "up", "-d", "timescaledb"],
        cwd=REPO_ROOT, check=True,
    )
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _container_health("db-monitoring-timescale") == "healthy":
            logger.info("timescaledb healthy")
            return
        time.sleep(2)
    raise SystemExit(f"timescaledb не стал healthy за {timeout_s}s")


def _container_health(name: str) -> str:
    """Return health status of a docker container, or 'absent' if not running."""
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Health.Status}}", name],
            capture_output=True, text=True, check=False,
        )
        return result.stdout.strip() if result.returncode == 0 else "absent"
    except (FileNotFoundError, OSError):
        return "absent"


def run_demo_prepare() -> None:
    """seed_demo_workspace + 14-day history + ML warmup."""
    logger.info("Запускаю make demo-prepare...")
    subprocess.run(
        [sys.executable, "-m", "scripts.demo_prepare"],
        cwd=REPO_ROOT, check=True,
    )


def verify_via_healthz(url: str, timeout_s: int = 10) -> bool:
    """Hit /healthz и проверить что backend = postgresql."""
    import json
    import urllib.error
    import urllib.request

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as resp:
                body = json.loads(resp.read())
            backend = body.get("checks", {}).get("monitor_db", {}).get("backend")
            if backend == "postgresql":
                logger.info("/healthz OK: backend=%s", backend)
                return True
            logger.warning(
                "/healthz monitor_db.backend = %r (expected postgresql)",
                backend,
            )
            return False
        except (urllib.error.URLError, json.JSONDecodeError, OSError):
            time.sleep(2)
    logger.warning("Не смог достучаться до %s за %ds — app не запущен?",
                    url, timeout_s)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--skip-demo-prepare", action="store_true",
        help="Только preserve broken-файла + запуск Timescale, не seed",
    )
    parser.add_argument(
        "--skip-timescale-up", action="store_true",
        help="Не пытаться поднимать timescaledb (если на host-Timescale)",
    )
    parser.add_argument(
        "--health-url",
        default="http://localhost:5001/healthz",
        help="URL для verify-step (skip если app не локально)",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
    )

    print("[1/4] Preserve broken monitor.db")
    preserved = preserve_broken_db()
    if preserved:
        print(f"  → {preserved.name}")

    if not args.skip_timescale_up:
        print("[2/4] Ensure timescaledb up")
        ensure_timescale_running()

    if not args.skip_demo_prepare:
        print("[3/4] Reseed demo workspace + 14-day history + ML warmup")
        run_demo_prepare()
    else:
        print("[3/4] Demo-prepare skipped (--skip-demo-prepare)")

    print("[4/4] Verify /healthz")
    if verify_via_healthz(args.health_url):
        print("\nRecovery complete. /dashboard/notifications должен снова работать.")
        return 0
    else:
        print(
            "\n⚠️ /healthz не подтвердил Timescale-backend — "
            "проверь app логи и docker compose ps."
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
