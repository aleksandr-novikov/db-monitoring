import atexit
import logging

from apscheduler.schedulers.background import BackgroundScheduler

from app.config import settings

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None

JOB_ID = "collect_all_tables"
FORECAST_JOB_ID = "retrain_forecasts"
CHANGEPOINT_JOB_ID = "detect_changepoints"
ANOMALY_JOB_ID = "retrain_anomaly_detectors"
PURGE_FAILED_LOGINS_JOB_ID = "purge_failed_logins"


def start_scheduler(app) -> None:
    global _scheduler

    if _scheduler is not None and _scheduler.running:
        logger.debug("Scheduler already running, skipping second start")
        return

    interval = app.config.get("COLLECT_INTERVAL_MINUTES", 15)

    _scheduler = BackgroundScheduler()
    _scheduler.add_job(
        collect_all_tables,
        "interval",
        minutes=interval,
        id=JOB_ID,
        name=JOB_ID,
    )
    _scheduler.add_job(
        retrain_forecasts,
        "cron",
        hour=3,
        minute=0,
        id=FORECAST_JOB_ID,
        name=FORECAST_JOB_ID,
    )
    _scheduler.add_job(
        detect_changepoints,
        "interval",
        hours=1,
        id=CHANGEPOINT_JOB_ID,
        name=CHANGEPOINT_JOB_ID,
    )
    _scheduler.add_job(
        retrain_anomaly_detectors,
        "cron",
        hour=4,
        minute=0,
        id=ANOMALY_JOB_ID,
        name=ANOMALY_JOB_ID,
    )
    # Retention для failed_login_attempts (#56) — append-only журнал, без
    # этого джоба растёт линейно от brute-force-трафика. Daily в 02:30 —
    # окно lockout-а 15 минут, так что суточная очистка не сдвигает
    # активные счётчики.
    _scheduler.add_job(
        purge_failed_logins,
        "cron",
        hour=2,
        minute=30,
        id=PURGE_FAILED_LOGINS_JOB_ID,
        name=PURGE_FAILED_LOGINS_JOB_ID,
    )
    _scheduler.start()
    atexit.register(_scheduler.shutdown, wait=False)
    logger.info("Metrics scheduler started (interval=%d min)", interval)


def get_scheduler() -> BackgroundScheduler | None:
    return _scheduler


def collect_all_tables() -> None:
    from datetime import UTC, datetime

    from app.db import list_tables
    from app.metrics_storage import save_metrics
    from collectors.metrics_collector import MetricsCollector

    logger.info("Job %s started", JOB_ID)
    collector = MetricsCollector()
    run_ts = datetime.now(UTC)
    total_saved = 0
    for table in list_tables():
        rows = collector.collect(table["table_name"], ts=run_ts)
        if rows:
            saved = save_metrics(rows)
            total_saved += saved
            logger.debug("Saved %d metrics for table %s", saved, table["table_name"])
    logger.info("Job %s finished: %d metrics saved across all tables", JOB_ID, total_saved)

    # Schema-drift sweep runs in the same tick — same target-DB connection
    # already warm, and schema reads are cheap (information_schema).
    from collectors.schema_collector import collect_all_schemas
    counts = collect_all_schemas()
    logger.info("Schema sweep finished: %s", counts)

    # Schema drift notifications — batch events per table into one message.
    if counts["events"] > 0:
        _notify_schema_drift_events()

    # Distribution-drift кеш обновляется здесь же — тик уже прогрел
    # column_distribution, расчёт быстрый (всё внутри monitor.db).
    from ml.drift import compute_and_store_drift_all
    drift_counts = compute_and_store_drift_all()
    logger.info("Drift cache refreshed: %s", drift_counts)

    if total_saved > 0:
        _score_recent_anomalies()


def _notify_schema_drift_events() -> None:
    from datetime import timedelta

    from app.db import list_tables
    from app.metrics_storage import get_schema_events
    from app.notifications.telegram import notify_schema_drift

    window = timedelta(minutes=settings.COLLECT_INTERVAL_MINUTES + 5)
    for t in list_tables():
        name = t["table_name"]
        try:
            events = get_schema_events(name, window=window)
            if events:
                notify_schema_drift(name, events)
        except Exception as exc:
            logger.warning("Schema drift notification failed for %s: %s", name, exc)


def _score_recent_anomalies() -> None:
    """Score the last 24 h of metrics for each table using the persisted model.

    Runs after every collection tick. Silently skips tables whose model has
    not been trained yet — the nightly retrain job handles the initial scoring.
    """
    from app.db import list_tables
    from app.metrics_storage import save_anomaly_scores
    from app.notifications.telegram import notify_anomaly
    from ml.anomaly_detector import InsufficientDataError, score_table

    for t in list_tables():
        name = t["table_name"]
        try:
            scores = score_table(name, window_days=1)
            if scores:
                save_anomaly_scores([{**s, "table_name": name} for s in scores])
                anomalies = [s for s in scores if s["is_anomaly"]]
                if anomalies:
                    latest = max(anomalies, key=lambda s: s["ts"])
                    try:
                        notify_anomaly(name, latest["ts"], latest["score"])
                    except Exception as exc:
                        logger.warning("Anomaly notification failed for %s: %s", name, exc)
        except InsufficientDataError:
            pass
        except Exception as exc:
            logger.warning("Anomaly scoring skipped for %s: %s", name, exc)


def retrain_forecasts() -> None:
    from ml.forecast import retrain_all

    logger.info("Job %s started", FORECAST_JOB_ID)
    counts = retrain_all()
    logger.info("Job %s finished: %s", FORECAST_JOB_ID, counts)


def detect_changepoints() -> None:
    from app.notifications.telegram import notify_changepoint
    from ml.changepoint import detect_all

    logger.info("Job %s started", CHANGEPOINT_JOB_ID)
    counts = detect_all()
    logger.info("Job %s finished: detected=%d tables=%d errors=%d",
                CHANGEPOINT_JOB_ID, counts["detected"], counts["tables"], counts["errors"])

    for event in counts.get("events", []):
        try:
            notify_changepoint(
                event["table_name"],
                event["metric_name"],
                event["value_before"],
                event["value_after"],
                event["ts"],
            )
        except Exception as exc:
            logger.warning("Changepoint notification failed: %s", exc)


def retrain_anomaly_detectors() -> None:
    from app.db import list_tables
    from app.metrics_storage import save_anomaly_scores
    from ml.anomaly_detector import InsufficientDataError, retrain_all, score_table

    logger.info("Job %s started", ANOMALY_JOB_ID)
    counts = retrain_all()
    logger.info("Anomaly models retrained: %s", counts)

    # After retraining, score the full 14-day history for every table so the
    # dashboard has up-to-date annotations without waiting for collect ticks.
    scored = 0
    for t in list_tables():
        name = t["table_name"]
        try:
            scores = score_table(name, window_days=14)
            if scores:
                save_anomaly_scores([{**s, "table_name": name} for s in scores])
                scored += len(scores)
        except InsufficientDataError:
            pass
        except Exception as exc:
            logger.warning("Post-retrain scoring failed for %s: %s", name, exc)
    logger.info("Job %s finished: %d scores saved", ANOMALY_JOB_ID, scored)


def purge_failed_logins() -> None:
    """Daily retention for the failed_login_attempts table (#56).

    Drops records older than 30 days. The lockout window itself is 15 min,
    so a 30d retention keeps ~enough audit trail to investigate sustained
    attacks without growing the table indefinitely.
    """
    from datetime import timedelta

    from app.metrics_storage import purge_old_failed_logins

    deleted = purge_old_failed_logins(retention=timedelta(days=30))
    logger.info("Job %s finished: %d failed_login_attempts purged",
                PURGE_FAILED_LOGINS_JOB_ID, deleted)
