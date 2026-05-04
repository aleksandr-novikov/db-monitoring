import atexit
import logging

from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None

JOB_ID = "collect_all_tables"
FORECAST_JOB_ID = "retrain_forecasts"
CHANGEPOINT_JOB_ID = "detect_changepoints"
ANOMALY_JOB_ID = "retrain_anomaly_detectors"


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
    _scheduler.start()
    atexit.register(_scheduler.shutdown, wait=False)
    logger.info("Metrics scheduler started (interval=%d min)", interval)


def get_scheduler() -> BackgroundScheduler | None:
    return _scheduler


def collect_all_tables() -> None:
    from app.db import list_tables
    from app.metrics_storage import save_metrics
    from collectors.metrics_collector import MetricsCollector

    logger.info("Job %s started", JOB_ID)
    collector = MetricsCollector()
    total_saved = 0
    for table in list_tables():
        rows = collector.collect(table["table_name"])
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

    if total_saved > 0:
        _score_recent_anomalies()


def _score_recent_anomalies() -> None:
    """Score the last 24 h of metrics for each table using the persisted model.

    Runs after every collection tick. Silently skips tables whose model has
    not been trained yet — the nightly retrain job handles the initial scoring.
    """
    from ml.anomaly_detector import InsufficientDataError, score_table
    from app.db import list_tables
    from app.metrics_storage import save_anomaly_scores

    for t in list_tables():
        name = t["table_name"]
        try:
            scores = score_table(name, window_days=1)
            if scores:
                save_anomaly_scores([{**s, "table_name": name} for s in scores])
        except InsufficientDataError:
            pass
        except Exception as exc:
            logger.debug("Anomaly scoring skipped for %s: %s", name, exc)


def retrain_forecasts() -> None:
    from ml.forecast import retrain_all

    logger.info("Job %s started", FORECAST_JOB_ID)
    counts = retrain_all()
    logger.info("Job %s finished: %s", FORECAST_JOB_ID, counts)


def detect_changepoints() -> None:
    from ml.changepoint import detect_all

    logger.info("Job %s started", CHANGEPOINT_JOB_ID)
    counts = detect_all()
    logger.info("Job %s finished: %s", CHANGEPOINT_JOB_ID, counts)


def retrain_anomaly_detectors() -> None:
    from ml.anomaly_detector import InsufficientDataError, retrain_all, score_table
    from app.db import list_tables
    from app.metrics_storage import save_anomaly_scores

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
