import atexit
import logging

from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def start_scheduler(app) -> None:
    global _scheduler

    if _scheduler is not None and _scheduler.running:
        logger.debug("Scheduler already running, skipping second start")
        return

    interval = app.config.get("COLLECT_INTERVAL_MINUTES", 15)

    _scheduler = BackgroundScheduler()
    _scheduler.add_job(_collect_all, "interval", minutes=interval)
    _scheduler.start()
    atexit.register(_scheduler.shutdown, wait=False)
    logger.info("Metrics scheduler started (interval=%d min)", interval)


def _collect_all() -> None:
    from app.db import list_tables
    from app.metrics_storage import save_metrics
    from collectors.metrics_collector import MetricsCollector

    collector = MetricsCollector()
    for table in list_tables():
        rows = collector.collect(table["table_name"])
        if rows:
            saved = save_metrics(rows)
            logger.debug("Saved %d metrics for table %s", saved, table["table_name"])
