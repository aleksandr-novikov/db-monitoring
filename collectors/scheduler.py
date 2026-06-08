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

    # #54: register per-connection collection jobs for every active
    # row in `connections`. The global `collect_all_tables` above stays
    # registered for backward-compat (`legacy` tenant), but new tenant
    # data flows through the per-project jobs registered here.
    from collectors.per_project import register_jobs_for_all_active_connections

    try:
        register_jobs_for_all_active_connections(_scheduler)
    except Exception as exc:
        logger.warning("per-project job registration failed: %s", exc)


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
    # #53: every metrics row carries a project_id. The global collector
    # tick predates per-project scheduling (#54 will reroute this), so we
    # write to the 'legacy' tenant — same bucket the schema migration
    # backfills onto existing rows.
    project_id = "legacy"
    for table in list_tables():
        rows = collector.collect(table["table_name"], ts=run_ts)
        if rows:
            saved = save_metrics(rows, project_id)
            total_saved += saved
            logger.debug("Saved %d metrics for table %s", saved, table["table_name"])
    logger.info("Job %s finished: %d metrics saved across all tables", JOB_ID, total_saved)

    # Schema-drift sweep runs in the same tick — same target-DB connection
    # already warm, and schema reads are cheap (information_schema).
    from collectors.schema_collector import collect_all_schemas
    counts = collect_all_schemas()
    logger.info("Schema sweep finished: %s", counts)

    # Schema drift notifications — runs unconditionally so per-project events
    # written by per_project.py are also dispatched, not just legacy sweep events.
    _notify_schema_drift_events()

    # Distribution-drift кеш обновляется здесь же — тик уже прогрел
    # column_distribution, расчёт быстрый (всё внутри monitor.db).
    from ml.drift import compute_and_store_drift_all
    drift_counts = compute_and_store_drift_all()
    logger.info("Drift cache refreshed: %s", drift_counts)

    if total_saved > 0:
        _score_recent_anomalies()


def _legacy_telegram_config() -> tuple[str | None, str | None, int | None]:
    """Look up Telegram config for the ``'legacy'`` tenant — the single-tenant
    bucket the global scheduler runs under.

    Returns ``(bot_token_or_none, chat_id_or_none, throttle_minutes_or_none)``.
    All three None means no notification will be sent — and that's the
    point: post-#143 there's no global ``settings.TELEGRAM_*`` fallback,
    so the legacy scheduler is mute unless an explicit ``project_id='legacy'``
    row exists in ``project_notifications``. Per-tenant alerts will be
    wired into the per-project collector path in a follow-up ticket.
    """
    from app import crypto
    from app.metrics_storage import get_project_notifications

    cfg = get_project_notifications("legacy")
    if cfg is None:
        return None, None, None
    token_encrypted = cfg.get("telegram_bot_token")
    chat_id = cfg.get("telegram_chat_id")
    throttle = cfg.get("throttle_minutes")
    bot_token = None
    if token_encrypted:
        try:
            bot_token = crypto.decrypt_token(token_encrypted)
        except crypto.InvalidToken:
            logger.warning(
                "legacy project_notifications.telegram_bot_token failed to "
                "decrypt — key rotation without re-encryption?"
            )
    return bot_token, chat_id, throttle


def _iter_notification_projects() -> list[str]:
    """Return project_ids to iterate for per-tenant notifications.

    Always includes 'legacy' so the global scheduler path keeps working,
    even when legacy has no Telegram config (notifications are skipped via
    load_project_telegram_config returning None).
    """
    from app.metrics_storage import list_project_ids_with_telegram
    project_ids = list_project_ids_with_telegram()
    if "legacy" not in project_ids:
        project_ids = ["legacy", *project_ids]
    return project_ids


def _notify_schema_drift_events() -> None:
    from datetime import timedelta

    from app.metrics_storage import get_schema_events, list_metric_tables
    from app.notifications.telegram import load_project_telegram_config, notify_schema_drift

    window = timedelta(minutes=settings.COLLECT_INTERVAL_MINUTES + 5)
    for project_id in _iter_notification_projects():
        cfg = load_project_telegram_config(project_id)
        if cfg is None:
            continue

        tables = list_metric_tables(project_id) or (None if project_id == "legacy" else [])
        if tables is None:
            from app.db import list_tables
            tables = [t["table_name"] for t in list_tables()]

        bot_token, chat_id, throttle = cfg
        for name in tables:
            try:
                events = get_schema_events(name, project_id=project_id, window=window)
                if events:
                    notify_schema_drift(
                        project_id, name, events,
                        bot_token=bot_token, chat_id=chat_id,
                        throttle_minutes=throttle,
                    )
            except Exception as exc:
                logger.warning(
                    "[project=%s] schema drift notification failed for %s: %s",
                    project_id, name, exc,
                )


def _score_recent_anomalies() -> None:
    """Score the last 24 h of metrics for each table using the persisted model.

    Runs after every collection tick. Silently skips tables whose model has
    not been trained yet — the nightly retrain job handles the initial scoring.
    """
    from app.db import list_tables
    from app.metrics_storage import save_anomaly_scores
    from app.notifications.telegram import notify_anomaly
    from ml.anomaly_detector import InsufficientDataError, score_table

    bot_token, chat_id, throttle = _legacy_telegram_config()
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
                        notify_anomaly(
                            "legacy", name, latest["ts"], latest["score"],
                            bot_token=bot_token, chat_id=chat_id,
                            throttle_minutes=throttle,
                        )
                    except Exception as exc:
                        logger.warning("Anomaly notification failed for %s: %s", name, exc)
        except InsufficientDataError:
            pass
        except Exception as exc:
            logger.warning("Anomaly scoring skipped for %s: %s", name, exc)


def _iter_retrain_projects() -> list[str]:
    """Project ids that need ML retraining: any project with metric rows
    plus ``'legacy'`` for back-compat with single-tenant setups."""
    from app.metrics_storage import list_project_ids_with_metrics
    ids = list_project_ids_with_metrics()
    if "legacy" not in ids:
        ids = ["legacy", *ids]
    return ids


def retrain_forecasts() -> None:
    from app.metrics_storage import list_metric_tables
    from ml.forecast import retrain_all

    logger.info("Job %s started", FORECAST_JOB_ID)
    total: dict[str, int] = {"trained": 0, "skipped": 0, "errors": 0}
    for project_id in _iter_retrain_projects():
        # list_metric_tables=[] when a tenant has no metrics yet → skip
        # entirely (no global-DB fallback for tenants; only legacy may
        # introspect ``app.db.list_tables``, which retrain_all does
        # internally when tables=None).
        tables = list_metric_tables(project_id)
        if not tables and project_id != "legacy":
            continue
        counts = retrain_all(project_id=project_id, tables=tables or None)
        for k, v in counts.items():
            total[k] = total.get(k, 0) + v
        logger.debug("[project=%s] forecast retrain: %s", project_id, counts)
    logger.info("Job %s finished: %s", FORECAST_JOB_ID, total)


def detect_changepoints() -> None:
    from app.metrics_storage import list_metric_tables
    from app.notifications.telegram import load_project_telegram_config, notify_changepoint
    from ml.changepoint import detect_all

    logger.info("Job %s started", CHANGEPOINT_JOB_ID)

    project_ids = _iter_notification_projects()
    total_detected = 0
    for project_id in project_ids:
        tables = list_metric_tables(project_id) or (None if project_id == "legacy" else [])
        counts = detect_all(project_id=project_id, tables=tables)
        total_detected += counts.get("detected", 0)
        logger.debug(
            "[project=%s] changepoints: detected=%d tables=%d errors=%d",
            project_id, counts["detected"], counts["tables"], counts["errors"],
        )

        cfg = load_project_telegram_config(project_id)
        if cfg is None:
            continue
        bot_token, chat_id, throttle = cfg
        for event in counts.get("events", []):
            try:
                notify_changepoint(
                    project_id,
                    event["table_name"],
                    event["metric_name"],
                    event["value_before"],
                    event["value_after"],
                    event["ts"],
                    bot_token=bot_token, chat_id=chat_id,
                    throttle_minutes=throttle,
                )
            except Exception as exc:
                logger.warning(
                    "[project=%s] changepoint notification failed: %s", project_id, exc,
                )

    logger.info("Job %s finished: total detected=%d across %d projects",
                CHANGEPOINT_JOB_ID, total_detected, len(project_ids))


def retrain_anomaly_detectors() -> None:
    from app.metrics_storage import list_metric_tables, save_anomaly_scores
    from ml.anomaly_detector import InsufficientDataError, retrain_all, score_table

    logger.info("Job %s started", ANOMALY_JOB_ID)
    total_counts: dict[str, int] = {"trained": 0, "skipped": 0, "errors": 0}
    scored = 0
    for project_id in _iter_retrain_projects():
        tables = list_metric_tables(project_id)
        if not tables and project_id != "legacy":
            continue
        counts = retrain_all(project_id=project_id, tables=tables or None)
        for k, v in counts.items():
            total_counts[k] = total_counts.get(k, 0) + v
        logger.debug("[project=%s] anomaly retrain: %s", project_id, counts)

        # Post-retrain scoring lives in the same per-project loop so the
        # dashboard annotations align with the freshly-trained model.
        # Legacy may have a single-tenant ``DATABASE_URL`` to live-list
        # tables from when no metric rows are stored yet (fresh install
        # before the first tick). Tenants always use stored metric tables.
        if project_id == "legacy" and not tables:
            try:
                from app.db import list_tables as _legacy_list_tables
                score_targets = [t["table_name"] for t in _legacy_list_tables()]
            except Exception as exc:
                # Multi-tenant deploys often have no global DATABASE_URL —
                # don't crash the whole retrain job because of it.
                logger.debug("legacy live list_tables() unavailable: %s", exc)
                score_targets = []
        else:
            score_targets = tables
        for name in score_targets:
            try:
                scores = score_table(name, window_days=14, project_id=project_id)
                if scores:
                    save_anomaly_scores(
                        [{**s, "table_name": name} for s in scores],
                        project_id=project_id,
                    )
                    scored += len(scores)
            except InsufficientDataError:
                pass
            except Exception as exc:
                logger.warning(
                    "[project=%s] post-retrain scoring failed for %s: %s",
                    project_id, name, exc,
                )
    logger.info("Anomaly models retrained: %s", total_counts)
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
