"""Tests for live_demo._run_collector_tick tenant scoping (#138)."""
from unittest.mock import patch


def test_run_collector_tick_with_connection_calls_per_project():
    """_run_collector_tick с connection_id вызывает collect_for_connection."""
    mock_latest = {"value": 42.0}
    with (
        patch("collectors.per_project.collect_for_connection") as mock_collect,
        patch("app.metrics_storage.get_latest_metric", return_value=mock_latest),
    ):
        from scripts.live_demo import _run_collector_tick
        result = _run_collector_tick("proj-a", "conn-a")

    mock_collect.assert_called_once_with("proj-a", "conn-a")
    assert result == 42


def test_run_collector_tick_with_connection_reads_from_project():
    """_run_collector_tick с connection_id читает метрику из project_id, не из legacy."""
    with (
        patch("collectors.per_project.collect_for_connection"),
        patch("app.metrics_storage.get_latest_metric", return_value=None) as mock_get,
    ):
        from scripts.live_demo import _run_collector_tick
        _run_collector_tick("proj-a", "conn-a")

    args = mock_get.call_args
    assert args[0][2] == "proj-a", "должен читать метрику из proj-a, не из legacy"


def test_run_collector_tick_no_connection_uses_global_collector():
    """_run_collector_tick без connection_id использует глобальный collect_all_tables."""
    with (
        patch("collectors.scheduler.collect_all_tables") as mock_collect,
        patch("app.metrics_storage.get_latest_metric", return_value=None),
    ):
        from scripts.live_demo import _run_collector_tick
        _run_collector_tick("legacy", None)

    mock_collect.assert_called_once()


def test_run_collector_tick_no_connection_reads_from_legacy():
    """_run_collector_tick без connection_id читает метрику из 'legacy', не из project_id."""
    with (
        patch("collectors.scheduler.collect_all_tables"),
        patch("app.metrics_storage.get_latest_metric", return_value=None) as mock_get,
    ):
        from scripts.live_demo import _run_collector_tick
        _run_collector_tick("some-project", None)

    args = mock_get.call_args
    assert args[0][2] == "legacy", "без connection_id collector пишет в legacy"


def test_live_demo_cli_requires_connection_id_for_non_legacy():
    """--project-id != legacy без --connection-id завершается с ошибкой."""
    import subprocess
    result = subprocess.run(
        ["python", "-m", "scripts.live_demo",
         "--project-id", "proj-x", "--dry-run"],
        capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert "connection-id" in result.stderr.lower()
