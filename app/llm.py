"""LLM root-cause explanation via NVIDIA NIM (#36).

explain_anomaly(table, metric, ts) is the public entry point.
It checks the cache first, calls NIM if needed, falls back to rule-based
explanations when NIM is unavailable.
"""

import json
import re
from datetime import UTC, datetime, timedelta

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.config import settings
from app.metrics_storage import (
    get_anomaly_scores,
    get_changepoints,
    get_metrics,
    get_schema_snapshot,
)


def _context_window(ts: str) -> timedelta:
    """Return a timedelta that covers `ts` plus a 2-hour buffer from now."""
    try:
        ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if ts_dt.tzinfo is None:
            ts_dt = ts_dt.replace(tzinfo=UTC)
        age = datetime.now(UTC) - ts_dt
        return max(timedelta(hours=48), age + timedelta(hours=2))
    except (ValueError, TypeError):
        return timedelta(hours=48)


def _summary_stats(values: list[float]) -> dict | None:
    """Sample-statistics for a value series — feeds the LLM historical
    context block (#171). Returns None if too few points to summarise."""
    cleaned = [float(v) for v in values if v is not None]
    if len(cleaned) < 3:
        return None
    sorted_v = sorted(cleaned)
    n = len(sorted_v)
    median = sorted_v[n // 2] if n % 2 else (sorted_v[n // 2 - 1] + sorted_v[n // 2]) / 2
    mean = sum(cleaned) / n
    return {
        "min": sorted_v[0],
        "max": sorted_v[-1],
        "median": median,
        "mean": mean,
        "n": n,
    }


def _format_history_block(
    rc_stats: dict | None, current_rc: float | None,
    nr_stats: dict | None, current_nr: float | None,
) -> str:
    """Human-readable history block for the prompt. Empty string if no
    historical stats — avoids planting a placeholder that the LLM might
    treat as authoritative."""
    lines: list[str] = []
    if rc_stats and current_rc is not None and rc_stats["median"] > 0:
        ratio = current_rc / rc_stats["median"]
        lines.append(
            f"Historical row_count (last 48 h, n={rc_stats['n']}): "
            f"min={int(rc_stats['min'])}, median={int(rc_stats['median'])}, "
            f"max={int(rc_stats['max'])}. "
            f"Current value at the anomaly: {int(current_rc)} "
            f"({ratio:.2f}x of median)."
        )
    if nr_stats and current_nr is not None and nr_stats["median"] >= 0:
        delta = current_nr - nr_stats["median"]
        lines.append(
            f"Historical null_rate (last 48 h, n={nr_stats['n']}): "
            f"min={nr_stats['min']:.3f}, median={nr_stats['median']:.3f}, "
            f"max={nr_stats['max']:.3f}. "
            f"Current value at the anomaly: {current_nr:.3f} "
            f"(Δ={delta:+.3f})."
        )
    if not lines:
        return ""
    return "Historical context:\n" + "\n".join(f"  {ln}" for ln in lines)


def _build_prompt(
    table: str, metric: str, ts: str, project_id: str = "legacy"
) -> str:
    schema = get_schema_snapshot(table) or []
    schema_text = ", ".join(
        f"{c['name']} {c['type']}{'?' if c.get('nullable') else ''}"
        for c in schema
    ) or "unknown"

    window = _context_window(ts)
    recent_rc = get_metrics(table, "row_count", project_id, window=window)
    recent_nr = get_metrics(table, "null_rate", project_id, window=window)
    changepoints = get_changepoints(
        table, window=max(timedelta(days=14), window), project_id=project_id
    )
    anomaly_scores = get_anomaly_scores(table, project_id=project_id, window=window)

    # Limit context to rows at or before ts so the LLM sees the state
    # at the moment of the anomaly, not current state.
    recent_rc = [r for r in recent_rc if r["ts"] <= ts]
    recent_nr = [r for r in recent_nr if r["ts"] <= ts]

    def _fmt(raw_ts: str) -> str:
        return raw_ts[:16].replace("T", " ") + " UTC"

    rc_sample = [
        f"{_fmt(r['ts'])}: {int(r['value'])}" for r in recent_rc[-10:]
    ]
    nr_sample = [
        f"{_fmt(r['ts'])}: {r['value']:.3f}" for r in recent_nr[-10:]
    ]
    cp_text = "\n".join(
        f"  {_fmt(c['ts'])} {c['metric_name']}: {c['value_before']:.2f} → {c['value_after']:.2f}"
        for c in changepoints
    ) or "  none"
    anomaly_at_ts = next(
        (a for a in anomaly_scores if a["ts"] == ts and a["is_anomaly"]), None
    )
    anomaly_score_text = (
        f"{anomaly_at_ts['score']:.4f}" if anomaly_at_ts else "not available"
    )
    ts_fmt = _fmt(ts)

    # #171: подмешиваем сводный исторический контекст — без него LLM
    # часто отвечает шаблонно («возможный сбой в системе сбора»). С
    # явным baseline + multiple легко сформулировать конкретный
    # «8x от типичного прироста» как в acceptance issue.
    rc_stats = _summary_stats([r["value"] for r in recent_rc])
    nr_stats = _summary_stats([r["value"] for r in recent_nr])
    current_rc = recent_rc[-1]["value"] if recent_rc else None
    current_nr = recent_nr[-1]["value"] if recent_nr else None
    history_block = _format_history_block(
        rc_stats, current_rc, nr_stats, current_nr
    )

    return f"""You are a database reliability expert. Analyze the anomaly below and explain its root cause.

Table: {table}
Schema: {schema_text}
Anomaly detected at: {ts_fmt}
Trigger metric: {metric}
Isolation Forest score at {ts_fmt}: {anomaly_score_text}

IMPORTANT: The IsolationForest detector has CONFIRMED this point is an anomaly
(is_anomaly=1, score is negative). Your job is to explain WHY this looks
anomalous given the data, not to argue whether it is. If the data truly
looks stable to you, state briefly that the deviation is borderline
(e.g. "small deviation from baseline") — do NOT write "no anomaly" or
"data is stable" as the main explanation.

Recent row_count (last 48 h):
{chr(10).join(rc_sample) or "  no data"}

Recent null_rate (last 48 h):
{chr(10).join(nr_sample) or "  no data"}

{history_block}

Recent change-points (last 14 days):
{cp_text}

Based on this context, provide a root-cause explanation grounded in the
actual numbers (e.g. "вырос на X строк, это в Yx выше среднего за 7 дней").
Avoid generic phrases like "возможный сбой в системе сбора данных" unless
the data actually supports that hypothesis.

Respond ONLY with valid JSON (no markdown, no extra text):
{{"explanation": "...", "suggested_fix": "...", "confidence": 0.85}}

Respond in Russian."""


@retry(
    retry=retry_if_exception_type(httpx.RequestError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _call_nim(prompt: str) -> str:
    """Call NIM /chat/completions. Retries only on network errors, not 4xx/5xx."""
    url = settings.NIM_BASE_URL.rstrip("/") + "/chat/completions"
    payload = {
        "model": settings.NIM_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
        "max_tokens": 512,
    }
    with httpx.Client(timeout=httpx.Timeout(60.0)) as client:
        resp = client.post(
            url,
            json=payload,
            headers={
                "Authorization": f"Bearer {settings.NIM_API_KEY}",
                "Content-Type": "application/json",
            },
        )
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"]


def _parse_nim_response(raw: str) -> dict:
    """Parse LLM output → {explanation, suggested_fix, confidence}.

    Handles clean JSON and JSON wrapped in markdown code fences.
    Falls back to None if parsing fails entirely (caller must handle).
    """
    text = raw.strip()

    # Strip markdown code fence if present
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        # Try to extract first JSON object from the string
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if not m:
            return {}
        try:
            parsed = json.loads(m.group())
        except json.JSONDecodeError:
            return {}

    explanation = str(parsed.get("explanation") or "").strip()
    suggested_fix = str(parsed.get("suggested_fix") or "").strip()
    raw_conf = parsed.get("confidence", 0.5)
    try:
        confidence = max(0.0, min(1.0, float(raw_conf)))
    except (TypeError, ValueError):
        confidence = 0.5

    if not explanation:
        return {}

    return {
        "explanation": explanation,
        "suggested_fix": suggested_fix,
        "confidence": confidence,
    }


def _rule_based_explain(
    table: str, metric: str, ts: str, project_id: str = "legacy"
) -> dict:
    """Template fallback when NIM is unavailable. confidence=0.3."""
    window = _context_window(ts)
    all_rc = get_metrics(table, "row_count", project_id, window=window)
    all_nr = get_metrics(table, "null_rate", project_id, window=window)

    # Use only rows up to and including ts so the comparison reflects
    # conditions at the moment of the anomaly, not current state.
    recent_rc = [r for r in all_rc if r["ts"] <= ts]
    recent_nr = [r for r in all_nr if r["ts"] <= ts]

    if metric == "row_count" and len(recent_rc) >= 2:
        prev_val = recent_rc[-2]["value"]
        curr_val = recent_rc[-1]["value"]
        if curr_val < prev_val * 0.5:
            return {
                "explanation": (
                    f"Резкое снижение количества строк в таблице {table}: "
                    f"с {int(prev_val)} до {int(curr_val)}. "
                    "Возможные причины: удаление данных, усечение таблицы или сбой ETL-процесса."
                ),
                "suggested_fix": (
                    "Проверьте логи ETL-пайплайна и историю изменений. "
                    "Убедитесь, что плановые удаления не затронули лишние данные."
                ),
                "confidence": 0.3,
            }
        if curr_val > prev_val * 2:
            return {
                "explanation": (
                    f"Аномальный рост количества строк в таблице {table}: "
                    f"с {int(prev_val)} до {int(curr_val)}. "
                    "Возможные причины: дублирование вставок, повторный запуск импорта."
                ),
                "suggested_fix": (
                    "Проверьте уникальные ограничения. "
                    "Убедитесь, что задача импорта не запустилась дважды."
                ),
                "confidence": 0.3,
            }

    if metric == "null_rate" and recent_nr:
        curr_nr = recent_nr[-1]["value"]
        if curr_nr >= 0.2:
            return {
                "explanation": (
                    f"Высокий уровень NULL в таблице {table}: {curr_nr:.1%}. "
                    "Возможные причины: сбой в заполнении обязательных полей, "
                    "изменение схемы или ошибка трансформации."
                ),
                "suggested_fix": (
                    "Проверьте ETL-пайплайн и логи последней загрузки. "
                    "Убедитесь, что все обязательные поля заполнены корректно."
                ),
                "confidence": 0.3,
            }

    return {
        "explanation": (
            f"Обнаружена аномалия в таблице {table} по метрике {metric} в момент {ts}. "
            "Требуется ручная проверка данных."
        ),
        "suggested_fix": "Проверьте последние изменения в данных и ETL-процессах.",
        "confidence": 0.3,
    }


def explain_anomaly(
    table: str, metric: str, ts: str, project_id: str = "legacy"
) -> dict:
    """Orchestrate LLM explanation with cache bypass (cache handled in the API layer).

    Returns {explanation, suggested_fix, confidence}.
    Never raises — falls back to rule-based on any NIM failure.
    """
    if not settings.NIM_API_KEY:
        return _rule_based_explain(table, metric, ts, project_id=project_id)

    try:
        prompt = _build_prompt(table, metric, ts, project_id=project_id)
        raw = _call_nim(prompt)
        parsed = _parse_nim_response(raw)
        if parsed:
            return parsed
        # JSON parsing failed — use rule-based
        return _rule_based_explain(table, metric, ts, project_id=project_id)
    except Exception:
        return _rule_based_explain(table, metric, ts, project_id=project_id)
