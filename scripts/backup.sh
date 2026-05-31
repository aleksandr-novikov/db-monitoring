#!/usr/bin/env bash
#
# Backup script for the monitored target DB AND the metrics store (#106).
#
# What it does
# ------------
# 1. Reads connection URLs from the environment (DATABASE_URL = monitored
#    target DB, MONITOR_DB_URL = metrics store; SQLite is supported for
#    the latter too).
# 2. Runs pg_dump on each (or `sqlite3 .backup` for SQLite metrics).
# 3. Gzips the result + writes ``backups/<dbname>-YYYYMMDD-HHMMSS.sql.gz``.
# 4. Computes SHA-256 alongside the dump so restore.sh can verify integrity.
# 5. Rotates: keeps last 7 daily + last 4 weekly (= last 4 Sundays).
#    Daily backups older than 7 days that AREN'T weekly archives are deleted.
#
# Usage
# -----
#   scripts/backup.sh                 # both DBs, default ./backups dir
#   BACKUP_DIR=/srv/dumps scripts/backup.sh
#
# Environment
# -----------
#   DATABASE_URL       — required for target backup; empty → skip
#   MONITOR_DB_URL     — required for metrics backup; empty → skip
#   BACKUP_DIR         — output directory; default ./backups
#   RETENTION_DAILY    — daily-copy retention, default 7
#   RETENTION_WEEKLY   — weekly-copy retention, default 4
#
# Exit codes: 0 ok, 1 fatal (no DBs reachable / unable to write dumps).

set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-./backups}"
RETENTION_DAILY="${RETENTION_DAILY:-7}"
RETENTION_WEEKLY="${RETENTION_WEEKLY:-4}"

mkdir -p "$BACKUP_DIR"

TS="$(date -u +%Y%m%d-%H%M%S)"
DOW="$(date -u +%u)"  # 1 (Mon) .. 7 (Sun)

# ── Helpers ───────────────────────────────────────────────────────────────

_log() { printf '%s [backup] %s\n' "$(date -u +%FT%TZ)" "$*"; }

_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        # macOS — shasum -a 256 is preinstalled.
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

# Extract just the path component (database name) from a libpq URL.
_dbname() {
    local url="$1"
    # postgresql://user:pass@host:port/dbname?opts → dbname
    python3 - <<PY 2>/dev/null || echo "db"
from urllib.parse import urlparse
p = urlparse("$url")
name = p.path.lstrip("/")
print(name if name else "db")
PY
}

_dump_postgres() {
    local url="$1" label="$2"
    local dbname; dbname="$(_dbname "$url")"
    local outfile="${BACKUP_DIR}/${label}-${dbname}-${TS}.sql.gz"
    _log "→ pg_dump ${label} (${dbname}) to ${outfile}"
    # --no-owner / --no-acl keep the dump portable across users/clusters
    # (we restore into a fresh DB on the same image — owners differ).
    # --format=plain is what gzip eats nicely; for >10G consider -Fc later.
    pg_dump --no-owner --no-acl --format=plain "$url" | gzip -9 > "$outfile"
    _sha256 "$outfile" > "${outfile}.sha256"
    _log "  done. sha256 → ${outfile}.sha256"
}

_dump_sqlite() {
    # SQLite metrics store — ``sqlite3 source .backup dest`` produces a
    # consistent on-disk snapshot even under concurrent writes (uses the
    # backup API, not a file copy).
    local url="$1" label="$2"
    local path="${url#sqlite:///}"
    path="${path#sqlite://}"
    if [[ ! -f "$path" ]]; then
        _log "  SQLite file not found at ${path}; skipping ${label}"
        return 0
    fi
    local outfile="${BACKUP_DIR}/${label}-$(basename "$path" .db)-${TS}.sqlite.gz"
    _log "→ sqlite3 backup ${label} (${path}) to ${outfile}"
    local tmp; tmp="$(mktemp)"
    sqlite3 "$path" ".backup '$tmp'"
    gzip -9 -c "$tmp" > "$outfile"
    rm -f "$tmp"
    _sha256 "$outfile" > "${outfile}.sha256"
    _log "  done. sha256 → ${outfile}.sha256"
}

_dump_any() {
    local url="$1" label="$2"
    [[ -z "$url" ]] && { _log "  ${label}: empty URL, skipping"; return 0; }
    case "$url" in
        postgresql://*|postgres://*)  _dump_postgres "$url" "$label" ;;
        sqlite://*|sqlite:///*)       _dump_sqlite   "$url" "$label" ;;
        *) _log "  ${label}: unsupported URL scheme, skipping (${url%%:*}://...)" ;;
    esac
}

# ── Rotation ──────────────────────────────────────────────────────────────
#
# Strategy: every backup is "daily" by default. On Sundays we also mark it
# "weekly" by writing a sibling marker file ``*.weekly``. Pruning then:
# - delete daily older than RETENTION_DAILY days that have NO .weekly marker
# - delete weekly older than (RETENTION_WEEKLY * 7) days
#
# Implemented in pure ``find`` so it works in busybox / minimal images.

_mark_weekly() {
    if [[ "$DOW" == "7" ]]; then
        for f in "$BACKUP_DIR"/*"${TS}".sql.gz "$BACKUP_DIR"/*"${TS}".sqlite.gz; do
            [[ -f "$f" ]] && touch "${f}.weekly"
        done
        _log "Marked today's backups as weekly (Sunday)"
    fi
}

_rotate() {
    # Daily: older than RETENTION_DAILY without .weekly companion.
    find "$BACKUP_DIR" -maxdepth 1 -type f \
        \( -name '*.sql.gz' -o -name '*.sqlite.gz' \) \
        -mtime "+${RETENTION_DAILY}" \
        | while read -r f; do
            if [[ ! -e "${f}.weekly" ]]; then
                _log "Rotating daily: ${f}"
                rm -f "$f" "${f}.sha256"
            fi
        done

    # Weekly: older than RETENTION_WEEKLY*7 days.
    local weekly_age=$((RETENTION_WEEKLY * 7))
    find "$BACKUP_DIR" -maxdepth 1 -type f -name '*.weekly' \
        -mtime "+${weekly_age}" \
        | while read -r marker; do
            local data="${marker%.weekly}"
            _log "Rotating weekly: ${data}"
            rm -f "$data" "${data}.sha256" "$marker"
        done
}

# ── Main ──────────────────────────────────────────────────────────────────

_log "Starting backup at ${TS}"
_dump_any "${DATABASE_URL:-}"    "target"
_dump_any "${MONITOR_DB_URL:-}"  "metrics"
_mark_weekly
_rotate
_log "Done."
