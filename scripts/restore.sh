#!/usr/bin/env bash
#
# Restore from a backup created by ``scripts/backup.sh`` (#106).
#
# Usage
# -----
#   scripts/restore.sh backups/target-monitor-20260530-031500.sql.gz
#
# What it does
# ------------
# 1. Verifies the SHA-256 sidecar (``<file>.sha256``) — refuses to restore
#    if the dump has bit-rotted on disk or in transit.
# 2. Detects format by extension: .sql.gz → ``psql``, .sqlite.gz → copy.
# 3. For Postgres restores: prompts for the target DSN (or reads RESTORE_URL
#    env). DROP + CREATE DATABASE is intentionally NOT performed — operator
#    decides whether to wipe; we just stream the dump into whatever DSN is
#    given. This avoids accidentally nuking the live DB by typo.
# 4. For SQLite: writes to a new file alongside the backup unless
#    RESTORE_PATH is set. Live monitor.db is never overwritten silently.
#
# Exit codes: 0 ok, 1 file/checksum problem, 2 restore failure.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <backup-file> [RESTORE_URL=postgresql://...]" >&2
    exit 1
fi

BACKUP="$1"

# ── Checksum verification ────────────────────────────────────────────────

if [[ ! -f "$BACKUP" ]]; then
    echo "FATAL: backup file not found: $BACKUP" >&2
    exit 1
fi
if [[ ! -f "${BACKUP}.sha256" ]]; then
    echo "FATAL: missing sidecar ${BACKUP}.sha256 — refusing to trust dump" >&2
    exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$BACKUP" | awk '{print $1}')"
else
    actual="$(shasum -a 256 "$BACKUP" | awk '{print $1}')"
fi
expected="$(awk '{print $1}' "${BACKUP}.sha256")"

if [[ "$actual" != "$expected" ]]; then
    echo "FATAL: sha256 mismatch" >&2
    echo "  expected: $expected" >&2
    echo "  actual:   $actual" >&2
    echo "Dump is corrupt — try the previous backup." >&2
    exit 1
fi
echo "[restore] sha256 verified: $expected"

# ── Dispatch by format ───────────────────────────────────────────────────

case "$BACKUP" in
    *.sql.gz)
        if [[ -z "${RESTORE_URL:-}" ]]; then
            echo "FATAL: set RESTORE_URL=postgresql://... to point the restore" >&2
            echo "       (we won't guess to avoid clobbering the live DB)" >&2
            exit 1
        fi
        echo "[restore] streaming dump into $RESTORE_URL"
        gunzip -c "$BACKUP" | psql "$RESTORE_URL"
        echo "[restore] done."
        ;;
    *.sqlite.gz)
        RESTORE_PATH="${RESTORE_PATH:-${BACKUP%.sqlite.gz}.restored.sqlite}"
        if [[ -e "$RESTORE_PATH" ]]; then
            echo "FATAL: $RESTORE_PATH already exists, refusing to overwrite" >&2
            exit 1
        fi
        echo "[restore] decompressing to $RESTORE_PATH"
        gunzip -c "$BACKUP" > "$RESTORE_PATH"
        echo "[restore] sqlite3 integrity_check:"
        sqlite3 "$RESTORE_PATH" "PRAGMA integrity_check;" | head -5
        echo "[restore] done. To use:  cp $RESTORE_PATH monitor.db  (after stopping the app)"
        ;;
    *)
        echo "FATAL: unrecognised backup format: $BACKUP" >&2
        echo "       expected .sql.gz (Postgres) or .sqlite.gz (SQLite)" >&2
        exit 2
        ;;
esac
