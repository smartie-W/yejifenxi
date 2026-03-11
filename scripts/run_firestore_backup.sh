#!/bin/zsh
set -euo pipefail

ROOT="/Users/wang/Documents/codex/sales-performance-web"
LOG_DIR="$ROOT/backups/logs"
LOG_FILE="$LOG_DIR/firestore-backup.log"
NUTSTORE_DIR="/Users/wang/Nutstore Files/.symlinks/坚果云/yejifenxi-backups/firestore"
PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin"
NODE_BIN="/usr/local/bin/node"

if [ ! -x "$NODE_BIN" ]; then
  NODE_BIN="$(command -v node || true)"
fi

mkdir -p "$LOG_DIR"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] backup start" >> "$LOG_FILE"

cd "$ROOT"
"$NODE_BIN" scripts/backup_firestore.mjs >> "$LOG_FILE" 2>&1

if [ -d "/Users/wang/Nutstore Files/.symlinks/坚果云" ]; then
  mkdir -p "$NUTSTORE_DIR"
  (
    rsync -a "$ROOT/backups/firestore/" "$NUTSTORE_DIR/" >> "$LOG_FILE" 2>&1 && \
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] nutstore sync done" >> "$LOG_FILE"
  ) &
fi

# Keep local backups for 90 days.
find "$ROOT/backups/firestore" -mindepth 1 -maxdepth 1 -type d -name '20*' -mtime +90 -exec rm -rf {} + >> "$LOG_FILE" 2>&1 || true

echo "[$(date '+%Y-%m-%d %H:%M:%S')] backup done" >> "$LOG_FILE"
