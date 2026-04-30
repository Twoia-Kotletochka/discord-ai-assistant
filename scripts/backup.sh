#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -n "${DOCKER_COMPOSE_CMD:-}" ]]; then
  read -r -a COMPOSE_BIN <<< "$DOCKER_COMPOSE_CMD"
else
  COMPOSE_BIN=(docker compose)
fi

compose() {
  "${COMPOSE_BIN[@]}" "$@"
}

BACKUP_ROOT="${BACKUP_DIR:-$ROOT_DIR/backups}"
STAMP="$(date -u +"%Y%m%d-%H%M%S")"
DEST="$BACKUP_ROOT/$STAMP"

mkdir -p "$DEST"

echo "Creating backup in $DEST"

{
  echo "created_at_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "git_commit=$(git rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "git_branch=$(git branch --show-current 2>/dev/null || echo unknown)"
  echo
  echo "[docker compose ps]"
  compose ps 2>/dev/null || true
} > "$DEST/manifest.txt"

if [[ -f .env ]]; then
  sed -E 's/^([^#=]*(TOKEN|KEY|PASSWORD|SECRET)[^=]*)=.*/\1=[redacted]/I' .env > "$DEST/env.redacted"
  if [[ "${INCLUDE_ENV:-0}" == "1" ]]; then
    cp .env "$DEST/.env"
    chmod 600 "$DEST/.env"
    echo "Included .env because INCLUDE_ENV=1. Treat this backup as secret."
  fi
else
  echo "Warning: .env not found; skipped env metadata." >&2
fi

DB_CID="$(compose ps -q db 2>/dev/null || true)"
DB_RUNNING="false"
if [[ -n "$DB_CID" ]]; then
  DB_RUNNING="$(docker inspect -f '{{.State.Running}}' "$DB_CID" 2>/dev/null || echo false)"
fi

if [[ "$DB_RUNNING" == "true" ]]; then
  echo "Dumping MariaDB..."
  compose exec -T db sh -lc 'mariadb-dump -u"$MARIADB_USER" -p"$MARIADB_PASSWORD" --single-transaction --quick --routines --events "$MARIADB_DATABASE"' > "$DEST/db.sql"
  gzip -9 "$DEST/db.sql"
else
  echo "Warning: db container is not running; skipped MariaDB dump." >&2
fi

echo "Archiving bot data volume..."
compose run --rm --no-deps --user 0:0 --entrypoint tar bot -czf - -C /app/data . > "$DEST/bot-data.tgz"

tar -czf "$DEST.tar.gz" -C "$BACKUP_ROOT" "$STAMP"

echo "Backup directory: $DEST"
echo "Backup archive:   $DEST.tar.gz"
