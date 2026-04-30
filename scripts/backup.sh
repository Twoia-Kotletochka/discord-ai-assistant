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
BACKUP_ARCHIVE_RETENTION="${BACKUP_ARCHIVE_RETENTION:-2}"
STAMP="$(date -u +"%Y%m%d-%H%M%S")"
DEST="$BACKUP_ROOT/$STAMP"

if ! [[ "$BACKUP_ARCHIVE_RETENTION" =~ ^[0-9]+$ ]] || [[ "$BACKUP_ARCHIVE_RETENTION" -lt 1 ]]; then
  echo "BACKUP_ARCHIVE_RETENTION must be a positive integer." >&2
  exit 2
fi

prune_old_backups() {
  local retention="$1"
  local root="$2"
  local path
  local index
  local stale
  local nullglob_was_set=0
  local -a dirs archives sorted_dirs sorted_archives

  shopt -q nullglob && nullglob_was_set=1 || true
  shopt -s nullglob

  for path in "$root"/[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9][0-9][0-9]; do
    [[ -d "$path" ]] && dirs+=("${path##*/}")
  done
  for path in "$root"/[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9][0-9][0-9].tar.gz; do
    [[ -f "$path" ]] && archives+=("${path##*/}")
  done

  if [[ "$nullglob_was_set" -eq 0 ]]; then
    shopt -u nullglob
  fi

  if [[ "${#dirs[@]}" -gt 0 ]]; then
    while IFS= read -r stale; do
      [[ -n "$stale" ]] && sorted_dirs+=("$stale")
    done < <(printf '%s\n' "${dirs[@]}" | LC_ALL=C sort -r)
  fi
  for ((index = retention; index < ${#sorted_dirs[@]}; index++)); do
    stale="${sorted_dirs[$index]}"
    echo "Pruning old backup directory: $root/$stale"
    rm -rf -- "$root/$stale"
  done

  if [[ "${#archives[@]}" -gt 0 ]]; then
    while IFS= read -r stale; do
      [[ -n "$stale" ]] && sorted_archives+=("$stale")
    done < <(printf '%s\n' "${archives[@]}" | LC_ALL=C sort -r)
  fi
  for ((index = retention; index < ${#sorted_archives[@]}; index++)); do
    stale="${sorted_archives[$index]}"
    echo "Pruning old backup archive: $root/$stale"
    rm -f -- "$root/$stale"
  done
}

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
echo "Keeping latest $BACKUP_ARCHIVE_RETENTION backup set(s)."
prune_old_backups "$BACKUP_ARCHIVE_RETENTION" "$BACKUP_ROOT"
