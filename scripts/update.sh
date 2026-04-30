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

SKIP_BACKUP=0
SKIP_PULL=0
SHOW_LOGS=0

for arg in "$@"; do
  case "$arg" in
    --skip-backup) SKIP_BACKUP=1 ;;
    --skip-pull) SKIP_PULL=1 ;;
    --logs) SHOW_LOGS=1 ;;
    -h|--help)
      cat <<'USAGE'
Usage: scripts/update.sh [--skip-backup] [--skip-pull] [--logs]

Updates the server without deleting Docker volumes:
  1. creates a backup by default;
  2. pulls latest git changes with --ff-only;
  3. rebuilds bot/panel images;
  4. recreates containers with docker compose up -d --remove-orphans.
USAGE
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

if [[ ! -f .env ]]; then
  echo "Missing .env. Copy .env.example to .env and fill secrets first." >&2
  exit 1
fi

if [[ "$SKIP_BACKUP" != "1" ]]; then
  "$ROOT_DIR/scripts/backup.sh"
else
  echo "Skipping backup because --skip-backup was passed."
fi

if [[ "$SKIP_PULL" != "1" ]]; then
  if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    branch="$(git branch --show-current)"
    if [[ -z "$branch" ]]; then
      echo "Git is in detached HEAD; skipping git pull." >&2
    else
      git fetch --prune origin
      git pull --ff-only origin "$branch"
    fi
  else
    echo "Not a git repository; skipping git pull." >&2
  fi
else
  echo "Skipping git pull because --skip-pull was passed."
fi

compose pull db || true
compose build bot panel
compose up -d --remove-orphans
compose ps

if [[ "$SHOW_LOGS" == "1" ]]; then
  compose logs -f --tail=160 bot panel
fi
