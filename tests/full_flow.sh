#!/usr/bin/env bash
set -euo pipefail

mkdir -p data
rm -f data/read_model.json data/events.jsonl

DOCKER_BIN="docker"
if ! docker info >/dev/null 2>&1 && command -v docker.exe >/dev/null 2>&1; then
  DOCKER_BIN="docker.exe"
fi

compose() {
  "$DOCKER_BIN" compose "$@"
}

compose down --remove-orphans >/dev/null 2>&1 || true
compose up -d --build rabbitmq api consumer

compose run --rm --build producer

for _ in $(seq 1 30); do
  if [ -f data/read_model.json ] && grep -q '"7001"' data/read_model.json; then
    break
  fi
  sleep 1
done

test -f data/read_model.json
grep -q '"event_user"' data/read_model.json
grep -q '"Event Hotel"' data/read_model.json
grep -q '"status": "cancelled"' data/read_model.json
grep -q '"booking_cancelled"' data/read_model.json

test -f data/events.jsonl
test "$(wc -l < data/events.jsonl | tr -d ' ')" -ge 4

echo "lab_6 full flow passed"
