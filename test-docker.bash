#!/usr/bin/env bash
docker compose -f docker-compose.test.yaml down --remove-orphans
docker compose -f docker-compose.test.yaml run --build reference
docker compose -f docker-compose.test.yaml down
docker image prune -f
poetry run ruff format --check
poetry run ruff check
