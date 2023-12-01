#!/usr/bin/env bash
docker compose -f docker-compose.test.yaml down
docker compose -f docker-compose.test.yaml run --build reference
docker compose -f docker-compose.test.yaml down
docker image prune -f
