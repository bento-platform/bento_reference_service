#!/bin/bash

export ASGI_APP="bento_reference_service.main:app"

# Set default internal port to 5000
: "${INTERNAL_PORT:=5000}"

uvicorn \
  --workers 1 \
  --loop uvloop \
  --host 0.0.0.0 \
  --port "${INTERNAL_PORT}" \
  "${ASGI_APP}"
