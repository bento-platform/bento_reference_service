#!/bin/bash

export ASGI_APP="bento_reference_service.app:app"

if [[ -z "${INTERNAL_PORT}" ]]; then
  # Set default internal port to 5000
  export INTERNAL_PORT=5000
fi

uvicorn \
  --workers 1 \
  --loop uvloop \
  --host 0.0.0.0 \
  --port "${INTERNAL_PORT}" \
  "${ASGI_APP}"
