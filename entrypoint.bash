#!/bin/bash

if [[ -z "${INTERNAL_PORT}" ]]; then
  # Set default internal port to 5000
  export INTERNAL_PORT=5000
fi

uvicorn bento_reference_service.app:app \
  --workers 1 \
  --loop uvloop \
  --host 0.0.0.0 \
  --port "${INTERNAL_PORT}"
