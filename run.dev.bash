#!/bin/bash

# Set .gitconfig for development
/set_gitconfig.bash

export ASGI_APP="bento_reference_service.app:app"

# Set default internal port to 5000
: "${INTERNAL_PORT:=5000}"

python -m poetry install
python -m debugpy --listen 0.0.0.0:9511 -m \
  uvicorn \
  --host 0.0.0.0 \
  --port "${INTERNAL_PORT}" \
  --reload \
  "${ASGI_APP}"
