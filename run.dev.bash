#!/bin/bash

# Set .gitconfig for development
/set_gitconfig.bash

# Source the development virtual environment
source /env/bin/activate

export ASGI_APP="bento_reference_service.app:app"

if [[ -z "${INTERNAL_PORT}" ]]; then
  # Set default internal port to 5000
  export INTERNAL_PORT=5000
fi

python -m poetry install
python -m debugpy --listen 0.0.0.0:5678 -m \
  uvicorn \
  --host 0.0.0.0 \
  --port "${INTERNAL_PORT}" \
  --reload \
  "${ASGI_APP}"
