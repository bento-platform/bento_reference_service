FROM ghcr.io/bento-platform/bento_base_image:python-debian-2024.08.12

LABEL org.opencontainers.image.description="Local development image for the Bento reference service."
LABEL devcontainer.metadata='[{ \
  "remoteUser": "bento_user", \
  "customizations": { \
    "vscode": { \
      "extensions": ["ms-python.python", "eamodio.gitlens"], \
      "settings": {"workspaceFolder": "/reference"} \
    } \
  } \
}]'

# FastAPI uses uvicorn for a development server as well
RUN pip install --upgrade pip && pip install --no-cache-dir "uvicorn[standard]==0.30.6"
WORKDIR /reference

COPY pyproject.toml .
COPY poetry.lock .

COPY run.dev.bash .

# Install production + development dependencies
# Without --no-root, we get errors related to the code not being copied in yet.
# But we don't want the code here, otherwise Docker cache doesn't work well.
RUN poetry config virtualenvs.create false && \
    poetry --no-cache install --no-root

# Tell the service that we're running a local development container
ENV BENTO_CONTAINER_LOCAL=true

# Don't copy in actual code, since it'll be mounted in via volume for development
CMD [ "bash", "./run.dev.bash" ]
