FROM ghcr.io/bento-platform/bento_base_image:python-debian-2023.02.09

# FastAPI uses uvicorn for a development server as well
RUN pip install --no-cache-dir poetry==1.3.2 "uvicorn[standard]==0.20.0"

WORKDIR /reference

COPY pyproject.toml .
COPY poetry.toml .
COPY poetry.lock .

# Install production + development dependencies
# Without --no-root, we get errors related to the code not being copied in yet.
# But we don't want the code here, otherwise Docker cache doesn't work well.
RUN poetry install --no-root

# Don't copy in actual code, since it'll be mounted in via volume for development

CMD [ "bash", "./entrypoint.dev.bash" ]
