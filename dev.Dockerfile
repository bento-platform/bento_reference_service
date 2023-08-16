FROM ghcr.io/bento-platform/bento_base_image:python-debian-2023.02.27

# FastAPI uses uvicorn for a development server as well
RUN source /env/bin/activate && pip install --upgrade pip && pip install --no-cache-dir "uvicorn[standard]==0.23.2"
WORKDIR /reference

COPY pyproject.toml .
COPY poetry.lock .

COPY entrypoint.bash .
COPY run.dev.bash .

# Install production + development dependencies
# Without --no-root, we get errors related to the code not being copied in yet.
# But we don't want the code here, otherwise Docker cache doesn't work well.
RUN source /env/bin/activate && poetry install --no-root

# Don't copy in actual code, since it'll be mounted in via volume for development
CMD [ "bash", "./entrypoint.bash" ]
ENTRYPOINT [ "bash", "./run.dev.bash" ]
