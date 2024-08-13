FROM ghcr.io/bento-platform/bento_base_image:python-debian-2024.08.12

# Use uvicorn (instead of hypercorn) in production since I've found
# multiple benchmarks showing it to be faster - David L
RUN pip install --no-cache-dir "uvicorn[standard]==0.30.6"

WORKDIR /reference

# Make sure we have a temporary directory in the container, although ideally one should
# be mounted in so it's not stored on the overlay FS.
RUN mkdir -p tmp

COPY pyproject.toml .
COPY poetry.lock .

# Install production dependencies
# Without --no-root, we get errors related to the code not being copied in yet.
# But we don't want the code here, otherwise Docker cache doesn't work well.
RUN poetry config virtualenvs.create false && \
    poetry --no-cache install --without dev --no-root

# Manually copy only what's relevant
# (Don't use .dockerignore, which allows us to have development containers too)
COPY bento_reference_service bento_reference_service
COPY entrypoint.bash .
COPY run.bash .
COPY LICENSE .
COPY README.md .

# Install the module itself, locally (similar to `pip install -e .`)
RUN poetry install --without dev

ENTRYPOINT [ "bash", "./entrypoint.bash" ]
CMD [ "bash", "./run.bash" ]
