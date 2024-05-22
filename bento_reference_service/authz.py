import re
from bento_lib.auth.middleware.fastapi import FastApiAuthMiddleware
from .config import get_config
from .logger import get_logger

__all__ = [
    "authz_middleware",
]

# Non-standard middleware setup so that we can import the instance and use it for dependencies too
config = get_config()  # TODO: Find a way to DI this
authz_middleware = FastApiAuthMiddleware.build_from_pydantic_config(
    config,
    get_logger(config),
    exempt_request_patterns=(
        (r"GET", re.escape(config.service_docs_path)),
        (r"GET", re.escape(config.service_openapi_path)),
    ),
)
