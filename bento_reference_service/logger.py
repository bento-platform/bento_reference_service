import logging
import structlog

from bento_lib.logging.structured.configure import configure_structlog, configure_structlog_uvicorn
from fastapi import Depends
from functools import lru_cache
from typing import Annotated

from .config import ConfigDependency
from .constants import BENTO_SERVICE_KIND

__all__ = [
    "get_logger",
    "LoggerDependency",
]


@lru_cache
def get_logger(config: ConfigDependency) -> structlog.stdlib.BoundLogger:
    # configure_structlog(json_logs=not config.bento_container_local, log_level=config.log_level)
    configure_structlog(json_logs=True, log_level=config.log_level)
    configure_structlog_uvicorn()

    return structlog.stdlib.get_logger(f"{BENTO_SERVICE_KIND}.logger")


LoggerDependency = Annotated[logging.Logger, Depends(get_logger)]
