from bento_lib.config.pydantic import BentoBaseConfig
from fastapi import Depends
from functools import lru_cache
from pathlib import Path
from typing import Annotated

from .constants import SERVICE_GROUP, SERVICE_ARTIFACT

__all__ = [
    "Config",
    "get_config",
    "ConfigDependency",
]


class Config(BentoBaseConfig):
    service_id: str = f"{SERVICE_GROUP}:{SERVICE_ARTIFACT}"
    service_name: str = "Bento Reference Service"
    service_description: str = "Reference data (genomes & annotations) service for the Bento platform."
    service_url_base_path: str = "http://127.0.0.1:5000"  # Base path to construct URIs from

    database_uri: str = "postgres://localhost:5432"
    data_path: Path = Path(__file__).parent / "data"

    file_response_chunk_size: int = 1024 * 16  # 16 KiB at a time
    response_substring_limit: int = 10000  # TODO: Refine default


@lru_cache()
def get_config():
    return Config()


ConfigDependency = Annotated[Config, Depends(get_config)]
