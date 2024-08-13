from bento_lib.config.pydantic import BentoFastAPIBaseConfig
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


class Config(BentoFastAPIBaseConfig):
    service_id: str = f"{SERVICE_GROUP}:{SERVICE_ARTIFACT}"
    service_name: str = "Bento Reference Service"
    service_description: str = "Reference data (genomes & annotations) service for the Bento platform."

    database_uri: str = "postgres://localhost:5432"
    file_ingest_tmp_dir: Path = Path(__file__).parent.parent / "tmp"  # Default to repository `tmp` folder
    file_ingest_chunk_size: int = 1024 * 256  # 256 KiB at a time

    file_response_chunk_size: int = 1024 * 256  # 256 KiB at a time
    response_substring_limit: int = 100000  # 100 KB

    feature_response_record_limit: int = 1000


@lru_cache()
def get_config():
    return Config()


ConfigDependency = Annotated[Config, Depends(get_config)]
