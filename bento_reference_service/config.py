from fastapi import Depends
from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings
from typing import Annotated

from .constants import SERVICE_GROUP, SERVICE_ARTIFACT

__all__ = [
    "Config",
    "get_config",
    "ConfigDependency",
]


class Config(BaseSettings):
    service_id: str = f"{SERVICE_GROUP}:{SERVICE_ARTIFACT}"
    service_name: str = "Bento Reference Service"
    data_path: Path = Path(__file__).parent / "data"
    service_url_base_path: str = "http://127.0.0.1:5000"  # Base path to construct URIs from

    response_substring_limit: int = 10000  # TODO: Refine default

    # /service-info customization
    service_contact_url: str = "mailto:info@c3g.ca"


@lru_cache()
def get_config():
    return Config()


ConfigDependency = Annotated[Config, Depends(get_config)]
