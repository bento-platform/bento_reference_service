from pathlib import Path
from pydantic import BaseSettings
from .constants import SERVICE_GROUP, SERVICE_ARTIFACT

__all__ = [
    "Config",
    "config",
]


class Config(BaseSettings):
    service_id: str = f"{SERVICE_GROUP}:{SERVICE_ARTIFACT}"
    service_name: str = "Bento Reference Service"
    data_path: Path = Path(__file__).parent / "data"
    service_url_base_path: str = "http://127.0.0.1:5000"  # Base path to construct URIs from

    elasticsearch_url: str = "http://localhost:9200"  # Connection URL for Elasticsearch

    response_substring_limit: int = 10000  # TODO: Refine default


config = Config()
