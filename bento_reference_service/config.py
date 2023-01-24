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


config = Config()
